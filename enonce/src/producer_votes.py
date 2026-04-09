#!/usr/bin/env python3
"""Producteur Kafka — topic vote_events_raw (implémentation à compléter selon les repères TODO)."""

import csv   # ajouté pour lire candidates.csv
import json
import os
import time
import random
import uuid
import datetime as dt
from pathlib import Path

from confluent_kafka import Producer

BOOTSTRAP = "localhost:9092"
TOPIC = "vote_events_raw"
_ROOT = Path(__file__).resolve().parent.parent.parent
COMMUNES_FILE = _ROOT / "data" / "communes_fr.json"
SAMPLE_COMMUNES = _ROOT / "data" / "communes_fr_sample.json"
CANDIDATES_FILE = _ROOT / "data" / "candidates.csv"
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "0"))  # <=0: infinite live stream
START_DELAY_MS = float(os.getenv("START_DELAY_MS", "20"))

# Cache des candidats (lu une seule fois depuis le CSV pour éviter de relire à chaque événement)
_candidates_data: list[dict] = []


def _get_candidates() -> list[dict]:
    """Charge et met en cache la liste des candidats depuis data/candidates.csv."""
    global _candidates_data
    if not _candidates_data:
        with CANDIDATES_FILE.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            _candidates_data = list(reader)
    return _candidates_data


def load_communes() -> list[dict]:
    """
    TODO 0 — Charger les communes (à implémenter)

    Étapes obligatoires :
    1) Lire le fichier JSON : priorité à COMMUNES_FILE ; s’il est absent, utiliser SAMPLE_COMMUNES.
    2) Ouvrir en UTF-8, lire tout le texte, puis json.loads(...) sur ce contenu.
    3) Le fichier fourni est en général une LISTE d’objets ; chaque objet a au minimum :
         - "code" (str INSEE, ex. "75056")   → city_code
         - "nom" (str)                       → city_name
         - "codeDepartement" (str, ex. "75") → department_code (indispensable pour ksqlDB + carte)
         - "codeRegion" (str)                → region_code (optionnel mais utile)
    4) Filtrer : ne garder que les dicts où code et nom sont non vides.
    5) Si la liste finale est vide : lever une erreur explicite (RuntimeError ou message + arrêt) —
       sinon random.choice lèvera une erreur plus tard.

    Ne pas boucler sur votes_municipales_sample.jsonl pour publier : le sujet impose le mode live (génération).
    """
    # → Réponse : Charger le fichier JSON des communes avec priorité au fichier complet, sinon le sample.
    # → Explication : Path.exists() vérifie la présence du fichier sans exception.
    #                 .read_text() + json.loads() est plus lisible qu'un open() + f.read() + json.load().
    #                 Le filtre garantit que random.choice() ne lira jamais un objet incomplet.

    # Choisir la source : fichier complet ou fallback sample
    source = COMMUNES_FILE if COMMUNES_FILE.exists() else SAMPLE_COMMUNES
    if not source.exists():
        raise RuntimeError(
            f"Fichier communes introuvable : {source}.\n"
            "Lancez d'abord : python enonce/src/generate_votes_data.py"
        )

    # Lire et désérialiser le JSON
    communes = json.loads(source.read_text(encoding="utf-8"))

    # Filtrer : garder uniquement les communes avec code INSEE et nom non vides
    communes = [
        c for c in communes
        if c.get("code") and c.get("nom")
    ]

    if not communes:
        raise RuntimeError(
            f"Aucune commune valide dans {source}. Vérifier le format du fichier JSON."
        )

    return communes


def build_realtime_event(communes: list[dict], sent: int) -> dict:
    """
    TODO 4 — Événement JSON généré à la volée (aligné validateur + ksqlDB + chargeur)

    Préparation :
    - Tirer une commune : c = random.choice(communes) (vérifier que la liste n’est pas vide avant).
    - Lire data/candidates.csv (csv ou split) pour choisir un candidate_id EXISTANT et sa colonne
      political_block → exposer la même valeur sous la clé candidate_block dans le JSON.

    Dictionnaire à retourner (clés exactes recommandées, types JSON) :
      vote_id            : str uuid unique (str(uuid.uuid4()))
      election_id        : str fixe ex. "muni_2026"
      event_time         : str ISO8601 UTC finissant par Z (maintenant UTC)
      city_code          : str = c["code"]
      city_name          : str = c["nom"]
      department_code    : str = str(c.get("codeDepartement", ""))  # OBLIGATOIRE pour vote_count_by_dept_block
      region_code        : str = str(c.get("codeRegion", ""))
      polling_station_id : str inventé ex. "PS_001"
      candidate_id       : str depuis le CSV
      candidate_name     : str optionnel (colonne du CSV si présente)
      candidate_party    : str optionnel (colonne party du CSV)
      candidate_block    : str = political_block du CSV  # OBLIGATOIRE pour agrégat département × bloc
      channel            : str parmi "booth" | "mobile" | "assist_terminal"
      signature_ok       : bool True en général
      voter_hash         : str inventé
      ingestion_ts       : str ISO comme event_time

    Optionnel mais très utile pour le validateur / KPI rejets :
    - Dans ~3 à 8 % des cas : signature_ok False OU candidate_id inconnu (ex. "C99") OU vote_id "".
      Sinon rejected_by_reason restera vide et les métriques « rejets » seront fausses.

    Contrainte sujet : ne pas utiliser une boucle « for line in open(jsonl) » comme source principale d’envoi.
    """
    # → Réponse : Tirer une commune aléatoire et un candidat aléatoire du CSV ; injecter ~5-8% de votes invalides.
    # → Explication : Les erreurs injectées si (r < 0.03 / 0.06 / 0.08) alimentent rejected_by_reason en ksqlDB.
    #                 candidate_block DOIT être la valeur political_block du CSV pour que le TODO 6 ksqlDB fonctionne.
    #                 department_code DOIT venir de codeDepartement pour la carte départementale.

    # 1) Tirer une commune et un candidat aléatoires
    c = random.choice(communes)
    candidates = _get_candidates()
    # Utiliser le biais géographique (TODO 5) si le département est connu
    dept_code = str(c.get("codeDepartement", ""))
    candidate = _weighted_candidate_for_dept(dept_code, candidates)

    # 2) Valeurs par défaut (vote valide)
    vote_id      = str(uuid.uuid4())
    signature_ok = True
    candidate_id = candidate["candidate_id"]

    # 3) Injecter ~8 % de votes invalides pour alimenter les métriques de rejet
    r = random.random()
    if r < 0.03:
        signature_ok = False          # ~3 % : signature invalide (INVALID_SIGNATURE)
    elif r < 0.06:
        candidate_id = "C99"          # ~3 % : candidat inconnu (UNKNOWN_CANDIDATE)
    elif r < 0.08:
        vote_id = ""                  # ~2 % : vote_id manquant (MISSING_VOTE_ID)

    now_z = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
    return {
        "vote_id":            vote_id,
        "election_id":        "muni_2026",
        "event_time":         now_z,
        "city_code":          c["code"],
        "city_name":          c.get("nom", ""),
        "department_code":    str(c.get("codeDepartement", "")),  # OBLIGATOIRE pour vote_count_by_dept_block
        "region_code":        str(c.get("codeRegion", "")),
        "polling_station_id": f"PS_{random.randint(1, 120):03d}",
        "candidate_id":       candidate_id,
        "candidate_name":     candidate.get("candidate_name", ""),
        "candidate_party":    candidate.get("party", ""),
        "candidate_block":    candidate.get("political_block", "autre"),  # OBLIGATOIRE pour dépt × bloc
        "channel":            random.choice(["booth", "mobile", "assist_terminal"]),
        "signature_ok":       signature_ok,
        "voter_hash":         f"vh_{uuid.uuid4().hex[:16]}",
        "ingestion_ts":       now_z,
    }


# TODO 5 (avancé) — Territoire non uniforme
#
# Objectif : un département / une commune ne vote pas comme une autre (poids par bloc politique).
# Piste minimale acceptable :
#   - Construire un dict dept_code → distribution { "gauche": 0.3, "droite": 0.25, ... }, tirer un bloc,
#     puis un candidat du CSV dont political_block correspond (après normalisation : ex. "ecologiste" → "gauche"
#     si vous vous alignez avec le sujet carte / blocs).
# Piste avancée : télécharger un CSV data.gouv (résultats agrégés par dept), parser, en déduire des poids.
#
# Sans TODO 5 : tirage uniforme parmi les candidats du CSV reste une version minimale acceptable.
#
# → Réponse : Implémentation minimale avec biais géographique simplifié.
# → Explication : Certains départements (ex: 75, 13, 69 = grandes villes) favorisent la gauche,
#                 d'autres (ex: 06, 83 = Paca) la droite. Le tirage pondéré par bloc puis par candidat
#                 produit une carte plus réaliste que le tirage uniforme.

# Distribution simplifiée bloc → poids par famille de département (numéro département)
_DEPT_BIAS: dict[str, dict[str, float]] = {
    # Grandes métropoles de gauche
    "default_urban": {"extreme_gauche": 0.12, "gauche": 0.28, "centre": 0.20, "droite": 0.22, "droite_nationale": 0.10, "autre": 0.08},
    # Paca / littoral méditerranéen : droite nationale plus forte
    "default_south": {"extreme_gauche": 0.08, "gauche": 0.18, "centre": 0.18, "droite": 0.22, "droite_nationale": 0.26, "autre": 0.08},
    # Défaut neutre
    "default":       {"extreme_gauche": 0.10, "gauche": 0.22, "centre": 0.22, "droite": 0.22, "droite_nationale": 0.16, "autre": 0.08},
}

# Mapping département → profil de vote
_DEPT_PROFILE: dict[str, str] = {
    **{d: "default_urban" for d in ["75", "69", "13", "33", "31", "59", "67", "76", "44", "34"]},
    **{d: "default_south" for d in ["06", "83", "13", "30", "34", "66", "84"]},
}


def _weighted_candidate_for_dept(dept_code: str, candidates: list[dict]) -> dict:
    """
    Tire un candidat avec biais géographique (TODO 5).
    Algorithme : 1) choisir un bloc selon la distribution du département,
                 2) choisir un candidat dans ce bloc.
    Repli : si aucun candidat du bloc, tirage uniforme.
    """
    profile_key = _DEPT_PROFILE.get(dept_code, "default")
    distribution = _DEPT_BIAS[profile_key]

    # Tirage pondéré d'un bloc
    blocs   = list(distribution.keys())
    weights = list(distribution.values())
    chosen_bloc = random.choices(blocs, weights=weights, k=1)[0]

    # Filtrer les candidats dont political_block correspond au bloc choisi
    pool = [c for c in candidates if c.get("political_block") == chosen_bloc]
    return random.choice(pool) if pool else random.choice(candidates)


def main() -> None:
    # Configuration du producteur avec buffer plus large et linger pour regrouper les envois
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "queue.buffering.max.messages": 500000,   # 500k au lieu de 100k par défaut
        "queue.buffering.max.kbytes":   1048576,   # 1 Go max
        "linger.ms":                    50,         # regroupe les envois par lots de 50ms
        "batch.num.messages":           1000,       # taille de lot
    })
    sent = 0
    communes = load_communes()
    # Ordre conseillé : finir TODO 0 et TODO 4 avant de tester la boucle.

    while True:
        evt = build_realtime_event(communes, sent)

        # TODO 1 — Clé Kafka (partitionnement)
        # Attendu : bytes UTF-8, stable par zone ; exemple :
        #   key = (evt.get("city_code") or "UNKNOWN").encode("utf-8")
        # Éviter key=None si vous voulez un partitionnement prévisible par commune.
        #
        # → Réponse : Utiliser city_code comme clé partitionne tous les votes d'une même commune ensemble.
        # → Explication : Kafka achemine tous les messages avec la même clé vers la même partition,
        #                garantissant l'ordre des événements par commune (utile pour les fenêtres glissantes).
        key = (evt.get("city_code") or "UNKNOWN").encode("utf-8")

        # TODO 2 — Valeur = JSON UTF-8
        # Attendu :
        #   value = json.dumps(evt, ensure_ascii=False).encode("utf-8")
        # ensure_ascii=False conserve les accents ; .encode("utf-8") est requis pour confluent-kafka.
        #
        # → Réponse : Sérialiser le dict Python en JSON, puis encoder en bytes UTF-8.
        # → Explication : ensure_ascii=False préserve les caractères accentués des noms de communes.
        value = json.dumps(evt, ensure_ascii=False).encode("utf-8")

        # Envoi avec retry automatique sur BufferError (queue pleine)
        while True:
            try:
                producer.produce(TOPIC, key=key, value=value)
                break
            except BufferError:
                # Queue pleine : attendre que des messages soient livrés
                producer.poll(1.0)

        sent += 1

        # Drainer la queue interne du producteur pour éviter BufferError: Local: Queue full
        # poll(0) = non-bloquant : traite les callbacks de livraison en attente sans attendre
        producer.poll(0)

        # Flush périodique toutes les 500 messages pour maintenir la queue à un niveau sain
        if sent % 500 == 0:
            producer.flush(timeout=5)
            print(f"  [{sent}] messages envoyés...")

        # TODO 3 (optionnel) — Rythme
        # Après chaque message (ou chaque rafale), time.sleep(START_DELAY_MS / 1000.0) pour voir le flux dans Kafka UI.
        #
        # → Réponse : Déjà implémenté ci-dessous avec la variable d'environnement START_DELAY_MS.
        # → Explication : START_DELAY_MS=20 (défaut) = 50 messages/seconde. Mettre à 0 pour le mode batch rapide.
        if START_DELAY_MS > 0:
            time.sleep(START_DELAY_MS / 1000.0)

        if MAX_MESSAGES > 0 and sent >= MAX_MESSAGES:
            break

    producer.flush()
    print(f"Envoyé {sent} message(s) sur {TOPIC} (flux temps réel)")


if __name__ == "__main__":
    main()
