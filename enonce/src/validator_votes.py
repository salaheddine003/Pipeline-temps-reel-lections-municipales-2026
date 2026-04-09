#!/usr/bin/env python3
"""Validateur — topics vote_events_raw → valides / rejetés (implémentation à compléter)."""

import json
from pathlib import Path

from confluent_kafka import Consumer, Producer

BOOTSTRAP = "localhost:9092"
TOPIC_IN = "vote_events_raw"
TOPIC_VALID = "vote_events_valid"
TOPIC_REJECTED = "vote_events_rejected"
GROUP_ID = "votes-validator-group"

_ROOT = Path(__file__).resolve().parent.parent.parent
CANDIDATES_FILE = _ROOT / "data" / "candidates.csv"


def load_candidate_ids() -> set[str]:
    ids = set()
    with CANDIDATES_FILE.open("r", encoding="utf-8") as f:
        next(f)
        for line in f:
            parts = line.strip().split(",")
            if parts and parts[0]:
                ids.add(parts[0])
    return ids


def main() -> None:
    candidates = load_candidate_ids()

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe([TOPIC_IN])

    stats = {"valid": 0, "rejected": 0}
    print("Validateur démarré…")
    # Tant que les TODO 2 et 3 ne sont pas implémentés, aucun message n’est envoyé vers Kafka (normal).

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Erreur consumer:", msg.error())
            continue

        evt = json.loads(msg.value().decode("utf-8"))

        # TODO 1 — Règles dans CET ORDRE (sinon messages d’erreur incohérents)
        #
        # 1) Signature : rejeter si evt.get("signature_ok") is not True
        #    (ainsi None, False, ou toute valeur non strictement True part en rejet)
        #    Raison : "INVALID_SIGNATURE"
        #
        # 2) vote_id vide ou absent : not evt.get("vote_id")  -> rejet
        #    Raison : "MISSING_VOTE_ID"
        #
        # 3) candidate_id pas dans le set `candidates` (chargé depuis CSV)  -> rejet
        #    Raison : "UNKNOWN_CANDIDATE"
        #
        # Sinon : is_valid = True, reason = "".
        #
        # Astuce : construire (is_valid, reason) avec des if / elif / else pour une seule raison par message.
        #
        # → Réponse : Les 3 règles metier dans l'ordre de priorité imposé par le sujet.
        # → Explication : L'ordre est important : un vote avec signature_ok=False ET vote_id vide
        #                 sera classé INVALID_SIGNATURE (première règle atteinte), pas MISSING_VOTE_ID.
        if evt.get("signature_ok") is not True:
            is_valid = False
            reason   = "INVALID_SIGNATURE"
        elif not evt.get("vote_id"):
            is_valid = False
            reason   = "MISSING_VOTE_ID"
        elif evt.get("candidate_id") not in candidates:
            is_valid = False
            reason   = "UNKNOWN_CANDIDATE"
        else:
            is_valid = True
            reason   = ""

        if is_valid:
            # TODO 2 — Topic des valides
            # - Clé : même principe que le producteur, bytes UTF-8 :
            #     out_key = (evt.get("city_code") or "UNKNOWN").encode("utf-8")
            # - Valeur : le MÊME evt (dict Python) sérialisé comme à l’entrée :
            #     producer.produce(TOPIC_VALID, key=out_key, value=json.dumps(evt, ensure_ascii=False).encode("utf-8"))
            # - Puis : stats["valid"] += 1
            # - Appeler producer.flush() régulièrement (déjà fait tous les 500 messages ci-dessous).
            #
            # → Réponse : Publier le vote valide sur vote_events_valid en réutilisant le même JSON enrichi.
            # → Explication : On garde le même dict `evt` (pas de copie) : il contient déjà tous les champs
            #                 utiles pour ksqlDB (department_code, candidate_block, etc.).
            out_key = (evt.get("city_code") or "UNKNOWN").encode("utf-8")
            producer.produce(
                TOPIC_VALID,
                key=out_key,
                value=json.dumps(evt, ensure_ascii=False).encode("utf-8"),
            )
            stats["valid"] += 1
        else:
            # TODO 3 — Topic des rejets
            # 1) Ajouter la raison dans le JSON AVANT produce :
            #      evt["error_reason"] = reason
            # 2) Même clé que pour le valide (city_code en bytes).
            # 3) producer.produce(TOPIC_REJECTED, key=out_key, value=json.dumps(evt, ensure_ascii=False).encode("utf-8"))
            # 4) stats["rejected"] += 1
            #
            # → Réponse : Enrichir le JSON avec error_reason AVANT la sérialisation, puis publisher sur TOPIC_REJECTED.
            # → Explication : Le champ error_reason sera lu par le stream ksqlDB vote_events_rejected_stream
            #                 et aggrégé dans la table rejected_by_reason pour les KPI du dashboard.
            evt["error_reason"] = reason
            out_key = (evt.get("city_code") or "UNKNOWN").encode("utf-8")
            producer.produce(
                TOPIC_REJECTED,
                key=out_key,
                value=json.dumps(evt, ensure_ascii=False).encode("utf-8"),
            )
            stats["rejected"] += 1

        if (stats["valid"] + stats["rejected"]) % 500 == 0:
            producer.flush()
            print(
                f"Progression total={stats['valid'] + stats['rejected']} "
                f"valid={stats['valid']} rejected={stats['rejected']}"
            )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nArrêt validateur")
