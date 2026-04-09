#!/usr/bin/env python3
"""Chargeur des agrégats Kafka vers Cassandra (implémentation à compléter)."""

import re   # ajouté pour le fallback parsing de la clé Kafka

from cassandra.cluster import Cluster
from confluent_kafka import Consumer
import json

BOOTSTRAP = "localhost:9092"
TOPIC = "vote_agg_city_minute"
GROUP_ID = "agg-cassandra-loader"

CASSANDRA_HOSTS = ["127.0.0.1"]
KEYSPACE = "elections"


def main() -> None:
    # =========================================================================
    # TODO 1 — Connexion Cassandra + requêtes préparées (prepared statements)
    # =========================================================================
    # 1) cluster = Cluster(CASSANDRA_HOSTS)
    # 2) session = cluster.connect(KEYSPACE)   # keyspace "elections" doit exister (schema.cql)
    # 3) Préparer 3 requêtes INSERT (points d’interrogation ? pour le bind) :
    #    - INSERT INTO votes_by_city_minute (city_code, minute_bucket, candidate_id, votes_count) VALUES (?,?,?,?)
    #    - INSERT INTO votes_by_candidate_city (candidate_id, city_code, minute_bucket, votes_count) VALUES (?,?,?,?)
    #    - INSERT INTO votes_by_department_block (department_code, block, votes_count) VALUES (?,?,?)
    #    stmt = session.prepare("...")
    #
    # Ne pas laisser session = None sinon le chargeur ne peut rien insérer.
    #
    # → Réponse : Connexion au cluster Cassandra et préparation des 3 INSERT statements.
    # → Explication : session.prepare() compile la requête CQL une seule fois côté serveur ;
    #                 session.execute(stmt, (val1, val2, ...)) exécute avec les paramètres bindés.
    #                 C'est plus efficace (pas de re-parsing) et protège contre les injections CQL.

    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect(KEYSPACE)

    # INSERT dans votes_by_city_minute (partition city_code, clustering minute_bucket, candidate_id)
    stmt_city = session.prepare(
        "INSERT INTO votes_by_city_minute "
        "(city_code, minute_bucket, candidate_id, votes_count) VALUES (?,?,?,?)"
    )
    # INSERT dans votes_by_candidate_city (partition candidate_id, clustering city_code, minute_bucket)
    stmt_cand = session.prepare(
        "INSERT INTO votes_by_candidate_city "
        "(candidate_id, city_code, minute_bucket, votes_count) VALUES (?,?,?,?)"
    )
    # INSERT dans votes_by_department_block (partition department_code, clustering block)
    stmt_dept = session.prepare(
        "INSERT INTO votes_by_department_block "
        "(department_code, block, votes_count) VALUES (?,?,?)"
    )

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )

    # =========================================================================
    # TODO 2 — Topics Kafka (liste, pas un seul topic)
    # =========================================================================
    # S’abonner à une LISTE contenant au minimum :
    #   "VOTE_COUNT_BY_CITY_MINUTE"
    #   "VOTE_COUNT_BY_DEPT_BLOCK"
    # Optionnel selon votre sujet : "vote_agg_city_minute"
    #
    # Exemple :
    #   consumer.subscribe(["vote_agg_city_minute", "VOTE_COUNT_BY_CITY_MINUTE", "VOTE_COUNT_BY_DEPT_BLOCK"])
    #
    # Astuce : si vous relancez souvent, changez GROUP_ID (suffixe uuid) pour relire depuis le début
    # sans effet de groupe consumer figé.

    # =========================================================================
    # TODO 2 — Topics Kafka (liste, pas un seul topic)
    # =========================================================================
    # S'abonner à une LISTE contenant au minimum :
    #   "VOTE_COUNT_BY_CITY_MINUTE"
    #   "VOTE_COUNT_BY_DEPT_BLOCK"
    # Optionnel selon votre sujet : "vote_agg_city_minute"
    #
    # Exemple :
    #   consumer.subscribe(["vote_agg_city_minute", "VOTE_COUNT_BY_CITY_MINUTE", "VOTE_COUNT_BY_DEPT_BLOCK"])
    #
    # Astuce : si vous relancez souvent, changez GROUP_ID (suffixe uuid) pour relire depuis le début
    # sans effet de groupe consumer figé.
    #
    # → Réponse : S'abonner aux deux topics produits par ksqlDB (majuscules = nom créé automatiquement).
    # → Explication : VOTE_COUNT_BY_CITY_MINUTE = table fenêtre 1 min (Cassandra votes_by_city/candidate_city).
    #                 VOTE_COUNT_BY_DEPT_BLOCK   = table dépt × bloc  (Cassandra votes_by_department_block).

    consumer.subscribe([
        "VOTE_COUNT_BY_CITY_MINUTE",  # agrégat ville × candidat × fenêtre 1 min
        "VOTE_COUNT_BY_DEPT_BLOCK",   # agrégat département × bloc politique
    ])
    print("Chargeur Cassandra démarré…")

    inserted = 0
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Erreur consumer:", msg.error())
            continue

        # Ignorer les tombstones (messages DELETE dans le changelog ksqlDB)
        if msg.value() is None:
            continue

        try:
            row = json.loads(msg.value().decode("utf-8"))
        except Exception as exc:
            print(f"Erreur parsing JSON: {exc}")
            continue

        topic = msg.topic()

        # =====================================================================
        # TODO 3 — Mapping (clés souvent en MAJUSCULES dans le JSON ksqlDB)
        # =====================================================================
        #
        # Branche A — si topic == "VOTE_COUNT_BY_DEPT_BLOCK" :
        #   department = row.get("DEPARTMENT_CODE_V") or row.get("department_code_v")
        #              or row.get("DEPARTMENT_CODE") or row.get("department_code") or ""
        #   block      = row.get("BLOCK_V") or row.get("block_v")
        #              or row.get("CANDIDATE_BLOCK") or row.get("candidate_block") or "autre"
        #   votes      = int(row.get("TOTAL_VOTES") or row.get("total_votes") or 0)
        #   Si department non vide : INSERT votes_by_department_block
        #
        # Branche B — sinon (topics fenêtre ville / minute) :
        #   city         = row.get("CITY_CODE_V") or row.get("city_code_v") or row.get("CITY_CODE") or row.get("city_code")
        #   cand         = row.get("CANDIDATE_ID_V") or row.get("candidate_id_v") or ...
        #   minute_key   = row.get("WINDOWSTART") or row.get("WINDOW_START") or row.get("window_start")  # souvent epoch ms
        #   votes        = int(row.get("VOTES_IN_MINUTE") or row.get("votes_in_minute") or 0)
        #   Si city/cand manquent : parser msg.key() (bytes) avec regex (code INSEE 5 chiffres, candidat C##).
        #
        # =====================================================================
        # TODO 4 — INSERT
        # =====================================================================
        # - Dept/bloc : session.execute(stmt_dept, (department, block, votes))
        # - Ville minute : session.execute(stmt_city, (city, str(minute_key), cand, votes))
        # - Même ligne aussi dans votes_by_candidate_city : (cand, city, str(minute_key), votes)
        #
        # → Réponse (TODO 3 + 4) : Mapper les champs du JSON ksqlDB (MAJUSCULES) vers les colonnes Cassandra.
        # → Explication : ksqlDB écrit les clés JSON en MAJUSCULES dans les topics derivés (ex : CITY_CODE_V).
        #                 On couvre les deux casses pour être robuste aux différentes versions de ksqlDB.
        #                 Les tombstones (valeur None) sont ignorés au-dessus.

        if topic == "VOTE_COUNT_BY_DEPT_BLOCK":
            # --- Branche A : agrégat département × bloc ---
            department = (
                row.get("DEPARTMENT_CODE_V") or row.get("department_code_v")
                or row.get("DEPARTMENT_CODE")  or row.get("department_code") or ""
            )
            block = (
                row.get("BLOCK_V")         or row.get("block_v")
                or row.get("CANDIDATE_BLOCK") or row.get("candidate_block") or "autre"
            )
            votes = int(row.get("TOTAL_VOTES") or row.get("total_votes") or 0)

            if department:
                # TODO 4 — INSERT dans votes_by_department_block
                session.execute_async(stmt_dept, (department, block, votes))
                inserted += 1

        else:
            # --- Branche B : agrégat ville × candidat × fenêtre 1 min ---
            city = (
                row.get("CITY_CODE_V")     or row.get("city_code_v")
                or row.get("CITY_CODE")    or row.get("city_code") or ""
            )
            cand = (
                row.get("CANDIDATE_ID_V")  or row.get("candidate_id_v")
                or row.get("CANDIDATE_ID") or row.get("candidate_id") or ""
            )
            minute_key = (
                row.get("WINDOWSTART")
                or row.get("WINDOW_START")
                or row.get("window_start") or ""
            )
            votes = int(row.get("VOTES_IN_MINUTE") or row.get("votes_in_minute") or 0)

            # Fallback : parser la clé du message si les champs valeur sont absents
            if not city or not cand:
                try:
                    key_bytes = msg.key()
                    if key_bytes:
                        key_str   = key_bytes.decode("utf-8")
                        city_m    = re.search(r'"?(\d{5})"?', key_str)
                        cand_m    = re.search(r'"?(C\d+)"?',  key_str)
                        if city_m and not city:
                            city = city_m.group(1)
                        if cand_m and not cand:
                            cand = cand_m.group(1)
                except Exception:
                    pass

            if city and cand:
                # TODO 4 — INSERT dans votes_by_city_minute ET votes_by_candidate_city
                session.execute_async(stmt_city, (city, str(minute_key), cand, votes))
                session.execute_async(stmt_cand, (cand, city, str(minute_key), votes))
                inserted += 2

            if inserted % 2000 == 0 and inserted > 0:
                print(f"  [{inserted}] inserts Cassandra...")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nArrêt chargeur Cassandra")
