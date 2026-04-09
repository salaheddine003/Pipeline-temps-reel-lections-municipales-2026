-- Requêtes ksqlDB — à compléter (repères TODO ci-dessous)
--
-- ATTENTION : tant que vous n’avez pas ajouté vos propres lignes CREATE STREAM / CREATE TABLE …
-- NON COMMENTÉES, l’exécution « ksql < ce_fichier » ne crée aucun objet (SHOW STREAMS / SHOW TABLES restent vides).
-- Les blocs ci-dessous sont des modèles à copier ou à adapter, pas du SQL exécuté tel quel.
--
-- Les messages sont du JSON (VALUE_FORMAT='JSON'). Les noms de champs dans CREATE STREAM
-- doivent EXACTEMENT correspondre au JSON du producteur (sinon NULL ou échec).

-- Avant les CREATE : décommenter si besoin pour rejouer depuis le début des topics
SET 'auto.offset.reset'='earliest';

-- =============================================================================
-- TODO 1 : STREAM sur vote_events_valid
-- =============================================================================
-- Syntaxe modèle (adapter les types si le producteur envoie d'autres champs) :
--
-- CREATE STREAM IF NOT EXISTS vote_events_valid_stream (
--   vote_id VARCHAR,
--   election_id VARCHAR,
--   event_time VARCHAR,
--   city_code VARCHAR,
--   department_code VARCHAR,      -- OBLIGATOIRE si TODO 6 dept x bloc
--   polling_station_id VARCHAR,
--   candidate_id VARCHAR,
--   candidate_block VARCHAR,      -- OBLIGATOIRE si TODO 6 (aligné sur CSV political_block)
--   channel VARCHAR,
--   signature_ok BOOLEAN,
--   voter_hash VARCHAR,
--   ingestion_ts VARCHAR
-- ) WITH (
--   KAFKA_TOPIC='vote_events_valid',
--   VALUE_FORMAT='JSON'
-- );
--
-- Pièges :
-- - Oublier department_code ou candidate_block -> table dept x bloc vide.
-- - Mauvais nom de colonne JSON -> champ NULL en stream.
--
-- À compléter : votre CREATE STREAM (non commenté) pour vote_events_valid.

-- → Réponse : On mappe le topic Kafka vote_events_valid en stream ksqlDB pour pouvoir l'interroger en SQL.
-- → Explication : VALUE_FORMAT='JSON' dit à ksqlDB que chaque message est du JSON.
--                 Les noms de colonnes (vote_id, candidate_block, etc.) doivent correspondre EXACTEMENT aux clés JSON du producteur.
--                 department_code et candidate_block sont OBLIGATOIRES pour le TODO 6.

CREATE STREAM IF NOT EXISTS vote_events_valid_stream (
  vote_id            VARCHAR,
  election_id        VARCHAR,
  event_time         VARCHAR,
  city_code          VARCHAR,
  city_name          VARCHAR,
  department_code    VARCHAR,   -- OBLIGATOIRE : alimenté par le producteur (codeDepartement)
  region_code        VARCHAR,
  polling_station_id VARCHAR,
  candidate_id       VARCHAR,
  candidate_name     VARCHAR,
  candidate_party    VARCHAR,
  candidate_block    VARCHAR,   -- OBLIGATOIRE : political_block du CSV
  channel            VARCHAR,
  signature_ok       BOOLEAN,
  voter_hash         VARCHAR,
  ingestion_ts       VARCHAR
) WITH (
  KAFKA_TOPIC   = 'vote_events_valid',
  VALUE_FORMAT  = 'JSON'
);

-- =============================================================================
-- TODO 2 : STREAM sur vote_events_rejected
-- =============================================================================
-- Le JSON contient error_reason (ajouté par le validateur). Exemple :
--
-- CREATE STREAM IF NOT EXISTS vote_events_rejected_stream (
--   vote_id VARCHAR,
--   election_id VARCHAR,
--   event_time VARCHAR,
--   city_code VARCHAR,
--   polling_station_id VARCHAR,
--   candidate_id VARCHAR,
--   channel VARCHAR,
--   signature_ok BOOLEAN,
--   voter_hash VARCHAR,
--   ingestion_ts VARCHAR,
--   error_reason VARCHAR
-- ) WITH (
--   KAFKA_TOPIC='vote_events_rejected',
--   VALUE_FORMAT='JSON'
-- );
--
-- À compléter : votre CREATE STREAM pour vote_events_rejected.

-- → Réponse : Même principe que TODO 1, mais sur le topic des rejets ; le champ error_reason est ajouté par le validateur.
-- → Explication : Ce stream est la source pour l'aggregation rejected_by_reason (TODO 5).

CREATE STREAM IF NOT EXISTS vote_events_rejected_stream (
  vote_id            VARCHAR,
  election_id        VARCHAR,
  event_time         VARCHAR,
  city_code          VARCHAR,
  polling_station_id VARCHAR,
  candidate_id       VARCHAR,
  channel            VARCHAR,
  signature_ok       BOOLEAN,
  voter_hash         VARCHAR,
  ingestion_ts       VARCHAR,
  error_reason       VARCHAR   -- ajouté par validator_votes.py
) WITH (
  KAFKA_TOPIC  = 'vote_events_rejected',
  VALUE_FORMAT = 'JSON'
);

-- =============================================================================
-- TODO 3 : TABLE comptage par candidat (flux VALIDE uniquement)
-- =============================================================================
-- Modèle :
-- CREATE TABLE IF NOT EXISTS vote_count_by_candidate AS
-- SELECT
--   candidate_id,
--   AS_VALUE(candidate_id) AS candidate_id_v,
--   COUNT(*) AS total_votes
-- FROM vote_events_valid_stream
-- GROUP BY candidate_id
-- EMIT CHANGES;
--
-- AS_VALUE(...) expose la clé de groupement dans la valeur JSON (utile pour le tableau de bord ou d’autres consommateurs).
--
-- À compléter : votre CREATE TABLE AS SELECT ... vote_count_by_candidate.

-- → Réponse : Une TABLE AS SELECT crée une vue matérialisée qui se met à jour en continu.
-- → Explication : GROUP BY candidate_id produit une ligne par candidat avec le total cumulé.
--                 AS_VALUE(candidate_id) = exporte la clé dans la valeur JSON (sinon elle est dans la clé de message uniquement).
--                 EMIT CHANGES = mode streaming continu (la table se met à jour à chaque nouveau vote).

CREATE TABLE IF NOT EXISTS vote_count_by_candidate AS
SELECT
  candidate_id,
  AS_VALUE(candidate_id) AS candidate_id_v,
  COUNT(*) AS total_votes
FROM vote_events_valid_stream
GROUP BY candidate_id
EMIT CHANGES;

-- =============================================================================
-- TODO 4 : TABLE fenêtre 1 minute par ville + candidat
-- =============================================================================
-- Modèle :
-- CREATE TABLE IF NOT EXISTS vote_count_by_city_minute
-- WITH ( KEY_FORMAT='JSON' ) AS
-- SELECT
--   city_code,
--   candidate_id,
--   AS_VALUE(city_code) AS city_code_v,
--   AS_VALUE(candidate_id) AS candidate_id_v,
--   WINDOWSTART AS window_start,
--   WINDOWEND AS window_end,
--   COUNT(*) AS votes_in_minute
-- FROM vote_events_valid_stream
-- WINDOW TUMBLING (SIZE 1 MINUTE)
-- GROUP BY city_code, candidate_id
-- EMIT CHANGES;
--
-- Le topic Kafka dérivé s'appelle en général VOTE_COUNT_BY_CITY_MINUTE (majuscules) : ne pas le créer à la main.
--
-- À compléter : votre CREATE TABLE ... fenêtre 1 minute (vote_count_by_city_minute).

-- → Réponse : WINDOW TUMBLING (SIZE 1 MINUTE) regroupe les votes par tranches de 60 secondes.
-- → Explication : KEY_FORMAT='JSON' encode la clé composite (city_code+candidate_id+fenêtre) en JSON.
--                 WINDOWSTART / WINDOWEND = epoch ms début/fin de fenêtre, utilisés pour l'axe temporel du dashboard.
--                 AS_VALUE(...) expose city_code et candidate_id dans la valeur (plus facile à lire dans le chargeur).

CREATE TABLE IF NOT EXISTS vote_count_by_city_minute
WITH ( KEY_FORMAT = 'JSON' ) AS
SELECT
  city_code,
  candidate_id,
  AS_VALUE(city_code)     AS city_code_v,
  AS_VALUE(candidate_id)  AS candidate_id_v,
  WINDOWSTART             AS window_start,
  WINDOWEND               AS window_end,
  COUNT(*)                AS votes_in_minute
FROM vote_events_valid_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY city_code, candidate_id
EMIT CHANGES;

-- =============================================================================
-- TODO 5 : TABLE rejets par raison
-- =============================================================================
-- Modèle :
-- CREATE TABLE IF NOT EXISTS rejected_by_reason AS
-- SELECT
--   error_reason,
--   COUNT(*) AS rejected_count
-- FROM vote_events_rejected_stream
-- GROUP BY error_reason
-- EMIT CHANGES;
--
-- À compléter : votre CREATE TABLE ... rejected_by_reason.

-- → Réponse : Cette table cumule le nombre de rejets par type d'erreur (INVALID_SIGNATURE, MISSING_VOTE_ID, UNKNOWN_CANDIDATE).
-- → Explication : Source = vote_events_rejected_stream (topic des rejets produit par le validateur).
--                 Utile pour les KPI du dashboard (élève qualité des données).

CREATE TABLE IF NOT EXISTS rejected_by_reason AS
SELECT
  error_reason,
  AS_VALUE(error_reason)  AS error_reason_v,
  COUNT(*)                AS rejected_count
FROM vote_events_rejected_stream
GROUP BY error_reason
EMIT CHANGES;

-- =============================================================================
-- TODO 6 : TABLE département × bloc (carte / entrepôt)
-- =============================================================================
-- Modèle :
-- CREATE TABLE IF NOT EXISTS vote_count_by_dept_block
-- WITH ( KEY_FORMAT='JSON' ) AS
-- SELECT
--   department_code,
--   candidate_block,
--   AS_VALUE(department_code) AS department_code_v,
--   AS_VALUE(candidate_block) AS block_v,
--   COUNT(*) AS total_votes
-- FROM vote_events_valid_stream
-- WHERE department_code IS NOT NULL AND candidate_block IS NOT NULL
-- GROUP BY department_code, candidate_block
-- EMIT CHANGES;
--
-- Topic dérivé : VOTE_COUNT_BY_DEPT_BLOCK (créé par ksqlDB, pas kafka-topics --create).
-- Si la table est vide : vérifier le producteur (champs null) ou enlever le WHERE temporairement pour debug.
--
-- À compléter : votre CREATE TABLE ... vote_count_by_dept_block.

-- → Réponse : Regroupe les votes par département ET bloc politique pour la carte choropleth du dashboard.
-- → Explication : KEY_FORMAT='JSON' encode la clé composite (department_code, candidate_block) en JSON.
--                 Le WHERE exclut les votes sans département ou sans bloc (données manquantes du producteur).
--                 AS_VALUE() copie les clés dans la valeur pour faciliter la lecture dans le chargeur Cassandra.

CREATE TABLE IF NOT EXISTS vote_count_by_dept_block
WITH ( KEY_FORMAT = 'JSON' ) AS
SELECT
  department_code,
  candidate_block,
  AS_VALUE(department_code) AS department_code_v,
  AS_VALUE(candidate_block) AS block_v,
  COUNT(*)                  AS total_votes
FROM vote_events_valid_stream
WHERE department_code IS NOT NULL AND candidate_block IS NOT NULL
GROUP BY department_code, candidate_block
EMIT CHANGES;

-- =============================================================================
-- TODO 7 : Vérification
-- =============================================================================
-- SHOW TABLES;
-- SHOW QUERIES;
-- SELECT * FROM vote_count_by_candidate LIMIT 20;
-- SELECT * FROM vote_count_by_city_minute LIMIT 20;
-- SELECT * FROM rejected_by_reason LIMIT 20;
-- SELECT * FROM vote_count_by_dept_block LIMIT 20;
--
-- À exécuter en session ksqlDB (fichier ou interactif) : les requêtes ci-dessus pour contrôle (TODO 7).

-- → Réponse : Ces commandes vérifient que tous les objets ont été créés et que les données arrivent.
-- → Explication : SHOW TABLES liste les tables matérialisées ; SHOW QUERIES montre les requêtes en cours.
--                 Si SELECT retourne vide : vérifier que le producteur + validateur tournent.

SHOW STREAMS;
SHOW TABLES;
SHOW QUERIES;

SELECT * FROM vote_count_by_candidate     LIMIT 20;
SELECT * FROM vote_count_by_city_minute   LIMIT 20;
SELECT * FROM rejected_by_reason          LIMIT 20;
SELECT * FROM vote_count_by_dept_block    LIMIT 20;
