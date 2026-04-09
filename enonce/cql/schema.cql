-- Schéma CQL — à compléter (repères TODO ci-dessous)
--
-- ATTENTION : tant que vous n’avez pas ajouté vos propres instructions CQL NON COMMENTÉES
-- (CREATE KEYSPACE, USE, CREATE TABLE…), « cqlsh < ce_fichier » ne crée rien dans Cassandra.
-- Les modèles ci-dessous sont commentés : à recopier ou adapter en code actif.
--
-- Règle : PRIMARY KEY ((partition), clustering…) = ce que vous mettrez dans WHERE en lecture.
-- Pas de JOIN en production : une table par besoin de requête (tableau de bord).

-- =============================================================================
-- TODO 1 : Keyspace
-- =============================================================================
-- Copie conforme attendue :
-- CREATE KEYSPACE IF NOT EXISTS elections
-- WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
--
-- SimpleStrategy = suffisant en local Docker (un seul DC).
--
-- À compléter : votre CREATE KEYSPACE (non commenté).

-- → Réponse : SimpleStrategy avec replication_factor=1 est suffisant en local (un seul nœud Docker).
-- → Explication : Le keyspace est l'espace de nommage Cassandra (équivalent d'un schéma SQL).
--                 SimpleStrategy réplique sur un seul datacenter ; 1 réplique = suffisant en dev.

CREATE KEYSPACE IF NOT EXISTS elections
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- =============================================================================
-- TODO 2 : USE
-- =============================================================================
-- USE elections;
--
-- À compléter : instruction USE pour travailler dans le keyspace elections.

-- → Réponse : USE positionne le contexte sur le keyspace, comme un `\c dbname` en PostgreSQL.
-- → Explication : Toutes les tables CREATE TABLE suivantes seront créées dans ce keyspace.

USE elections;

-- =============================================================================
-- TODO 3 : votes_by_city_minute
-- =============================================================================
-- Lecture cible : « historique par commune, par minute, par candidat »
-- Partition = city_code (un WHERE city_code = '…' par requête).
-- Clustering : minute_bucket (souvent texte epoch ms ou ISO), candidate_id.
--
-- Modèle :
-- CREATE TABLE IF NOT EXISTS votes_by_city_minute (
--   city_code text,
--   minute_bucket text,
--   candidate_id text,
--   votes_count bigint,
--   PRIMARY KEY ((city_code), minute_bucket, candidate_id)
-- ) WITH CLUSTERING ORDER BY (minute_bucket DESC, candidate_id ASC);
--
-- Colonnes à remplir depuis le chargeur : alignées sur les agrégats ksqlDB (ville, fenêtre, candidat, count).
--
-- À compléter : votre CREATE TABLE votes_by_city_minute (partition sur city_code).

-- → Réponse : La partition sur city_code permet un WHERE city_code='...' rapide (O(1)).
-- → Explication : minute_bucket DESC = les minutes récentes d'abord ; candidate_id ASC = tri stable.
--                 votes_count est bigint car les comptages peuvent dépasser 2^31 sur une grande élection.

CREATE TABLE IF NOT EXISTS votes_by_city_minute (
  city_code     text,
  minute_bucket text,
  candidate_id  text,
  votes_count   bigint,
  PRIMARY KEY ((city_code), minute_bucket, candidate_id)
) WITH CLUSTERING ORDER BY (minute_bucket DESC, candidate_id ASC);

-- =============================================================================
-- TODO 4 : votes_by_candidate_city
-- =============================================================================
-- Lecture cible : « pour un candidat, répartition par ville et temps »
-- Partition = candidate_id.
--
-- Modèle :
-- CREATE TABLE IF NOT EXISTS votes_by_candidate_city (
--   candidate_id text,
--   city_code text,
--   minute_bucket text,
--   votes_count bigint,
--   PRIMARY KEY ((candidate_id), city_code, minute_bucket)
-- ) WITH CLUSTERING ORDER BY (city_code ASC, minute_bucket DESC);
--
-- À compléter : votre CREATE TABLE votes_by_candidate_city (partition sur candidate_id).

-- → Réponse : La partition sur candidate_id regroupe tous les votes d'un même candidat ensemble.
-- → Explication : On lit toute la répartition géographique d'un candidat avec un seul WHERE candidate_id='C01'.
--                 city_code ASC + minute_bucket DESC = tri utile pour afficher les communes puis les minutes récentes.

CREATE TABLE IF NOT EXISTS votes_by_candidate_city (
  candidate_id  text,
  city_code     text,
  minute_bucket text,
  votes_count   bigint,
  PRIMARY KEY ((candidate_id), city_code, minute_bucket)
) WITH CLUSTERING ORDER BY (city_code ASC, minute_bucket DESC);

-- =============================================================================
-- TODO 5 : votes_by_department_block
-- =============================================================================
-- Lecture cible : carte / agrégat par département et bloc politique
-- Partition = department_code, clustering = block.
--
-- Modèle :
-- CREATE TABLE IF NOT EXISTS votes_by_department_block (
--   department_code text,
--   block text,
--   votes_count bigint,
--   PRIMARY KEY ((department_code), block)
-- );
--
-- À compléter : votre CREATE TABLE votes_by_department_block (partition sur department_code).

-- → Réponse : Partition sur department_code = une requête WHERE par département pour la carte.
-- → Explication : Pas de CLUSTERING ORDER BY car on lit TOUS les blocs d'un département d'un coup.
--                 Cette table est alimentée par le topic ksqlDB VOTE_COUNT_BY_DEPT_BLOCK.

CREATE TABLE IF NOT EXISTS votes_by_department_block (
  department_code text,
  block           text,
  votes_count     bigint,
  PRIMARY KEY ((department_code), block)
);

-- =============================================================================
-- TODO 6 : SELECT de test (adapter aux données générées)
-- =============================================================================
-- Exemples (décommenter / adapter les codes INSEE et dept. à vos données) :
-- SELECT * FROM elections.votes_by_city_minute WHERE city_code='75056' LIMIT 20;
-- SELECT * FROM elections.votes_by_candidate_city WHERE candidate_id='C01' LIMIT 20;
-- SELECT * FROM elections.votes_by_department_block WHERE department_code='75' LIMIT 20;

-- → Réponse : Ces SELECT vérifient que les 3 tables ont été alimentées correctement.
-- → Explication : Adapter les codes INSEE (75056 = Paris, 75 = département) à vos données réelles.
--                 Si le résultat est vide, vérifier que le producteur, le validateur et le chargeur tournent.

-- Tests de contrôle (à exécuter depuis cqlsh) :
SELECT * FROM elections.votes_by_city_minute        LIMIT 20;
SELECT * FROM elections.votes_by_candidate_city     WHERE candidate_id = 'C01' LIMIT 20;
SELECT * FROM elections.votes_by_department_block   LIMIT 20;
