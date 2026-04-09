# Présentation TP — Pipeline temps réel : Élections municipales 2026
### Durée : 10 minutes | 2 présentateurs

---

## PLAN DE PASSAGE

| Temps       | Qui          | Section                                      |
|-------------|--------------|----------------------------------------------|
| 0:00 – 1:30 | Personne A  | Introduction + contexte métier               |
| 1:30 – 4:00 | Personne A  | Architecture technique (schéma global)       |
| 4:00 – 5:30 | Personne B  | Traitement des données (ksqlDB + validation) |
| 5:30 – 7:00 | Personne B  | Stockage Cassandra (modélisation)            |
| 7:00 – 9:00 | Personne A  | Démo live du dashboard                       |
| 9:00 – 10:00| Personne B  | Bilan + difficultés + conclusion             |

---

## SLIDE 1 — Introduction (Personne A, ~1 min 30)

**Titre :** *Pipeline temps réel — Élections municipales 2026*

**Script :**

> Bonjour, nous allons vous présenter notre TP de data engineering
> sur la mise en œuvre d'un pipeline temps réel de traitement de votes.
>
> **Le contexte métier** : simuler une soirée électorale municipale en France.
> Des bureaux de vote envoient des bulletins un par un en temps réel.
> Notre objectif : ingérer, valider, agréger et visualiser ces votes
> en continu, en quelques secondes de bout en bout.
>
> **Les contraintes** :
> - Volume : des milliers de votes par minute sur ~35 000 communes
> - Qualité : environ 8 % de votes invalides (signature, ID manquant, candidat inconnu)
> - 10 candidats répartis dans 6 blocs politiques (extrême gauche → droite nationale)
> - Données issues d'un fichier `candidates.csv` et du référentiel INSEE des communes

---

## SLIDE 2 — Architecture globale (Personne A, ~2 min 30)

**Titre :** *Architecture du pipeline événementiel*

**Afficher le schéma :**

```
  Producteur           Kafka              Validateur           Kafka
  (Python)    →   vote_events_raw    →    (Python)    →   vote_events_valid
                                                       →   vote_events_rejected
                                               ↓
                                           ksqlDB
                                      (agrégations temps réel)
                                               ↓
                                     4 tables matérialisées
                                               ↓
                                    Chargeur Cassandra (Python)
                                               ↓
                                     3 tables Cassandra
                                               ↓
                                    Dashboard Streamlit (Python)
```

**Script :**

> Notre architecture repose sur **7 services Docker** orchestrés par Docker Compose :
>
> 1. **Le producteur** (`producer_votes.py`) génère des événements JSON
>    simulant des votes réels. Chaque événement contient : vote_id, commune,
>    département, candidat, bloc politique, timestamp, signature.
>    La clé Kafka est le `city_code` → tous les votes d'une commune
>    vont dans la même partition, ce qui garantit l'ordre par zone.
>
> 2. **Le broker Kafka** (Confluent 7.5.0) reçoit les messages dans le topic
>    `vote_events_raw` avec 3 partitions.
>
> 3. **Le validateur** (`validator_votes.py`) consomme ce topic et applique
>    3 règles de validation dans l'ordre :
>    - `INVALID_SIGNATURE` : signature_ok doit être True
>    - `MISSING_VOTE_ID` : vote_id ne doit pas être vide
>    - `UNKNOWN_CANDIDATE` : le candidat doit exister dans notre référentiel
>
>    Les votes valides vont dans `vote_events_valid`,
>    les rejetés dans `vote_events_rejected` avec le motif d'erreur.
>
> 4. **ksqlDB** traite les flux en continu avec 4 agrégations matérialisées.
>
> 5. **Le chargeur Cassandra** consomme les topics ksqlDB et insère dans 3 tables.
>
> 6. **Le dashboard Streamlit** lit Cassandra et affiche les résultats en temps réel.

---

## SLIDE 3 — Traitement temps réel ksqlDB (Personne B, ~1 min 30)

**Titre :** *Agrégations continues avec ksqlDB*

**Script :**

> ksqlDB est le moteur de traitement de flux. Nous avons défini :
>
> **2 streams** (lectures des topics Kafka) :
> - `vote_events_valid_stream` → lit `vote_events_valid`
> - `vote_events_rejected_stream` → lit `vote_events_rejected`
>
> **4 tables matérialisées** (CREATE TABLE AS SELECT) :
>
> | Table                       | Agrégation                                    |
> |-----------------------------|-----------------------------------------------|
> | `vote_count_by_candidate`   | COUNT(*) GROUP BY candidate_id                |
> | `vote_count_by_city_minute` | COUNT(*) GROUP BY city, candidate WINDOW 1 min |
> | `rejected_by_reason`        | COUNT(*) GROUP BY error_reason                 |
> | `vote_count_by_dept_block`  | COUNT(*) GROUP BY département, bloc politique  |
>
> La table `vote_count_by_city_minute` utilise une **fenêtre glissante
> TUMBLING de 1 minute** — c'est ce qui nous permet de tracer
> l'évolution des votes minute par minute sur le dashboard.
>
> Le point clé : ces tables se mettent à jour **automatiquement**
> à chaque nouveau message. Pas de batch, pas de cron — c'est du vrai temps réel.

---

## SLIDE 4 — Modélisation Cassandra (Personne B, ~1 min 30)

**Titre :** *Stockage orienté requêtes — Cassandra*

**Script :**

> Cassandra est notre couche de service. La modélisation suit le principe
> fondamental de Cassandra : **une table par requête**.
>
> Nous avons 3 tables dans le keyspace `elections` :
>
> | Table                        | Clé de partition    | Clustering           | Usage dashboard              |
> |------------------------------|---------------------|----------------------|------------------------------|
> | `votes_by_city_minute`       | city_code           | minute_bucket, cand  | Série temporelle par commune |
> | `votes_by_candidate_city`    | candidate_id        | city_code, minute    | Classement des candidats     |
> | `votes_by_department_block`  | department_code     | block                | Carte de France par bloc     |
>
> **Choix de conception** :
> - `votes_by_city_minute` et `votes_by_candidate_city` contiennent les **mêmes données**
>   mais avec des clés de partition différentes → accès O(1) par commune OU par candidat
> - Le `minute_bucket` est un timestamp epoch en millisecondes (issu de ksqlDB WINDOWSTART)
> - `votes_by_department_block` agrège par département × bloc politique pour la carte choroplèthe
>
> Le chargeur Python consomme les topics ksqlDB avec des **prepared statements**
> (INSERT ... VALUES (?,?,?,?)) pour la performance et la sécurité contre les injections CQL.

---

## SLIDE 5 — Démo live (Personne A, ~2 min)

**Titre :** *Dashboard temps réel — Streamlit*

**Ouvrir http://localhost:8501 et montrer :**

**Script :**

> Voici notre dashboard Streamlit qui se rafraîchit toutes les 30 secondes.
>
> **En haut** : les KPI — nombre total de votes agrégés, et le candidat en tête.
>
> **Graphique 1 — Votes par bloc politique** :
> On voit la répartition entre les 6 blocs : extrême gauche, gauche, centre,
> droite, droite nationale, autre. Les couleurs respectent les conventions
> politiques françaises (rouge = gauche, bleu = droite, orange = centre).
>
> **Graphique 2 — Votes par parti** :
> Plus granulaire — les 10 partis avec leurs couleurs propres.
> On distingue par exemple LFI en rouge foncé, ECO en vert, REN en jaune.
>
> **Graphique 3 — Évolution par minute** :
> La courbe montre le nombre de votes traités par fenêtre d'1 minute.
> Quand on a peu de points, un bar chart s'affiche ; avec plus de données,
> c'est une courbe avec marqueurs.
>
> **La carte Leaflet** :
> C'est la pièce maîtresse. Chaque département est coloré selon
> son **bloc politique dominant** — celui qui a le plus de votes.
> L'intensité de la couleur est proportionnelle à la part du bloc leader.
> Au survol, on voit le nom et le code du département.
> La légende en bas à gauche indique la correspondance couleur ↔ bloc.
>
> **En bas** : les classements — top 10 communes et top candidats en barres horizontales.
>
> *(Montrer Kafka UI sur http://localhost:8080 brièvement — les topics, les messages)*

---

## SLIDE 6 — Bilan et conclusion (Personne B, ~1 min)

**Titre :** *Bilan technique et difficultés rencontrées*

**Script :**

> **Ce qui fonctionne** :
> - Pipeline complet de bout en bout : producteur → validateur → ksqlDB → Cassandra → dashboard
> - ~8 % de votes rejetés (conforme à la spec), 3 motifs de rejet identifiés
> - Carte de France interactive avec couleurs politiques par département
> - Agrégations temps réel avec fenêtres temporelles d'1 minute
>
> **Les difficultés rencontrées** :
> - **BufferError Kafka** : le producteur saturait la queue interne.
>   Solution : try/except avec retry + configuration buffer élargie (500K messages).
> - **Cassandra + Python 3.12** : le module `asyncore` a été supprimé en Python 3.12.
>   Solution : installation du backport `pyasyncore`.
> - **ksqlDB clés en MAJUSCULES** : les champs JSON dans les topics dérivés ksqlDB
>   sont en MAJUSCULES. Il faut couvrir les deux casses dans le chargeur.
> - **Carte Choropleth** : `folium.Choropleth` ne supporte que des gradients numériques.
>   On est passé à `folium.GeoJson` avec une `style_function` pour des couleurs discrètes par bloc.
>
> **Ce qu'on aurait pu ajouter** :
> - Kafka Connect au lieu d'un chargeur Python manuel
> - Schema Registry avec Avro pour typer les messages
> - Auto-refresh WebSocket au lieu du polling 30s Streamlit
>
> Merci pour votre attention. Des questions ?

---

## AIDE-MÉMOIRE — Commandes de démo

```bash
# Infrastructure
docker compose up -d

# Créer les topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic vote_events_raw --partitions 3
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic vote_events_valid --partitions 3
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic vote_events_rejected --partitions 3

# Schema Cassandra
Get-Content enonce\cql\schema.cql | docker exec -i cassandra cqlsh

# Requêtes ksqlDB
docker cp enonce\sql\ksqldb_queries.sql ksqldb:/tmp/queries.sql
docker exec ksqldb ksql --file /tmp/queries.sql http://localhost:8088

# Lancer le pipeline
python enonce\src\producer_votes.py          # Terminal 1
python enonce\src\validator_votes.py         # Terminal 2
python enonce\src\load_to_cassandra.py       # Terminal 3
streamlit run enonce\src\dashboard_streamlit.py  # Terminal 4

# URLs
# Dashboard : http://localhost:8501
# Kafka UI  : http://localhost:8080
```

---

## RÉSUMÉ TECHNIQUE (anti-sèche)

| Composant        | Technologie         | Port  | Rôle                                      |
|------------------|---------------------|-------|--------------------------------------------|
| Broker           | Apache Kafka 7.5    | 9092  | Bus de messages événementiel               |
| Stream processor | ksqlDB              | 8088  | Agrégations SQL continues                  |
| Base de données  | Apache Cassandra 4.1| 9042  | Tables dénormalisées lecture rapide         |
| Dashboard        | Streamlit + Plotly   | 8501  | Visualisation temps réel                   |
| Monitoring       | Kafka UI             | 8080  | Inspection topics/messages/consumer groups  |
| Producteur       | Python confluent-kafka| —   | Génération événements vote                 |
| Validateur       | Python confluent-kafka| —   | Filtrage qualité (3 règles)                |
| Chargeur         | Python cassandra-driver| —  | Insertion Cassandra depuis ksqlDB          |
| Carte            | Folium + Leaflet     | —    | Choroplèthe politique par département      |
