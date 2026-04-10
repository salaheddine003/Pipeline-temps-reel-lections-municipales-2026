# 🗳️ Kafka Vote Stream — Pipeline Temps Réel Élections Municipales 2026

[![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5-black?logo=apachekafka)](https://kafka.apache.org/)
[![ksqlDB](https://img.shields.io/badge/ksqlDB-0.29-blue)](https://ksqldb.io/)
[![Cassandra](https://img.shields.io/badge/Cassandra-4.1-1287B1?logo=apachecassandra)](https://cassandra.apache.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit)](https://streamlit.io/)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python)](https://python.org/)

Pipeline de **data engineering temps réel** simulant une soirée électorale municipale en France : ingestion événementielle, validation qualité, agrégations continues, stockage analytique et dashboard interactif.

---

## 📋 Table des matières

- [Architecture](#-architecture)
- [Technologies](#-technologies)
- [Structure du projet](#-structure-du-projet)
- [Prérequis](#-prérequis)
- [Installation & Lancement](#-installation--lancement)
- [Pipeline de données](#-pipeline-de-données)
- [Dashboard](#-dashboard)
- [Équipe](#-équipe)

---

## 🏗️ Architecture

```
┌─────────────┐     ┌─────────────────┐     ┌──────────────┐
│  Producteur │────▶│   Kafka Broker   │────▶│  Validateur  │
│   Python    │     │ vote_events_raw  │     │    Python    │
└─────────────┘     └─────────────────┘     └──────┬───────┘
                                                   │
                                    ┌──────────────┴──────────────┐
                                    ▼                             ▼
                          vote_events_valid            vote_events_rejected
                                    │                             │
                                    ▼                             ▼
                            ┌──────────────┐            ┌────────────────┐
                            │    ksqlDB    │            │  rejected_by   │
                            │  4 tables    │            │    _reason     │
                            │ matérialisées│            └────────────────┘
                            └──────┬───────┘
                                   │
                                   ▼
                          ┌─────────────────┐
                          │   Cassandra     │
                          │   3 tables      │
                          │  dénormalisées  │
                          └──────┬──────────┘
                                 │
                                 ▼
                        ┌──────────────────┐
                        │    Streamlit     │
                        │   Dashboard     │
                        │  + Carte France │
                        └──────────────────┘
```

---

## 🛠️ Technologies

| Composant | Technologie | Port | Rôle |
|-----------|-------------|------|------|
| Broker | Apache Kafka 7.5 | 9092 | Bus de messages événementiel |
| Stream processing | ksqlDB | 8088 | Agrégations SQL continues |
| Base de données | Apache Cassandra 4.1 | 9042 | Tables dénormalisées lecture rapide |
| Dashboard | Streamlit + Plotly + Folium | 8501 | Visualisation temps réel |
| Monitoring | Kafka UI | 8080 | Inspection topics/messages |
| Schema Registry | Confluent Schema Registry | 8081 | Registre de schémas |
| Orchestration | Docker Compose | — | 7 services conteneurisés |

---

## 📁 Structure du projet

```
Kafka-vote-stream-main/
├── docker-compose.yml              # 7 services Docker
├── data/
│   ├── candidates.csv              # 10 candidats (C01-C10), partis, blocs
│   ├── communes_fr.json            # Référentiel INSEE ~35 000 communes
│   ├── communes_fr_sample.json     # Échantillon pour tests rapides
│   └── votes_municipales_sample.jsonl
├── enonce/
│   ├── requirements.txt            # Dépendances Python
│   ├── cql/
│   │   └── schema.cql              # Keyspace + 3 tables Cassandra
│   ├── sql/
│   │   └── ksqldb_queries.sql      # 2 streams + 4 tables matérialisées
│   └── src/
│       ├── producer_votes.py       # Générateur d'événements votes
│       ├── validator_votes.py      # Validation qualité (3 règles)
│       ├── load_to_cassandra.py    # Chargeur ksqlDB → Cassandra
│       ├── dashboard_streamlit.py  # Dashboard interactif
│       └── generate_votes_data.py  # Génération données initiales
└── README.md
```

---

## ⚙️ Prérequis

- **Docker Desktop** avec **≥ 8 Go RAM** alloués
- **Python 3.10+** (testé avec 3.12)
- **Git** installé

---

## 🚀 Installation & Lancement

### 1. Cloner le repo

```bash
git clone https://github.com/<votre-user>/Kafka-vote-stream.git
cd Kafka-vote-stream
```

### 2. Démarrer l'infrastructure Docker

```bash
docker compose up -d
```

Vérifier que les 7 services tournent :
```bash
docker compose ps
```

### 3. Installer les dépendances Python

```bash
python -m venv .venv
# Linux/Mac : source .venv/bin/activate
# Windows   : .venv\Scripts\Activate.ps1

pip install -r enonce/requirements.txt
pip install pyasyncore   # Fix Python 3.12 + Cassandra
```

### 4. Créer les topics Kafka

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic vote_events_raw --partitions 3
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic vote_events_valid --partitions 3
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic vote_events_rejected --partitions 3
```

### 5. Appliquer le schéma Cassandra

```bash
# Linux/Mac
docker exec -i cassandra cqlsh < enonce/cql/schema.cql

# Windows PowerShell
Get-Content enonce\cql\schema.cql | docker exec -i cassandra cqlsh
```

### 6. Appliquer les requêtes ksqlDB

```bash
docker cp enonce/sql/ksqldb_queries.sql ksqldb:/tmp/queries.sql
docker exec ksqldb ksql --file /tmp/queries.sql http://localhost:8088
```

### 7. Lancer le pipeline (4 terminaux)

```bash
# Terminal 1 — Producteur
python enonce/src/producer_votes.py

# Terminal 2 — Validateur
python enonce/src/validator_votes.py

# Terminal 3 — Chargeur Cassandra
python enonce/src/load_to_cassandra.py

# Terminal 4 — Dashboard
streamlit run enonce/src/dashboard_streamlit.py
```

### 8. Accéder aux interfaces

| Interface | URL |
|-----------|-----|
| **Dashboard Streamlit** | http://localhost:8501 |
| **Kafka UI** | http://localhost:8080 |
| **ksqlDB** | http://localhost:8088 |

---

## 🔄 Pipeline de données

### Producteur (`producer_votes.py`)
- Génère des événements JSON simulant des votes réels
- Clé Kafka = `city_code` (partitionnement par commune)
- ~8 % de votes invalides injectés volontairement
- Configurable : `MAX_MESSAGES`, `START_DELAY_MS`

### Validateur (`validator_votes.py`)
3 règles de validation dans l'ordre :
1. **INVALID_SIGNATURE** — `signature_ok` doit être `True`
2. **MISSING_VOTE_ID** — `vote_id` non vide
3. **UNKNOWN_CANDIDATE** — candidat existant dans `candidates.csv`

### ksqlDB — Agrégations temps réel
| Table | Agrégation |
|-------|-----------|
| `vote_count_by_candidate` | COUNT par candidat |
| `vote_count_by_city_minute` | COUNT par commune × candidat, fenêtre TUMBLING 1 min |
| `rejected_by_reason` | COUNT par motif de rejet |
| `vote_count_by_dept_block` | COUNT par département × bloc politique |

### Cassandra — 3 tables dénormalisées
| Table | Clé de partition | Usage |
|-------|-----------------|-------|
| `votes_by_city_minute` | city_code | Série temporelle |
| `votes_by_candidate_city` | candidate_id | Classement candidats |
| `votes_by_department_block` | department_code | Carte de France |

---

## 📊 Dashboard

Le dashboard Streamlit (http://localhost:8501) affiche en temps réel les résultats électoraux. Les données se rafraîchissent automatiquement toutes les 30 secondes.

### 1. Répartition par bloc politique
Graphique en barres horizontales affichant les votes cumulés par bloc : **Gauche** (rouge foncé), **Droite** (bleu marine), **Centre** (orange/doré), **Droite nationale** (bleu), **Extrême gauche** (rouge), **Autre** (gris). Les 3 premiers blocs (Gauche, Droite, Centre) sont au coude-à-coude autour de ~50k votes chacun.

![Répartition par bloc politique](https://github.com/user-attachments/assets/36bfb56e-f439-4955-895e-913a53e6b8b9)

### 2. Répartition par parti / liste
Graphique en barres classant les 10 partis par nombre de votes. **LR** (bleu) domine avec ~51k votes, suivi de **PS** (rouge), **ECO** (vert), **REN** (jaune), **MDM** (orange), **LFI** (rouge foncé) autour de ~25k chacun. **RN** et **UDR** (bleus) suivent à ~18k, puis **SE** (gris) et **REG** (vert foncé) à ~8k.

### 3. Évolution temporelle des votes
Courbe chronologique (axe X = heure UTC, axe Y = nombre de votes) montrant l'évolution minute par minute. Un pic initial massif (~220k votes) lors du lancement du producteur, puis stabilisation avec des petits volumes lors des relances successives.

### 4. Carte de France — Bloc politique dominant par département
Carte interactive Leaflet/Folium de la France métropolitaine. Chaque département est coloré selon son **bloc politique dominant** :
- 🔴 Rouge/rose = Gauche ou Extrême gauche
- 🟠 Orange/doré = Centre
- 🔵 Bleu = Droite ou Droite nationale
- ⚪ Gris = Autre

L'opacité est proportionnelle à la part du bloc leader. Au survol, un tooltip affiche le nom et code du département. Une légende en bas à gauche indique la correspondance couleur ↔ bloc.

### 5. Top communes & classement candidats
Deux graphiques côte à côte en barres horizontales :
- **Top 10 communes** les plus actives (ex : Valmeinier, Septmoncel les Molunes, Saint-Lubin-de-la-Haye...) avec ~15-20 votes chacune
- **Classement des candidats** avec le nom complet de la liste (ex : "Liste Les Républicains Ville" en tête à ~50k, suivi des listes Socialiste, Écologiste, Renaissance, MoDem, LFI autour de ~25k)

---

## 👥 Équipe

| Nom | Email |
|-----|-------|
| Thanina Bellahsene | thaninabellahsene2208@gmail.com |
| Salaheddine Abbar | salaheddine.abbar@gmail.com |

---

## 📄 Licence

Projet académique — Usage éducatif uniquement.
