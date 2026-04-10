# 🗳️ Présentation — Kafka Vote Stream

## Pipeline Temps Réel · Élections Municipales 2026

---

## 🎯 Objectif du projet

Construire un pipeline **end-to-end** de traitement de données en temps réel simulant une soirée électorale municipale en France.

**Problématique :** Comment ingérer, valider, agréger et visualiser des centaines de milliers de votes en temps réel avec une architecture distribuée ?

---

## 🏗️ Architecture technique

```
Producteur  →  Kafka (3 topics)  →  Validateur  →  ksqlDB  →  Cassandra  →  Dashboard
```

**7 services Docker** orchestrés :
- **Kafka + Zookeeper** — Bus de messages événementiel
- **ksqlDB** — Agrégations SQL sur flux
- **Cassandra** — Stockage dénormalisé
- **Schema Registry + Connect** — Écosystème Confluent
- **Kafka UI** — Supervision

---

## 📝 Jeu de données

- **10 candidats** répartis en 5 partis et 3 blocs politiques (Gauche, Centre, Droite)
- **~35 000 communes** françaises (référentiel INSEE)
- **~8 % de votes invalides** injectés volontairement pour tester la validation

---

## 🔗 Étapes du pipeline

### 1. Producteur (`producer_votes.py`)
- Génère des événements JSON avec `confluent-kafka`
- Clé = `city_code` → garantit l'ordre par commune
- Buffer élargi (500 000 messages) + flush périodique
- Callback de livraison pour traçabilité

### 2. Validateur (`validator_votes.py`)
- Consomme `vote_events_raw`
- 3 règles de validation :
  - Signature valide ?
  - ID de vote présent ?
  - Candidat reconnu ?
- Route vers `vote_events_valid` ou `vote_events_rejected`
- **Résultat :** ~92 % valides, ~8 % rejetés

### 3. Agrégation ksqlDB
- 2 **streams** sur les topics validated/rejected
- 4 **tables matérialisées** avec fenêtres TUMBLING de 1 minute
- Requêtes persistantes (CTAS) qui tournent en continu

### 4. Chargeur Cassandra (`load_to_cassandra.py`)
- Consomme les topics ksqlDB via `__confluent-ksql-*`
- 3 tables dénormalisées pour lectures rapides
- Insertions asynchrones (`execute_async`)

### 5. Dashboard Streamlit (`dashboard_streamlit.py`)
- Rafraîchissement auto toutes les 30 s
- KPIs, graphiques Plotly, carte Folium de France

---

## 📊 Dashboard — Ce qu'on visualise

| Visualisation | Description |
|---------------|-------------|
| **KPIs** | Total votes valides, candidat en tête, taux de participation |
| **Répartition par bloc** | Barres horizontales Gauche / Centre / Droite |
| **Répartition par parti** | 5 partis avec couleurs politiques |
| **Série temporelle** | Évolution minute par minute |
| **Carte de France** | Département coloré par bloc politique dominant (Leaflet) |
| **Top 10 communes** | Communes les plus actives |
| **Classement candidats** | Votes cumulés par candidat |

---

## 🗺️ Focus : Carte de France

- Utilise **Folium + GeoJSON** des départements
- Couleur = bloc politique dominant (rouge/orange/bleu)
- Opacité proportionnelle à la part du bloc leader
- Tooltip interactif avec détails par département
- Légende HTML

---

## ⚡ Chiffres clés

| Métrique | Valeur |
|----------|--------|
| Messages produits | ~52 000+ |
| Taux de validation | ~92 % |
| Messages rejetés | ~8 % |
| Tables Cassandra | 3 |
| Tables ksqlDB | 4 (persistent queries) |
| Latence bout en bout | ~secondes |
| Services Docker | 7 |

---

## 🧪 Défis rencontrés & solutions

| Problème | Solution |
|----------|----------|
| `BufferError: Queue full` | try/except + poll(1s) retry + buffer 500K |
| `asyncore removed Python 3.12` | Backport pyasyncore |
| Map avec gradient numérique | GeoJson + style_function couleurs discrètes |
| Graphique plat avec 1 point | Détection : barchart si ≤3 points |
| Mapping champs ksqlDB → CQL | Normalisation UPPER→lower dans le loader |

---

## 🔧 Stack technique complète

| Couche | Outils |
|--------|--------|
| Messaging | Apache Kafka 7.5 (Confluent) |
| Stream processing | ksqlDB 0.29 |
| Base de données | Apache Cassandra 4.1 |
| Langage | Python 3.12 |
| Client Kafka | confluent-kafka |
| Client Cassandra | cassandra-driver |
| Dashboard | Streamlit |
| Graphiques | Plotly Express |
| Cartographie | Folium + streamlit-folium |
| Conteneurisation | Docker Compose |

---

## 🎬 Démo live

1. `docker compose up -d` — Infrastructure
2. Lancer producteur → validateur → loader → dashboard
3. Ouvrir http://localhost:8501
4. Observer les données évoluer en temps réel

---

## 👥 Équipe

- **Thanina Bellahsene** — thaninabellahsene2208@gmail.com
- **Salaheddine Abbar** — salaheddine.abbar@gmail.com

---

*Projet académique — Pipeline Kafka temps réel*
