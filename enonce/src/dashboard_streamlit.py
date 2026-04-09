#!/usr/bin/env python3
"""Tableau de bord Streamlit -- lecture Cassandra (implementation complete)."""

import csv
import json
import requests
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.express as px
import folium
from streamlit_folium import st_folium
from cassandra.cluster import Cluster

_ROOT = Path(__file__).resolve().parent.parent.parent
CANDIDATES_FILE = _ROOT / "data" / "candidates.csv"
COMMUNES_FILE   = _ROOT / "data" / "communes_fr.json"
SAMPLE_COMMUNES = _ROOT / "data" / "communes_fr_sample.json"
GEOJSON_URL = "https://france-geojson.gregoiredavid.fr/repo/departements.geojson"

st.set_page_config(page_title="Municipales 2026", layout="wide")
st.title("Municipales 2026 - Dashboard France")

# =============================================================================
# TODO 1 -- Connexion + lectures Cassandra
# =============================================================================
# 1) Cluster(["127.0.0.1"]) puis session = cluster.connect("elections")
# 2) Executer des SELECT sur les 3 tables (colonnes alignees sur schema.cql) :
#      votes_by_city_minute        -> city_code, minute_bucket, candidate_id, votes_count
#      votes_by_candidate_city     -> candidate_id, city_code, minute_bucket, votes_count
#      votes_by_department_block   -> department_code, block, votes_count   (peut etre vide au debut)
# 3) Option KPI votes valides / rejetes : lecture des topics ksqlDB compacts VOTE_COUNT_BY_CANDIDATE
#    et REJECTED_BY_REASON, ou approximation depuis les DataFrames si vous manquez de temps.
# 4) Si tables vides : afficher st.warning et st.stop() pour eviter des graphiques vides trompeurs.
#
# -> Reponse : Connexion avec st.cache_resource (singleton) + lecture des 3 tables avec cache TTL 30 s.
# -> Explication : @st.cache_resource garde la connexion Cassandra ouverte entre les re-runs Streamlit.
#                  @st.cache_data(ttl=30) recharge les donnees toutes les 30 secondes automatiquement.


@st.cache_resource
def _get_session():
    """Cree (une seule fois) la connexion Cassandra."""
    cluster = Cluster(["127.0.0.1"])
    return cluster.connect("elections")


try:
    session = _get_session()
except Exception as exc:
    st.error(f"Impossible de se connecter a Cassandra : {exc}")
    st.info(
        "Verifiez que le conteneur est demarre : docker compose up cassandra\n"
        "et que le schema a ete applique : "
        "docker exec -i cassandra cqlsh < enonce/cql/schema.cql"
    )
    st.stop()

# =============================================================================
# TODO 2 -- DataFrames
# =============================================================================
# Pour chaque jeu de resultats Cassandra : listes de dicts puis pd.DataFrame(...).
# Forcer votes_count en numerique : pd.to_numeric(df["votes_count"], errors="coerce").fillna(0)
# Si une table est vide mais l'autre non : repli / fallback pour ne pas casser l'app.
#
# -> Reponse : Fonction _load_cassandra_data() avec cache TTL 30 s ; conversion explicite en int.
# -> Explication : Le driver Cassandra retourne des types Python natifs, mais votes_count (bigint)
#                  peut arriver comme str dans certaines versions -- pd.to_numeric() couvre les deux.


@st.cache_data(ttl=30)
def _load_cassandra_data() -> tuple:
    """Lit les 3 tables Cassandra, retourne 3 DataFrames."""
    def _q(cql, cols):
        rows    = session.execute(cql)
        records = [{c: getattr(r, c, None) for c in cols} for r in rows]
        df = pd.DataFrame(records, columns=cols) if records else pd.DataFrame(columns=cols)
        if "votes_count" in df.columns:
            df["votes_count"] = pd.to_numeric(df["votes_count"], errors="coerce").fillna(0).astype(int)
        return df

    return (
        _q(
            "SELECT city_code, minute_bucket, candidate_id, votes_count "
            "FROM votes_by_city_minute LIMIT 200000",
            ["city_code", "minute_bucket", "candidate_id", "votes_count"],
        ),
        _q(
            "SELECT candidate_id, city_code, minute_bucket, votes_count "
            "FROM votes_by_candidate_city LIMIT 200000",
            ["candidate_id", "city_code", "minute_bucket", "votes_count"],
        ),
        _q(
            "SELECT department_code, block, votes_count "
            "FROM votes_by_department_block LIMIT 10000",
            ["department_code", "block", "votes_count"],
        ),
    )


df_city_minute, df_candidate_city, df_dept_block = _load_cassandra_data()

# Arret propre si les deux tables principales sont vides
if df_city_minute.empty and df_candidate_city.empty:
    st.warning(
        "Les tables Cassandra sont vides.\n"
        "Attendez que le pipeline soit demarre : "
        "producteur -> validateur -> ksqlDB -> chargeur Cassandra."
    )
    st.stop()

# =============================================================================
# TODO 3 -- Bloc / parti (candidates.csv)
# =============================================================================
# Charger data/candidates.csv : pour chaque candidate_id recuperer political_block -> colonne "block"
# et party -> colonne "party" sur le DataFrame des votes (map ou merge).
# Regle sujet : pour les graphiques PAR BLOC, ECO / ecologiste est rattache au bloc "gauche"
# (normaliser avant groupby : si block == "ecologiste" -> "gauche" pour ce graphe uniquement).
# Ne pas melanger les conventions "bloc" et "parti" sur un meme graphique.
#
# -> Reponse : Lecture CSV via csv.DictReader ; deux dicts de mapping candidate_id -> block et party.
# -> Explication : On utilise .map() sur la colonne candidate_id du DataFrame pour ajouter
#                  les colonnes "block" et "party" sans merge (plus rapide sur petits jeux).
#                  ECO est deja dans le bloc "gauche" dans candidates.csv (pas de normalisation
#                  supplementaire). POLITICAL_BLOCK original remplace par une vraie lecture CSV.


@st.cache_data
def _load_candidates() -> dict:
    """Retourne un dict candidate_id -> {block, party, name}."""
    mapping: dict = {}
    if CANDIDATES_FILE.exists():
        with CANDIDATES_FILE.open("r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                mapping[row["candidate_id"]] = {
                    "block": row["political_block"],
                    "party": row["party"],
                    "name":  row.get("candidate_name", row["candidate_id"]),
                }
    return mapping


candidates_map = _load_candidates()


def _add_candidate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute les colonnes block, party et candidate_name a partir de candidates_map."""
    df = df.copy()
    df["block"]          = df["candidate_id"].map(lambda c: candidates_map.get(c, {}).get("block", "autre"))
    df["party"]          = df["candidate_id"].map(lambda c: candidates_map.get(c, {}).get("party", c))
    df["candidate_name"] = df["candidate_id"].map(lambda c: candidates_map.get(c, {}).get("name", c))
    return df


df_candidate_city = _add_candidate_columns(df_candidate_city)
df_city_minute    = _add_candidate_columns(df_city_minute)

# Labels et couleurs officiels pour les blocs politiques
BLOC_LABELS = {
    "extreme_gauche":   "Extreme gauche",
    "gauche":           "Gauche",
    "centre":           "Centre",
    "droite":           "Droite",
    "droite_nationale": "Droite nationale",
    "autre":            "Autre",
}
BLOC_COLORS = {
    "Extreme gauche":   "#8B0000",
    "Gauche":           "#E3001B",
    "Centre":           "#FF9900",
    "Droite":           "#003189",
    "Droite nationale": "#0D2680",
    "Autre":            "#808080",
}
PARTY_COLORS = {
    "LFI": "#8B0000", "PS":  "#E3001B", "ECO": "#009900",
    "REN": "#FFCC00", "MDM": "#FF9900", "LR":  "#003189",
    "RN":  "#0D2680", "UDR": "#1A237E", "SE":  "#808080",
    "REG": "#006666",
}

# =============================================================================
# KPI -- remplacer par des valeurs calculees
# -> Reponse : total valides = somme de votes_count dans df_candidate_city.
#              Top candidat = candidate avec le plus grand total de votes.
#              Votes rejetes non stockes en Cassandra -> N/A.
# =============================================================================

total_valid = int(df_candidate_city["votes_count"].sum()) if not df_candidate_city.empty else 0
top_cand_id = (
    df_candidate_city.groupby("candidate_id")["votes_count"].sum().idxmax()
    if not df_candidate_city.empty else "N/A"
)
top_cand_name = candidates_map.get(top_cand_id, {}).get("name", top_cand_id) if top_cand_id != "N/A" else "N/A"

col1, col2, col3, col4 = st.columns(4)
col1.metric("Votes valides (agr.)", f"{total_valid:,}")
col2.metric("Votes rejetes",        "N/A *")
col3.metric("Taux rejet",           "N/A *")
col4.metric("Top candidat",         top_cand_name)
st.caption("* Votes rejetes non stockes en Cassandra. Consultez la table ksqlDB rejected_by_reason.")

# =============================================================================
# TODO 4 -- Bar chart par bloc
# =============================================================================
# df_candidate_city.groupby("block")["votes_count"].sum() -> reset_index
# px.bar(..., x=nom_affiche_bloc, y=votes_count, color="block", color_discrete_map={...})
# Legendes lisibles (extreme_gauche, gauche, ...).
#
# -> Reponse : groupby block + mapping BLOC_LABELS pour les legendes + BLOC_COLORS pour les couleurs.
# -> Explication : color_discrete_map force les couleurs politiques coherentes avec les conventions
#                  francaises. Ce graphique est distinct du graphe par parti (TODO 4bis) : les
#                  agregations et la palette sont differentes.

st.subheader("Votes par bloc politique")

df_bloc = (
    df_candidate_city
    .groupby("block", as_index=False)["votes_count"]
    .sum()
    .sort_values("votes_count", ascending=False)
)
df_bloc["bloc_label"] = df_bloc["block"].map(lambda b: BLOC_LABELS.get(b, b))

if not df_bloc.empty:
    fig_bloc = px.bar(
        df_bloc,
        x="bloc_label", y="votes_count",
        color="bloc_label",
        color_discrete_map=BLOC_COLORS,
        labels={"bloc_label": "Bloc politique", "votes_count": "Votes"},
        title="Repartition des votes valides par bloc politique",
    )
    fig_bloc.update_layout(showlegend=False)
    st.plotly_chart(fig_bloc, use_container_width=True)
else:
    st.info("Aucune donnee disponible pour le graphique par blocs.")

# =============================================================================
# TODO 4bis -- Bar chart par parti (nuance)
# =============================================================================
# groupby("party") ; couleurs par parti (ex. ECO vert) -- PAS les memes couleurs que le graphe par bloc.
# Deux graphiques distincts obligatoires.
#
# -> Reponse : groupby party ; palette PARTY_COLORS differente de BLOC_COLORS.
# -> Explication : 10 listes differentes des 6 blocs -> graphique plus granulaire.
#                  Les deux graphiques restent sur la meme page pour comparaison directe.

st.subheader("Votes par parti (nuance)")

df_party = (
    df_candidate_city
    .groupby("party", as_index=False)["votes_count"]
    .sum()
    .sort_values("votes_count", ascending=False)
)

if not df_party.empty:
    fig_party = px.bar(
        df_party,
        x="party", y="votes_count",
        color="party",
        color_discrete_map=PARTY_COLORS,
        labels={"party": "Parti / Liste", "votes_count": "Votes"},
        title="Repartition des votes valides par parti (nuances)",
    )
    fig_party.update_layout(showlegend=False)
    st.plotly_chart(fig_party, use_container_width=True)
else:
    st.info("Aucune donnee disponible pour le graphique par parti.")

# =============================================================================
# TODO 5 -- Serie temporelle
# =============================================================================
# Convertir minute_bucket : souvent epoch ms -> pd.to_datetime(..., unit="ms", utc=True) puis
# fuseau d'affichage ou parser une chaine ISO.
# Grouper par minute (somme votes_count), px.line(x=time, y=votes_count).
#
# -> Reponse : Detection automatique du format minute_bucket (epoch ms int vs chaine ISO).
# -> Explication : ksqlDB ecrit WINDOWSTART en epoch millisecondes (int).
#                  Si le producteur stocke une chaine ISO, pd.to_datetime() la parse directement.
#                  groupby ts = un point par minute sur la courbe.

st.subheader("Votes par minute")

if not df_city_minute.empty and "minute_bucket" in df_city_minute.columns:
    df_ts = df_city_minute.copy()
    non_null = df_ts["minute_bucket"].dropna()

    if not non_null.empty:
        try:
            # Tentative epoch ms (ksqlDB WINDOWSTART en millisecondes)
            int(non_null.iloc[0])
            df_ts["ts"] = pd.to_datetime(df_ts["minute_bucket"].astype(float), unit="ms", utc=True)
        except (ValueError, TypeError):
            # Chaine ISO (ex : "2026-04-07T14:30:00Z")
            df_ts["ts"] = pd.to_datetime(df_ts["minute_bucket"], utc=True, errors="coerce")

        df_ts_agg = (
            df_ts.dropna(subset=["ts"])
            .groupby("ts", as_index=False)["votes_count"]
            .sum()
            .sort_values("ts")
        )
        if not df_ts_agg.empty:
            n_points = len(df_ts_agg)
            if n_points <= 3:
                # Peu de fenetres : barres + texte pour lisibilite
                df_ts_agg["label"] = df_ts_agg["ts"].dt.strftime("%H:%M UTC")
                fig_ts = px.bar(
                    df_ts_agg,
                    x="label", y="votes_count",
                    text="votes_count",
                    labels={"label": "Fenetre (minute)", "votes_count": "Votes"},
                    title=f"Votes par fenetre de 1 minute ({n_points} fenetre(s) disponible(s))",
                )
                fig_ts.update_traces(textposition="outside")
            else:
                fig_ts = px.line(
                    df_ts_agg,
                    x="ts", y="votes_count",
                    markers=True,
                    labels={"ts": "Heure (UTC)", "votes_count": "Votes"},
                    title="Evolution du nombre de votes par minute (toutes communes)",
                )
            st.plotly_chart(fig_ts, use_container_width=True)
        else:
            st.info("Donnees temporelles insuffisantes.")
    else:
        st.info("Aucune donnee temporelle disponible.")
else:
    st.info("Table votes_by_city_minute vide -- serie temporelle indisponible.")

# =============================================================================
# TODO 6 -- Carte
# =============================================================================
# Priorite : agreger votes_by_department_block (department_code, block) pour carte dept x bloc dominant.
# Repli si table vide : barres top communes depuis votes_by_city_minute + noms depuis communes_fr.json.
# Geo : GeoJSON departements (URL publique) ou points communes si vous avez lat/lon dans le JSON.
#
# -> Reponse : folium.Choropleth avec le GeoJSON des departements francais ;
#              couleur = bloc dominant (votes_count max par departement).
# -> Explication : folium.Choropleth mappe department_code (propriete GeoJSON "code") a un indice
#                  numerique de bloc. Si df_dept_block est vide : fallback barres top communes.

st.subheader("Carte France (votes par departement)")


@st.cache_data(show_spinner="Chargement GeoJSON departements...")
def _load_geojson():
    """Charge le GeoJSON des departements francais depuis une URL publique."""
    try:
        resp = requests.get(GEOJSON_URL, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


@st.cache_data
def _load_commune_names() -> dict:
    """Charge le mapping code INSEE -> nom commune."""
    src = COMMUNES_FILE if COMMUNES_FILE.exists() else SAMPLE_COMMUNES
    if src.exists():
        data = json.loads(src.read_text(encoding="utf-8"))
        return {c["code"]: c.get("nom", c["code"]) for c in data if c.get("code")}
    return {}


if not df_dept_block.empty:
    # Determiner le bloc dominant par departement (ligne avec votes_count max)
    df_dominant = (
        df_dept_block
        .sort_values("votes_count", ascending=False)
        .drop_duplicates(subset="department_code")
        [["department_code", "block", "votes_count"]]
        .copy()
    )
    df_dominant["bloc_label"] = df_dominant["block"].map(lambda b: BLOC_LABELS.get(b, b))

    # Calculer la part du bloc leader pour moduler l'opacite
    df_dept_total = df_dept_block.groupby("department_code", as_index=False)["votes_count"].sum()
    df_dept_total.rename(columns={"votes_count": "total_dept"}, inplace=True)
    df_dominant = df_dominant.merge(df_dept_total, on="department_code", how="left")
    df_dominant["share"] = df_dominant["votes_count"] / df_dominant["total_dept"].replace(0, 1)

    # Couleurs discrete par bloc (hex)
    _BLOC_FILL = {
        "extreme_gauche":   "#8B0000",
        "gauche":           "#E3001B",
        "centre":           "#FF9900",
        "droite":           "#003189",
        "droite_nationale": "#0D2680",
        "autre":            "#808080",
    }
    dept_to_bloc  = dict(zip(df_dominant["department_code"], df_dominant["block"]))
    dept_to_share = dict(zip(df_dominant["department_code"], df_dominant["share"]))

    geojson_data = _load_geojson()
    if geojson_data:
        st.markdown("**Carte Leaflet - couleur politique par departement**")
        m = folium.Map(location=[46.6, 2.3], zoom_start=5, tiles="CartoDB positron")

        def _style_fn(feature):
            code = feature["properties"]["code"]
            bloc = dept_to_bloc.get(code)
            color = _BLOC_FILL.get(bloc, "#cccccc")
            share = dept_to_share.get(code, 0.3)
            opacity = 0.35 + 0.55 * share   # entre 0.35 et 0.90
            return {
                "fillColor":   color,
                "color":       "#444444",
                "weight":      0.6,
                "fillOpacity": opacity,
            }

        def _highlight_fn(feature):
            return {"weight": 2.5, "color": "#222222", "fillOpacity": 0.9}

        folium.GeoJson(
            geojson_data,
            style_function=_style_fn,
            highlight_function=_highlight_fn,
            tooltip=folium.GeoJsonTooltip(
                fields=["nom", "code"],
                aliases=["Departement", "Code"],
                sticky=True,
            ),
        ).add_to(m)

        # Legende HTML
        legend_html = '<div style="position:fixed;bottom:30px;left:30px;z-index:1000;background:white;padding:10px 14px;border-radius:6px;box-shadow:0 0 6px rgba(0,0,0,0.3);font-size:13px;">'
        legend_html += '<b>Bloc leader (departement)</b><br>'
        for bloc_key, label in BLOC_LABELS.items():
            c = _BLOC_FILL.get(bloc_key, "#808080")
            legend_html += f'<i style="background:{c};width:14px;height:14px;display:inline-block;margin-right:6px;border:1px solid #888;"></i>{label}<br>'
        legend_html += '<br><small>Intensite = part du bloc leader</small></div>'
        m.get_root().html.add_child(folium.Element(legend_html))

        st_folium(m, height=520, use_container_width=True)
    else:
        st.warning("GeoJSON des departements indisponible (pas de connexion Internet). Tableau ci-dessous.")

    # Tableau de synthese toujours affiche
    st.dataframe(
        df_dominant[["department_code", "bloc_label", "votes_count"]]
        .rename(columns={
            "department_code": "Dept",
            "bloc_label":      "Bloc dominant",
            "votes_count":     "Votes",
        }),
        use_container_width=True,
        hide_index=True,
    )

else:
    # Fallback : barres top communes si votes_by_department_block est vide
    st.info("Table votes_by_department_block vide -- affichage des top communes en substitution.")
    commune_names_fb = _load_commune_names()
    if not df_city_minute.empty:
        df_top_map = (
            df_city_minute
            .groupby("city_code", as_index=False)["votes_count"]
            .sum()
            .sort_values("votes_count", ascending=False)
            .head(15)
        )
        df_top_map["city_name"] = df_top_map["city_code"].map(lambda c: commune_names_fb.get(c, c))
        fig_map_fb = px.bar(
            df_top_map,
            x="city_name", y="votes_count",
            labels={"city_name": "Commune", "votes_count": "Votes"},
            title="Top 15 communes (fallback carte)",
        )
        st.plotly_chart(fig_map_fb, use_container_width=True)

# =============================================================================
# TODO 7 -- Top communes / Top candidats
# =============================================================================
# Top communes : groupby city_code sur df_city_minute, tri desc, head(10), joindre le nom commune.
# Top candidats : groupby candidate_id, tri desc.
#
# -> Reponse : groupby + sort_values + head(10) ; jointure des noms via _load_commune_names()
#              et candidates_map.
# -> Explication : orientation="h" (barres horizontales) + yaxis["autorange": "reversed"]
#                  affiche le #1 en haut de la liste (sens naturel du classement).

st.subheader("Top communes / Top candidats")
left, right = st.columns(2)

with left:
    # TODO 7 -- Top communes : groupby city_code sur df_city_minute, tri desc, head(10)
    st.markdown("**Top 10 communes**")
    if not df_city_minute.empty:
        commune_names_top = _load_commune_names()
        df_top_cities = (
            df_city_minute
            .groupby("city_code", as_index=False)["votes_count"]
            .sum()
            .sort_values("votes_count", ascending=False)
            .head(10)
        )
        df_top_cities["commune"] = df_top_cities["city_code"].map(
            lambda c: commune_names_top.get(c, c)
        )
        fig_top_cities = px.bar(
            df_top_cities,
            x="votes_count", y="commune", orientation="h",
            labels={"votes_count": "Votes", "commune": "Commune"},
            title="Top 10 communes (votes valides)",
        )
        fig_top_cities.update_layout(yaxis={"autorange": "reversed"})
        st.plotly_chart(fig_top_cities, use_container_width=True)
    else:
        st.info("Aucune donnee commune disponible.")

with right:
    # TODO 7 -- Top candidats : groupby candidate_id, tri desc
    st.markdown("**Top candidats**")
    if not df_candidate_city.empty:
        df_top_cands = (
            df_candidate_city
            .groupby(["candidate_id", "candidate_name"], as_index=False)["votes_count"]
            .sum()
            .sort_values("votes_count", ascending=False)
        )
        fig_top_cands = px.bar(
            df_top_cands,
            x="votes_count", y="candidate_name", orientation="h",
            labels={"votes_count": "Votes", "candidate_name": "Candidat"},
            title="Classement des candidats (votes valides)",
        )
        fig_top_cands.update_layout(yaxis={"autorange": "reversed"}, showlegend=False)
        st.plotly_chart(fig_top_cands, use_container_width=True)
    else:
        st.info("Aucune donnee candidat disponible.")
