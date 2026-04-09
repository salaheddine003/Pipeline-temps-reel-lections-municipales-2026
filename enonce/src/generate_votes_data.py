#!/usr/bin/env python3
"""Génère un jeu de données de votes municipaux simulés (fichiers sous data/)."""

import csv
import datetime as dt
import json
import random
import uuid
from pathlib import Path

import requests

# Racine du livrable (dossier qui contient `enonce/` et `data/`), indépendante du répertoire courant (CWD).
_ROOT = Path(__file__).resolve().parent.parent.parent
BASE = _ROOT / "data"
COMMUNES_URL = (
    "https://geo.api.gouv.fr/communes"
    "?fields=nom,code,codeDepartement,codeRegion,population,centre&format=json&geometry=centre"
)
def ensure_base() -> None:
    BASE.mkdir(parents=True, exist_ok=True)


def fetch_communes() -> list[dict]:
    fallback_path = BASE / "communes_fr_sample.json"
    try:
        resp = requests.get(COMMUNES_URL, timeout=25)
        resp.raise_for_status()
        communes = resp.json()
        (BASE / "communes_fr.json").write_text(
            json.dumps(communes, ensure_ascii=False), encoding="utf-8"
        )
        return communes
    except Exception:
        if fallback_path.exists():
            return json.loads(fallback_path.read_text(encoding="utf-8"))
        raise


def write_candidates() -> list[str]:
    rows = [
        ("C01", "Liste France Insoumise Locale", "LFI", "extreme_gauche"),
        ("C02", "Liste Socialiste Citoyenne", "PS", "gauche"),
        ("C03", "Liste Ecologiste Alliance Gauche", "ECO", "gauche"),
        ("C04", "Liste Renaissance Locale", "REN", "centre"),
        ("C05", "Liste MoDem Metropole", "MDM", "centre"),
        ("C06", "Liste Les Republicains Ville", "LR", "droite"),
        ("C07", "Liste Rassemblement National", "RN", "droite_nationale"),
        ("C08", "Liste UDR Territoriale", "UDR", "droite_nationale"),
        ("C09", "Liste Citoyenne Sans Etiquette", "SE", "autre"),
        ("C10", "Liste Regionaliste Locale", "REG", "autre"),
    ]
    out = BASE / "candidates.csv"
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["candidate_id", "candidate_name", "party", "political_block"])
        w.writerows(rows)
    return [r[0] for r in rows]


def generate_votes(communes: list[dict], candidates: list[str], n: int = 10_000) -> None:
    valid_communes = [c for c in communes if c.get("code") and c.get("nom")]
    channels = ["booth", "mobile", "assist_terminal"]
    now = dt.datetime.now(dt.UTC)
    random.seed(42)
    out = BASE / "votes_municipales_sample.jsonl"

    with out.open("w", encoding="utf-8") as w:
        for i in range(n):
            c = random.choice(valid_communes[:500])
            evt = {
                "vote_id": str(uuid.uuid4()),
                "election_id": "muni_2026",
                "event_time": (now + dt.timedelta(seconds=i % 1800)).isoformat().replace(
                    "+00:00", "Z"
                ),
                "city_code": c["code"],
                "city_name": c.get("nom", "Unknown"),
                "department_code": c.get("codeDepartement", ""),
                "region_code": c.get("codeRegion", ""),
                "polling_station_id": f"PS_{random.randint(1, 120):03d}",
                "candidate_id": random.choice(candidates),
                "channel": random.choice(channels),
                "signature_ok": True,
                "voter_hash": f"vh_{uuid.uuid4().hex[:16]}",
                "ingestion_ts": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
            }
            r = random.random()
            if r < 0.03:
                evt["signature_ok"] = False
            elif r < 0.06:
                evt["candidate_id"] = "C99"
            elif r < 0.08:
                evt["vote_id"] = ""
            w.write(json.dumps(evt, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    ensure_base()
    communes_data = fetch_communes()
    candidate_ids = write_candidates()
    generate_votes(communes_data, candidate_ids, n=10_000)
    print(f"OK : fichiers générés dans {BASE}/")
