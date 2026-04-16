"""
Producer long-running: poll USGS earthquake API toutes les POLL_INTERVAL
secondes et depose un fichier JSONL dans INCOMING_DIR pour que Spark
Structured Streaming le consomme en mode file-source.

Ecriture atomique: on ecrit <file>.tmp puis os.replace(...) vers le nom
final. Spark n'indexe donc que des fichiers complets.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# pyrefly: ignore [untyped-import]
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

INCOMING_DIR = Path(os.environ.get("INCOMING_DIR", "/data/incoming"))
USGS_API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
MIN_MAGNITUDE = float(os.environ.get("MIN_MAGNITUDE", "2.5"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "30"))
# Fenetre glissante large: l'USGS publie les evenements avec une latence
# de 5-30 min (parfois plus). Une fenetre etroite (ex: 2 min) rate tous
# les evenements qui apparaissent dans le catalogue apres son passage.
# On redemande donc la derniere heure a chaque poll et on deduplique
# cote client via SEEN_IDS pour eviter les PK violations en Postgres.
POLL_WINDOW_SECONDS = int(os.environ.get("POLL_WINDOW_SECONDS", "3600"))
INITIAL_LOOKBACK_DAYS = int(os.environ.get("INITIAL_LOOKBACK_DAYS", "7"))


def _parse_feature(feature: dict) -> dict:
    """Parse une feature GeoJSON USGS en dictionnaire normalisé.

    TODO: Compléter cette fonction
    - Extrayez les propriétés (properties) et les coordonnées (geometry.coordinates)
    - Convertissez le timestamp en ISO format
    - Retournez un dictionnaire avec les clés : event_id, magnitude, mag_type, place,
      event_time, latitude, longitude, depth_km, significance, tsunami, status, type,
      title, ingested_at

    Indices:
    - feature["properties"] contient les données principales
    - feature["geometry"]["coordinates"] = [lon, lat, depth_km]
    - feature["id"] ou properties["code"] = event_id
    - timestamp en ms dans properties["time"] -> convertir en ISO
    """
    raise NotImplementedError("À implémenter")


def _write_atomic(rows: list[dict], tag: str) -> Path:
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    final = INCOMING_DIR / f"eq_{tag}.json"
    tmp = final.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    os.replace(tmp, final)
    return final


def run() -> None:
    logging.info("Producer started. Polling %s every %ds (window=%ds). Output -> %s",
                 USGS_API_URL, POLL_INTERVAL, POLL_WINDOW_SECONDS, INCOMING_DIR)

    # Au premier poll, on regarde loin en arriere (INITIAL_LOOKBACK_DAYS)
    # pour avoir des donnees immediatement. Ensuite, POLL_WINDOW_SECONDS.
    seen_ids: set[str] = set()
    first_poll = True

    while True:
        now = datetime.now(tz=timezone.utc)
        if first_poll:
            window_start = now - timedelta(days=INITIAL_LOOKBACK_DAYS)
        else:
            window_start = now - timedelta(seconds=POLL_WINDOW_SECONDS)

        try:
            resp = requests.get(
                USGS_API_URL,
                params={
                    "format":       "geojson",
                    "starttime":    window_start.strftime("%Y-%m-%dT%H:%M:%S"),
                    "endtime":      now.strftime("%Y-%m-%dT%H:%M:%S"),
                    "minmagnitude": MIN_MAGNITUDE,
                    "orderby":      "time",
                },
                timeout=15,
            )
            resp.raise_for_status()
            features = resp.json().get("features", [])

            new_rows: list[dict] = []
            for f in features:
                row = _parse_feature(f)
                eid = row["event_id"]
                if eid and eid not in seen_ids:
                    seen_ids.add(eid)
                    new_rows.append(row)

            logging.info(
                "Fetched %d features (%s -> %s) - %d new, %d already seen",
                len(features),
                window_start.strftime("%H:%M:%S"),
                now.strftime("%H:%M:%S"),
                len(new_rows),
                len(features) - len(new_rows),
            )

            if new_rows:
                tag = now.strftime("%Y%m%dT%H%M%S")
                path = _write_atomic(new_rows, tag)
                logging.info("Wrote %d new rows -> %s", len(new_rows), path)

            first_poll = False

        except requests.RequestException as exc:
            logging.error("USGS API error: %s", exc)
        except Exception as exc:
            logging.exception("Unexpected error: %s", exc)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
