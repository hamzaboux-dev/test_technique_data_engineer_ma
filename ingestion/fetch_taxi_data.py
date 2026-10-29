"""
Ingestion : telecharge une tranche du dataset Chicago Taxi Trips via l'API SODA,
en paginant par curseur sur trip_start_timestamp (plus robuste que $offset, qui
ralentit fortement l'API au fil des pages sur ce dataset), et depose les pages
en CSV dans la couche bronze du datalake (bucket MinIO "datalake").

Periode choisie : 1er trimestre 2023 (2023-01-01 -> 2023-03-31).
Documente ce choix (et adapte-le si besoin) dans le README final.
"""

import io
import time
import requests
import pandas as pd
import boto3

BASE_URL = "https://data.cityofchicago.org/resource/wrvz-psew.json"
START = "2023-01-01T00:00:00"
END = "2023-03-31T23:59:59"
PAGE_SIZE = 50000
TIMEOUT_SECONDS = 120
MAX_RETRIES = 5

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "datalake"


def fetch_page(lower_bound: str) -> list[dict]:
    """
    Recupere une page de resultats a partir d'un curseur temporel (lower_bound),
    plutot que d'un offset numerique. Sur l'API SODA, $offset devient tres lent
    (voire timeout) a mesure qu'il grandit sur de gros volumes ; filtrer et
    trier sur une colonne indexee (trip_start_timestamp) reste rapide page apres
    page. Avec retry + backoff en cas de timeout ponctuel du serveur.
    """
    params = {
        "$where": f"trip_start_timestamp >= '{lower_bound}' AND trip_start_timestamp <= '{END}'",
        "$limit": PAGE_SIZE,
        "$order": "trip_start_timestamp",
    }

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=TIMEOUT_SECONDS)
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as exc:
            last_exc = exc
            wait = attempt * 10
            print(f"[ingestion] tentative {attempt}/{MAX_RETRIES} echouee ({exc}), retry dans {wait}s")
            time.sleep(wait)

    raise last_exc


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def ensure_bucket(s3):
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)


def run():
    s3 = get_s3_client()
    ensure_bucket(s3)

    cursor = START
    page_num, total_rows = 0, 0

    while True:
        rows = fetch_page(cursor)
        if not rows:
            break

        df = pd.DataFrame(rows)
        buf = io.BytesIO()
        df.to_csv(buf, index=False)

        key = f"bronze/taxi_trips/part-{page_num:04d}.csv"
        s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())

        total_rows += len(df)
        print(f"[ingestion] page {page_num} -> {key} ({len(df)} lignes)")

        # Curseur = dernier timestamp vu dans cette page (strictement superieur
        # pour la page suivante). Limite connue et documentee : des lignes
        # partageant exactement le meme timestamp que la derniere ligne de la
        # page pourraient etre sautees a la frontiere entre deux pages ; la
        # deduplication sur trip_id en couche Silver absorbe par ailleurs tout
        # chevauchement eventuel entre deux pages.
        last_ts = rows[-1]["trip_start_timestamp"]
        if last_ts == cursor and len(rows) < PAGE_SIZE:
            break
        cursor = last_ts

        page_num += 1

        if len(rows) < PAGE_SIZE:
            break

    print(f"[ingestion] termine : {total_rows} lignes sur {page_num + 1} fichiers.")


if __name__ == "__main__":
    run()