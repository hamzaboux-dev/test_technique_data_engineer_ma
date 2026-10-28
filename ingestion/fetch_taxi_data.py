"""
Ingestion : telecharge une tranche du dataset Chicago Taxi Trips via l'API SODA,
en paginant, filtree sur trip_start_timestamp, et depose les pages en CSV
dans la couche bronze du datalake (bucket MinIO "datalake").

Periode choisie : 1er trimestre 2023 (2023-01-01 -> 2023-03-31).
Documente ce choix (et adapte-le si besoin) dans le README final.
"""

import io
import requests
import pandas as pd
import boto3

BASE_URL = "https://data.cityofchicago.org/resource/wrvz-psew.json"
START = "2023-01-01T00:00:00"
END = "2023-03-31T23:59:59"
PAGE_SIZE = 50000

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET = "datalake"


def fetch_page(offset: int) -> list[dict]:
    params = {
        "$where": f"trip_start_timestamp between '{START}' and '{END}'",
        "$limit": PAGE_SIZE,
        "$offset": offset,
        "$order": "trip_start_timestamp",
    }
    resp = requests.get(BASE_URL, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


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

    offset, page_num, total_rows = 0, 0, 0
    while True:
        rows = fetch_page(offset)
        if not rows:
            break

        df = pd.DataFrame(rows)
        buf = io.BytesIO()
        df.to_csv(buf, index=False)

        key = f"bronze/taxi_trips/part-{page_num:04d}.csv"
        s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())

        total_rows += len(df)
        print(f"[ingestion] page {page_num} -> {key} ({len(df)} lignes)")

        offset += PAGE_SIZE
        page_num += 1

    print(f"[ingestion] termine : {total_rows} lignes sur {page_num} fichiers.")


if __name__ == "__main__":
    run()
