"""
Script d'ingestion : va chercher une tranche du dataset Chicago Taxi Trips
via l'API SODA, en paginant par curseur sur trip_start_timestamp, et depose
chaque page en CSV dans la couche bronze du datalake (bucket MinIO "datalake").

Periode choisie pour cet exercice : 1er trimestre 2023 (2023-01-01 -> 2023-03-31).
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
    Recupere une page de resultats aupres de l'API, a partir d'un curseur
    temporel plutot que d'un offset numerique. Sur l'API SODA, paginer avec
    $offset devient tres lent (jusqu'au timeout) a mesure que l'offset grandit ;
    filtrer et trier sur une colonne indexee comme trip_start_timestamp reste
    rapide page apres page, meme loin dans le dataset.

    En cas de timeout ou de coupure reseau ponctuelle, la fonction reessaie
    automatiquement plusieurs fois avant d'abandonner.
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
            print(f"[ingestion] tentative {attempt}/{MAX_RETRIES} echouee ({exc}), nouvel essai dans {wait}s")
            time.sleep(wait)

    raise last_exc


def get_s3_client():
    """
    Construit le client boto3 utilise pour parler a MinIO/Silo, qui expose
    une API compatible S3. On passe explicitement l'URL locale du service
    et les identifiants, puisqu'il ne s'agit pas d'un vrai compte AWS.
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def ensure_bucket(s3):
    """
    Verifie que le bucket "datalake" existe deja et le cree sinon. Permet
    de relancer le script autant de fois qu'on veut sans se soucier de
    l'etat initial du stockage.
    """
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)


def run():
    """
    Point d'entree principal : parcourt tout le dataset page par page a
    partir de la date de depart, et ecrit chaque page recue en CSV dans
    la couche bronze. S'arrete des que l'API ne renvoie plus de nouvelles
    lignes, ou renvoie une page incomplete (signe qu'on a atteint la fin
    de la periode demandee).
    """
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

        # Le curseur de la page suivante est le dernier timestamp vu dans
        # cette page. Limite connue : des lignes partageant exactement le
        # meme timestamp que la derniere ligne pourraient en theorie etre
        # sautees a la frontiere entre deux pages. Ce n'est pas genant ici
        # car la deduplication sur trip_id en couche Silver absorbe de
        # toute facon tout chevauchement eventuel entre deux pages.
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