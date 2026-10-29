"""
DAG Airflow : orchestre le pipeline Chicago Taxi de bout en bout.
ingestion (bronze) -> bronze_to_silver (spark) -> silver_to_gold (spark)
"""

import sys
from datetime import datetime

import docker
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/ingestion")

default_args = {
    "owner": "data-team",
    "retries": 1,
}


def _run_ingestion():
    from fetch_taxi_data import run
    run()


def _spark_submit(script_path: str):
    """Execute spark-submit a l'interieur du conteneur spark-master deja
    demarre, via le socket Docker partage (evite de dupliquer Spark/Java
    dans l'image Airflow)."""
    client = docker.DockerClient(base_url="unix://var/run/docker.sock")
    container = client.containers.get("spark-master")
    exit_code, output = container.exec_run(
        cmd=[
            "spark-submit",
            "--master", "spark://spark-master:7077",
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            script_path,
        ],
        environment={
            "HOME": "/tmp",
            "HADOOP_USER_NAME": "spark",
            "USER": "spark",
        },
        user="root",
        stream=False,
    )
    print(output.decode(errors="replace"))
    if exit_code != 0:
        raise RuntimeError(f"spark-submit a echoue (code {exit_code}) pour {script_path}")

with DAG(
    dag_id="taxi_pipeline",
    description="Pipeline Chicago Taxi : ingestion -> silver -> gold",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["datalake", "taxi"],
) as dag:

    ingest_raw = PythonOperator(
        task_id="ingest_raw",
        python_callable=_run_ingestion,
    )

    bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=lambda: _spark_submit("/opt/bitnami/spark/jobs/bronze_to_silver.py"),
    )

    silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=lambda: _spark_submit("/opt/bitnami/spark/jobs/silver_to_gold.py"),
    )

    ingest_raw >> bronze_to_silver >> silver_to_gold
