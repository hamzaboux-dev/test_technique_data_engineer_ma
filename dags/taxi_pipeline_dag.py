"""
DAG Airflow : orchestre le pipeline Chicago Taxi de bout en bout.
ingestion (bronze) -> bronze_to_silver (spark) -> silver_to_gold (spark)
"""

import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

sys.path.append("/opt/airflow/ingestion")

default_args = {
    "owner": "data-team",
    "retries": 1,
}


def _run_ingestion():
    from fetch_taxi_data import run
    run()


with DAG(
    dag_id="taxi_pipeline",
    description="Pipeline Chicago Taxi : ingestion -> silver -> gold",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # declenchement manuel pour la demo
    catchup=False,
    default_args=default_args,
    tags=["datalake", "taxi"],
) as dag:

    ingest_raw = PythonOperator(
        task_id="ingest_raw",
        python_callable=_run_ingestion,
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "spark-submit --master spark://spark-master:7077 "
            "/opt/airflow/spark_jobs/bronze_to_silver.py"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "spark-submit --master spark://spark-master:7077 "
            "/opt/airflow/spark_jobs/silver_to_gold.py"
        ),
    )

    ingest_raw >> bronze_to_silver >> silver_to_gold
