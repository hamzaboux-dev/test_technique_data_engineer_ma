FROM apache/airflow:2.9.3

USER airflow
RUN pip install --no-cache-dir \
    requests \
    boto3 \
    pandas \
    docker