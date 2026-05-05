FROM docker.arvancloud.ir/apache/airflow:latest

USER airflow

RUN pip install --no-cache-dir \
    faker \
    psycopg2-binary \
    kafka-python \
    pandas \
    requests