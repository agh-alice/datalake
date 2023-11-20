FROM apache/airflow:2.7.1

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark pyspark

COPY ./dags/ ${AIRFLOW_HOME}/dags/ 
