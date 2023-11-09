
import datetime
import pyspark
import os

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="postgres-dump-to-s3",
    schedule_interval=datetime.timedelta(seconds=30), # TODO change to days=1
    start_date=datetime.datetime(2023, 11, 8, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def PostgresDumpToS3():
    select_from_employees = PostgresOperator(
        task_id="select_from_employees",
        postgres_conn_id="tutorial_pg_conn",
        sql="""
            SELECT TOP 50 * FROM employees;
            );""",
    )

    @task
    def get_and_dump_data():
        url = "jdbc:postgresql://postgres:5432/mon_data"

        properties = {
            "user": "mon_user",
            "password": "cern",
            "driver": "org.postgresql.Driver"
        }
        table_name = "mon_jobs_data_v3"

        df = spark.read.jdbc(url, table_name, properties=properties)

    [get_and_dump_data()]


dag = PostgresDumpToS3()