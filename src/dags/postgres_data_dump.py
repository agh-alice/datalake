from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime
from utils import DatalakeFullAccessOperator

dag = DAG(
    'postgres_data_dump',
    description='DAG moving data from PostgreSQL database to S3',
    schedule="@daily",
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    concurrency = 1,
)

task = DatalakeFullAccessOperator(
    task_id='postgres_dump',
    path='postgres_dump.py',
    instances=1,
    dag=dag
)