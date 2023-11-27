from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime
from utils import DatalakeFullAccessOperator

dag = DAG(
    'datalake_healthcheck',
    description='Simple DAG to test if we can read data from the datalake',
    schedule="@hourly",
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    concurrency = 1,
)

task = DatalakeFullAccessOperator(
    task_id='read_from_datalake',
    path='debug/test_datalake_read.py',
    instances=1,
    dag=dag
)