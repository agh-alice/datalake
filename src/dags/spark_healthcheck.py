from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime
from utils import DatalakeFullAccessOperator

dag = DAG(
    'spark_healthcheck',
    description='Perform calculations on spark that don\'t require any dependencies',
    schedule="@hourly",
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    concurrency = 1,
)

task = DatalakeFullAccessOperator(
    task_id='test_spark_deployment',
    path='debug/test_spark_deployment.py',
    instances=1,
    dag=dag
)