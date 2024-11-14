from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime
from utils import DatalakeFullAccessOperator

dag = DAG(
    'data_dump',
    description='DAG moving data from PostgreSQL database to S3',
    schedule="@daily",
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
    concurrency = 1,
)

postgres_dump = DatalakeFullAccessOperator(
    task_id='postgres_dump',
    path='postgres_dump.py',
    instances=4,
    dag=dag
)

site_sonar_dump = DatalakeFullAccessOperator(
    task_id='site_sonar_dump',
    path='site_sonar.py',
    instances=4,
    dag=dag
)

postgres_dump >> site_sonar_dump