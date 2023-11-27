from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Define the DAG
dag = DAG(
    'test_spark',
    description='A simple tutorial DAG',
    schedule_interval=None,
    start_date=datetime(2023, 3, 22),
    catchup=False
)

# Define the BashOperator task
hello_world_task = BashOperator(
    task_id='hello_spark_task',
    bash_command='which python && which pip',
    dag=dag
)
