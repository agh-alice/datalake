import airflow
from datetime import datetime, timedelta
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_args = {
    'owner': 'jozimar',
    'start_date': datetime(2020, 11, 18),
    'retries': 10,
	  'retry_delay': timedelta(hours=1)
}
with airflow.DAG('spark-job-submit',
                  default_args=default_args,
                  schedule_interval='0 1 * * *') as dag:
    task_elt_documento_pagar = SparkSubmitOperator(
        task_id='spark_job_submit',
        conn_id='spark',
        application="./dags/sparkjob.py",
        total_executor_cores=3,
        executor_memory="30g",
    )