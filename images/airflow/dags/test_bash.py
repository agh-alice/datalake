import airflow
from datetime import datetime, timedelta
from airflow.contrib.operators.spark_submit_operator import BashOperator

default_args = {
    'owner': 'blazej',
    'start_date': datetime(2020, 11, 18),
    'retries': 10,
	  'retry_delay': timedelta(hours=1)
}
with airflow.DAG('bash_test',
                  default_args=default_args,
                  schedule_interval='0 1 * * *') as dag:
    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command="echo 1",
    )