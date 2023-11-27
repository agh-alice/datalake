from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime

dag = DAG(
    'test_spark_deployment',
    description='Perform calculations on spark that don\'t require any dependencies',
    schedule="@hourly",
    start_date=datetime.datetime(2023, 1, 1),
)

task = BashOperator(
    task_id='test_spark_deployment',
    bash_command='spark-submit \
    --master k8s://https://kubernetes.default.svc.cluster.local:443 \
    --deploy-mode cluster \
    --conf spark.executor.instances=1 \
    --conf spark.kubernetes.container.image=nowickib/spark-executor:latest \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.authenticate.executor.serviceAccountName=spark \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=/opt/spark/logs/spark-events \
    --conf spark.history.fs.logDirectory=/opt/spark/logs/spark-events \
    --name test_spark_deployment \
    local:///spark/debug/test_spark_deployment.py',
    dag=dag
)
