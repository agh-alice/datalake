from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime

dag = DAG(
    'read_from_datalake',
    description='Simple DAG to test if we can read data from the datalake',
    schedule="@hourly",
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False
)

task = BashOperator(
    task_id='read_from_datalake',
    bash_command='spark-submit \
    --master k8s://https://kubernetes.default.svc.cluster.local:443 \
    --deploy-mode cluster \
    --conf spark.executor.instances=1 \
    --conf spark.driver.extraClassPath=local://opt/spark/jars/bundle-2.20.18.jar:local://opt/spark/jars/nessie-spark-extensions-3.4_2.12-0.65.1.jar:local://opt/spark/jars/url-connection-client-2.20.18.jar:local://opt/spark/jars/iceberg-spark-runtime-3.4_2.12-1.3.0.jar:local://opt/spark/jars/postgresql-42.6.0.jar \
    --conf spark.kubernetes.namespace=datalake \
    --conf spark.kubernetes.container.image=nowickib/spark-executor:latest \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.authenticate.executor.serviceAccountName=spark \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions \
    --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
    --conf spark.sql.catalog.nessie.uri=$NESSIE_URL\
    --conf spark.sql.catalog.nessie.authentication.type=NONE \
    --conf spark.sql.catalog.nessie.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.nessie.s3.endpoint=$S3_ENDPOINT \
    --conf spark.sql.catalog.nessie.warehouse=s3a://alice-data-lake-dev \
    --conf spark.sql.catalog.nessie.s3.access-key-id=$S3_ACCESS_KEY_ID \
    --conf spark.sql.catalog.nessie.s3.secret-access-key=$S3_SECRET_ACCESS_KEY \
    --conf spark.sql.catalog.nessie.s3.path-style-access=true  \
    --conf spark.executorEnv.AWS_REGION=$S3_REGION \
    --conf spark.sql.defaultCatalog=nessie \
    --conf spark.kubernetes.driverEnv.AWS_REGION=$S3_REGION \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=/opt/spark/logs/spark-events \
    --conf spark.history.fs.logDirectory=/opt/spark/logs/spark-events \
    --conf spark.sql.catalogImplementation=in-memory \
    --name load_new_data \
    local:///spark/debug/read_from_datalake.py',
    dag=dag
)
