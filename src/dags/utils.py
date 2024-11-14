from airflow import DAG
from airflow.operators.bash import BashOperator


class DatalakeFullAccessOperator(BashOperator):
    def __init__(self, task_id: str, path: str, instances: int, dag: DAG, **kwargs) -> None:
        super().__init__(
            task_id=task_id,
            bash_command=f'spark-submit \
            --master k8s://https://kubernetes.default.svc.cluster.local:443 \
            --deploy-mode cluster \
            --conf spark.driver.memory=8G \
            --conf spark.executor.memory=8G \
            --conf spark.executor.instances={instances} \
            --conf spark.driver.extraClassPath=local://opt/spark/jars/bundle-2.20.18.jar:local://opt/spark/jars/nessie-spark-extensions-3.4_2.12-0.74.0.jar:local://opt/spark/jars/url-connection-client-2.20.18.jar:local://opt/spark/jars/iceberg-spark-runtime-3.4_2.12-1.3.0.jar:local://opt/spark/jars/postgresql-42.6.0.jar \
            --conf spark.kubernetes.namespace=datalake \
            --conf spark.kubernetes.container.image=ghcr.io/agh-alice/spark-executor:1986bd6 \
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
            --conf spark.sql.caseSensitive=true\
            --name {task_id} \
            local:///spark/{path}',
            dag=dag,
            **kwargs
        )
