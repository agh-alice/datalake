from pyspark.sql import SparkSession
from pyspark.sql import functions

path = "local://opt/spark/jars/"
jars = ["bundle-2.20.18.jar", "nessie-spark-extensions-3.4_2.12-0.65.1.jar", "url-connection-client-2.20.18.jar", "iceberg-spark-runtime-3.4_2.12-1.3.0.jar", "postgresql-42.6.0.jar"]

class_path = ":".join([path + jar for jar in jars])

spark = SparkSession.builder.master("k8s://https://kubernetes.default.svc.cluster.local:443") \
                            .appName("spark")\
                            .config("spark.executor.instances", 3)\
                            .config("spark.submit.deployMode", "client")\
                            .config("spark.driver.host", "spark-master")\
                            .config("spark.driver.port", "8002")\
                            .config("spark.driver.extraClassPath", class_path) \
                            .config("spark.blockManager.port", "8001")\
                            .config("spark.kubernetes.namespace", "default")\
                            .config("spark.kubernetes.container.image", "spark-executor")\
                            .config("spark.kubernetes.container.image.pullPolicy", "Never")\
                            .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")\
                            .config("spark.kubernetes.authenticate.executor.serviceAccountName", "spark")\
                            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")\
                            .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")\
                            .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")\
                            .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1")\
                            .config("spark.sql.catalog.nessie.authentication.type", "NONE")\
                            .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")\
                            .config("spark.sql.catalog.nessie.s3.endpoint", "http://minio:9000")\
                            .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse")\
                            .config("spark.sql.catalog.nessie.s3.access-key-id", "admin")\
                            .config("spark.sql.catalog.nessie.s3.secret-access-key", "password")\
                            .config("spark.sql.catalog.nessie.s3.path-style-access", "true") \
                            .config("spark.executorEnv.AWS_REGION", "eu-central-1") \
                            .config("spark.sql.defaultCatalog", "nessie")\
                            .config("spark.eventLog.enabled", "true")\
                            .config("spark.eventLog.dir", "/home/spark/spark-events")\
                            .config("spark.history.fs.logDirectory", "/home/spark/spark-events")\
                            .config("spark.sql.catalogImplementation", "in-memory")\
                            .getOrCreate()


from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
df.printSchema()
df.show(truncate=False)
df.writeTo("nessie.example").create()