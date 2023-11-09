from datetime import datetime
from pyspark.sql import SparkSession, functions
from pyspark import SparkConf, SparkContext
import sys


conf = (
    SparkConf()
    .setAppName("Spark Job - SQL2S3")
    .set("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/Financeiro")
    .set("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/Financeiro")
    .set("spark.network.timeout", 10000000)
    .set("spark.executor.heartbeatInterval", 10000000)
    .set("spark.storage.blockManagerSlaveTimeoutMs", 10000000)
    .set("spark.driver.maxResultSize", "20g")
)

spark = SparkSession.builder.appName().getOrCreate()

data_base = datetime.strptime(sys.argv[1], "%Y-%m-%d")
inicio = datetime.now()
print(inicio)

df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:{}".format(sys.argv[2]))
    .option("user", "Teste")
    .option("password", "teste")
    .option("numPartitions", 100)
    .option("partitionColumn", "Id")
    .option("lowerBound", 1)
    .option("upperBound", 488777675)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .option(
        "dbtable",
        "(select Id, DataVencimento AS Vencimento, TipoCod AS CodigoTipoDocumento, cast(recsld as FLOAT) AS Saldo from DocumentoPagar \
     where TipoCod in ('200','17') and RecPag = 'A') T",
    )
    .load()
)
group = (
    df.select("CodigoTipoDocumento", "Vencimento", "Saldo")
    .groupby(["CodigoTipoDocumento", "Vencimento"])
    .agg(functions.sum("Saldo").alias("Saldo"))
)

group.write.format("com.mongodb.spark.sql.DefaultSource").mode("overwrite").option(
    "database", "Financeiro"
).option("collection", "Fact_DocumentoPagar").save()
termino = datetime.now()
print(termino)
print(termino - inicio)
