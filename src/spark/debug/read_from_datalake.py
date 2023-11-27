"""
Read table from datalake
"""
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.appName("read_from_datalake").getOrCreate()

    df = spark.read.table("nessie.example")
    df.show()

    spark.stop()