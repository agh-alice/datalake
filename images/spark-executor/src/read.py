import sys
from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()

    df2 = spark.read.table("nessie.example")
    df2.show()

    spark.stop()