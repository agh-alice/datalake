from pyspark.sql import SparkSession
import requests
import lzma
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import DataFrame

from utils import flatten_schema, union_all, ceil_division

BATCH_SIZE = 1000
SLICES_PER_BATCH = 50
OUTPUT_TABLE = "nessie.site_sonar"

URL = "http://alimonitor.cern.ch/download/kalana/site-sonar-latest.xz"


def main():
    spark: SparkSession = SparkSession.builder.appName("update_site_sonar").getOrCreate()

    raw = requests.get(URL).content

    # decompress .xz file 
    data = lzma.decompress(raw).decode()
    lines = data.splitlines()

    dfs = []
    for i in range(ceil_division(len(lines), BATCH_SIZE)):
        batch = spark.sparkContext.parallelize(lines[i * BATCH_SIZE: (i+1) * BATCH_SIZE], numSlices=SLICES_PER_BATCH)
        df = spark.read.json(batch)
        dfs.append(df)

    df = union_all(dfs)
    df = flatten_schema(df)

    if not spark.catalog.tableExists(OUTPUT_TABLE):
        initial_schema = StructType([StructField('batch_index', IntegerType(), True)])
        initial_df: DataFrame = spark.createDataFrame([(0,)], schema=initial_schema)
        initial_df.writeTo(OUTPUT_TABLE).partitionedBy('batch_index').create()
        spark.sql(f"ALTER TABLE {OUTPUT_TABLE} SET TBLPROPERTIES ('write.spark.accept-any-schema'='true')")
    
    last_index = spark.read.table(OUTPUT_TABLE).select(F.max("batch_index").alias("last_index")).first()[0]

    df = df.withColumn("batch_index", F.lit(last_index+1))
    old_columns = spark.read.table(OUTPUT_TABLE).columns
    new_columns = [col for col in old_columns if col in df.columns] + [col for col in df.columns if col not in old_columns]

    df.select(*new_columns).writeTo(OUTPUT_TABLE).partitionedBy("batch_index").option("mergeSchema","true").append()

    spark.stop()


if __name__ == "__main__":
    main()