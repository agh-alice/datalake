import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, coalesce, col, lit, broadcast
from pyspark.sql.types import StructType
import psycopg2
from psycopg2 import sql

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def init_spark():
    """Initialize and configure Spark session."""
    logger.info("Initializing Spark session")
    return SparkSession.builder \
        .appName("postgres_dump") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def fetch_job_ids(spark, url, properties):
    """Fetch job IDs older than 7 days from PostgreSQL."""
    logger.info("Fetching job IDs older than 7 days from PostgreSQL")
    oldest_jobs_ids_query = "SELECT job_id FROM job_info WHERE last_update < NOW() - INTERVAL '7 days' ORDER BY last_update ASC"
    job_ids = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", f"({oldest_jobs_ids_query}) AS oldest_jobs") \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .option("driver", properties["driver"]) \
        .option("fetchsize", "1000") \
        .load() \
        .rdd.map(lambda row: row.job_id).collect()
    logger.info(f"Fetched {len(job_ids)} job IDs")
    return job_ids

def process_batch(spark, job_ids_batch, url, properties, json_schema, lpm_passname_exists):
    """Process a single batch of job IDs."""
    job_ids_str = ','.join([str(job_id) for job_id in job_ids_batch])
    logger.info(f"Processing batch of job IDs: {job_ids_str}")

    # Initialize PostgreSQL connection per partition
    conn = psycopg2.connect(
        dbname="mon_data", user="mon_user", password="cern", host="149.156.10.139", port="5432"
    )
    cursor = conn.cursor()

    try:
        # Load job_info data
        logger.info("Loading job_info data for batch")
        job_info_df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", f"(SELECT * FROM job_info WHERE job_id IN ({job_ids_str})) AS job_info") \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"]) \
            .option("fetchsize", "1000") \
            .load() \
            .fillna("").repartition("job_id")

        # Load mon_jobs_data_v3 data
        logger.info("Loading mon_jobs_data_v3 data for batch")
        mon_jobs_df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", f"(SELECT * FROM mon_jobs_data_v3 WHERE job_id IN ({job_ids_str})) AS mon_jobs_data") \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"]) \
            .option("fetchsize", "1000") \
            .load() \
            .fillna("").repartition("job_id")

        # Load and parse mon_jdls data with LPMPassName handling
        logger.info("Loading mon_jdls data for batch")
        mon_jdls_df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", f"(SELECT * FROM mon_jdls WHERE job_id IN ({job_ids_str})) AS mon_jdls") \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"]) \
            .option("fetchsize", "1000") \
            .load() \
            .withColumn("jsonData", from_json("full_jdl", json_schema))

        if lpm_passname_exists:
            mon_jdls_df = mon_jdls_df.select(
                col("job_id"),
                coalesce(col("jsonData.LPMPassName"), lit('')).alias("LPMPASSNAME")
            )
        else:
            mon_jdls_df = mon_jdls_df.select(
                col("job_id"),
                lit('').alias("LPMPASSNAME")
            )

        # Load trace data
        logger.info("Loading trace data for batch")
        trace_df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", f"(SELECT * FROM trace WHERE job_id IN ({job_ids_str})) AS trace") \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"]) \
            .option("fetchsize", "1000") \
            .load() \
            .fillna("").repartition("job_id")

        # Use broadcast for smaller DataFrames if necessary
        mon_jdls_df = broadcast(mon_jdls_df)

        logger.info("Writing data to Nessie main branch")
        spark.sql("USE REFERENCE main IN nessie")

        job_info_df.writeTo("nessie.job_info").option("numPartitions", 10).append()
        mon_jobs_df.writeTo("nessie.mon_jobs_data_v3").option("mergeSchema", "true").option("numPartitions", 10).append()
        mon_jdls_df.writeTo("nessie.mon_jdls_parsed").option("mergeSchema", "true").option("numPartitions", 10).append()
        trace_df.writeTo("nessie.trace").append()

        # Delete processed records in batch
        logger.info("Deleting processed records from PostgreSQL for batch")
        cursor.execute(sql.SQL(f"DELETE FROM job_info WHERE job_id IN ({job_ids_str})"))
        cursor.execute(sql.SQL(f"DELETE FROM mon_jobs_data_v3 WHERE job_id IN ({job_ids_str})"))
        cursor.execute(sql.SQL(f"DELETE FROM mon_jdls WHERE job_id IN ({job_ids_str})"))
        cursor.execute(sql.SQL(f"DELETE FROM trace WHERE job_id IN ({job_ids_str})"))

        logger.info("Successfully processed and deleted records for batch")
        conn.commit()

    except Exception as e:
        logger.error(f"Error processing batch for job IDs {job_ids_batch}: {e}", exc_info=True)
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    logger.info("Starting main processing")

    spark = init_spark()
    url = "jdbc:postgresql://149.156.10.139:5432/mon_data"
    properties = {"user": "mon_user", "password": "cern", "driver": "org.postgresql.Driver"}

    oldest_jobs_ids = fetch_job_ids(spark, url, properties)
    limit = 500

    logger.info("Inferring JSON schema for mon_jdls")
    sample_json = spark.read.jdbc(
        url=url, table="(SELECT full_jdl FROM mon_jdls LIMIT 10) AS mon_jdls_sample", properties=properties
    ).select("full_jdl")
    json_schema = spark.read.json(sample_json.rdd.map(lambda row: row.full_jdl)).schema
    lpm_passname_exists = "LPMPassName" in [field.name for field in json_schema.fields]

    logger.info("Starting parallel batch processing")
    spark.sparkContext.parallelize([oldest_jobs_ids[i:i + limit] for i in range(0, len(oldest_jobs_ids), limit)]) \
        .foreachPartition(lambda job_ids_batch: process_batch(spark, job_ids_batch, url, properties, json_schema, lpm_passname_exists))

    logger.info("Processing complete.")
    spark.stop()
