import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, coalesce, col, lit, broadcast
from pyspark.sql.types import StructType, StringType
import psycopg2
from psycopg2 import sql

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting Spark session")
    spark = SparkSession.builder \
        .appName("postgres_dump") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "10") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    url = "jdbc:postgresql://149.156.10.139:5432/mon_data"
    properties = {
        "user": "mon_user",
        "password": "cern",
        "driver": "org.postgresql.Driver"
    }

    conn = psycopg2.connect(
        dbname="mon_data",
        user="mon_user",
        password="cern",
        host="149.156.10.139",
        port="5432"
    )

    limit = 10000
    cursor = conn.cursor()

    logger.info("Fetching job IDs older than 7 days")
    oldest_jobs_ids_query = "SELECT job_id FROM job_info WHERE last_update < NOW() - INTERVAL '7 days' ORDER BY last_update ASC"
    oldest_jobs_ids = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", f"({oldest_jobs_ids_query}) AS oldest_jobs") \
        .option("user", "mon_user") \
        .option("password", "cern") \
        .option("driver", "org.postgresql.Driver") \
        .option("fetchsize", "1000") \
        .load() \
        .repartition(10, "job_id") \
        .rdd.map(lambda row: row.job_id).collect()

    logger.info(f"Number of job IDs to process: {len(oldest_jobs_ids)}")

    # Cache JSON schema for mon_jdls parsing
    logger.info("Inferring JSON schema for mon_jdls")
    sample_json = spark.read.jdbc(
        url=url, table="(SELECT full_jdl FROM mon_jdls LIMIT 10) AS mon_jdls_sample", properties=properties
    ).select("full_jdl")
    json_schema = spark.read.json(sample_json.rdd.map(lambda row: row.full_jdl)).schema

    # Check if LPMPassName exists in the inferred schema
    lpm_passname_exists = "LPMPassName" in [field.name for field in json_schema.fields]

    # Aggregate all processed job IDs for deletion after all batches are processed
    all_processed_job_ids = []

    for i in range(0, len(oldest_jobs_ids), limit):
        logger.info(f"Processing batch {i // limit + 1} of {len(oldest_jobs_ids) // limit + 1}")
        job_ids = oldest_jobs_ids[i:min(i + limit, len(oldest_jobs_ids))]
        job_ids_str = ','.join([str(job_id) for job_id in job_ids])

        try:
            # Load job_info data
            job_info_df = spark.read \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", f"(SELECT * FROM job_info WHERE job_id IN ({job_ids_str})) AS job_info") \
                .option("user", "mon_user") \
                .option("password", "cern") \
                .option("driver", "org.postgresql.Driver") \
                .option("fetchsize", "1000") \
                .load() \
                .fillna("").repartition("job_id")
            job_info_df.cache()

            # Load mon_jobs_data_v3 data
            mon_jobs_df = spark.read \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", f"(SELECT * FROM mon_jobs_data_v3 WHERE job_id IN ({job_ids_str})) AS mon_jobs_data") \
                .option("user", "mon_user") \
                .option("password", "cern") \
                .option("driver", "org.postgresql.Driver") \
                .option("fetchsize", "1000") \
                .load() \
                .fillna("").repartition("job_id")

            # Load and parse mon_jdls data with LPMPassName handling
            mon_jdls_df = spark.read \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", f"(SELECT * FROM mon_jdls WHERE job_id IN ({job_ids_str})) AS mon_jdls") \
                .option("user", "mon_user") \
                .option("password", "cern") \
                .option("driver", "org.postgresql.Driver") \
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
            trace_df = spark.read \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", f"(SELECT * FROM trace WHERE job_id IN ({job_ids_str})) AS trace") \
                .option("user", "mon_user") \
                .option("password", "cern") \
                .option("driver", "org.postgresql.Driver") \
                .option("fetchsize", "1000") \
                .load() \
                .fillna("").repartition("job_id")

            # Use broadcast for smaller DataFrames if necessary
            mon_jdls_df = broadcast(mon_jdls_df)

            # Write data to Nessie main branch with optimizations
            logger.info("Writing data to Nessie main branch")
            spark.sql("USE REFERENCE main IN nessie")

            job_info_df.coalesce(10).writeTo("nessie.job_info").option("numPartitions", 10).append()
            mon_jobs_df.coalesce(10).writeTo("nessie.mon_jobs_data_v3").option("mergeSchema", "false").option("numPartitions", 10).append()
            mon_jdls_df.coalesce(10).writeTo("nessie.mon_jdls_parsed").option("mergeSchema", "true").option("numPartitions", 10).append()
            trace_df.coalesce(10).writeTo("nessie.trace").append()

            # Accumulate processed job IDs for batch deletion after processing
            all_processed_job_ids.extend(job_ids)

        except Exception as e:
            logger.error(f"Error processing batch {i // limit + 1}: {e}", exc_info=True)
            conn.rollback()

    try:
        # Delete all processed records from PostgreSQL after all batches are completed
        if all_processed_job_ids:
            logger.info("Deleting processed records from PostgreSQL")
            all_job_ids_str = ','.join([str(job_id) for job_id in all_processed_job_ids])
            cursor.execute(sql.SQL(f"DELETE FROM job_info WHERE job_id IN ({all_job_ids_str})"))
            cursor.execute(sql.SQL(f"DELETE FROM mon_jobs_data_v3 WHERE job_id IN ({all_job_ids_str})"))
            cursor.execute(sql.SQL(f"DELETE FROM mon_jdls WHERE job_id IN ({all_job_ids_str})"))
            cursor.execute(sql.SQL(f"DELETE FROM trace WHERE job_id IN ({all_job_ids_str})"))
            logger.info("Finished deleting records from PostgreSQL")
            conn.commit()
    except Exception as e:
        logger.error(f"Error during batch deletion: {e}", exc_info=True)
        conn.rollback()

    cursor.close()
    conn.close()
    logger.info("Closing Spark session")
    spark.stop()
