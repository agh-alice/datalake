import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, concat, col, lit, coalesce, broadcast
from pyspark.sql.types import StringType
import psycopg2
from psycopg2 import sql

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting Spark session")
    spark = SparkSession.builder \
        .appName("postgres_dump") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "4g") \
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

    limit = 500
    cursor = conn.cursor()

    logger.info("Fetching job IDs older than 7 days")
    oldest_jobs_ids_query = "SELECT job_id FROM job_info WHERE last_update < NOW() - INTERVAL '7 days' ORDER BY last_update ASC"

    # Optimized JDBC read with partitioning
    oldest_jobs_ids = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", f"({oldest_jobs_ids_query}) AS oldest_jobs") \
        .option("user", "mon_user") \
        .option("password", "cern") \
        .option("driver", "org.postgresql.Driver") \
        .option("fetchsize", "1000") \
        .load()

    oldest_jobs_ids = oldest_jobs_ids.repartition(100, "job_id").rdd.map(lambda row: row.job_id).collect()

    logger.info(f"Number of job IDs to process: {len(oldest_jobs_ids)}")

    for i in range(0, len(oldest_jobs_ids), limit):
        logger.info(f"Processing batch {i // limit + 1} of {len(oldest_jobs_ids) // limit + 1}")

        job_ids = oldest_jobs_ids[i:min(i + limit, len(oldest_jobs_ids))]
        job_ids_str = ','.join([str(job_id) for job_id in job_ids])

        try:
            logger.info("Loading job_info data from PostgreSQL")
            job_info_df = spark.read \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", f"(SELECT * FROM job_info WHERE job_id IN ({job_ids_str})) AS job_info") \
                .option("user", "mon_user") \
                .option("password", "cern") \
                .option("driver", "org.postgresql.Driver") \
                .option("fetchsize", "1000") \
                .load()

            job_info_df.cache()

            logger.info("Loading mon_jobs_data_v3 data from PostgreSQL")
            mon_jobs_df = spark.read \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", f"(SELECT * FROM mon_jobs_data_v3 WHERE job_id IN ({job_ids_str})) AS mon_jobs_data") \
                .option("user", "mon_user") \
                .option("password", "cern") \
                .option("driver", "org.postgresql.Driver") \
                .option("fetchsize", "1000") \
                .load()

            logger.info("Loading mon_jdls data from PostgreSQL")
            mon_jdls_df = spark.read \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", f"(SELECT * FROM mon_jdls WHERE job_id IN ({job_ids_str})) AS mon_jdls") \
                .option("user", "mon_user") \
                .option("password", "cern") \
                .option("driver", "org.postgresql.Driver") \
                .option("fetchsize", "1000") \
                .load()

            # JDL parsing with optimized schema handling
            logger.info("Parsing JSON schema for mon_jdls")
            json_schema = spark.read.json(mon_jdls_df.rdd.map(lambda row: row.full_jdl)).schema
            json_schema = json_schema.add('LPMPASSNAME', StringType(), True).add('LPMPassName', StringType(), True)

            df_aux = mon_jdls_df.withColumn('jsonData', from_json(mon_jdls_df.full_jdl, json_schema)).select("job_id", "jsonData.*")
            df_aux = df_aux.withColumn("LPMPASSNAME_MERGED", concat(coalesce(col("LPMPASSNAME"), lit('')), coalesce(col("LPMPassName"), lit(''))))
            df_aux = df_aux.drop('LPMPASSNAME').drop('LPMPassName')
            mon_jdls_df = df_aux.withColumnRenamed("LPMPASSNAME_MERGED", "LPMPASSNAME")

            logger.info("Loading trace data from PostgreSQL")
            trace_df = spark.read \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", f"(SELECT * FROM trace WHERE job_id IN ({job_ids_str})) AS trace") \
                .option("user", "mon_user") \
                .option("password", "cern") \
                .option("driver", "org.postgresql.Driver") \
                .option("fetchsize", "1000") \
                .load()

            # Use broadcast for smaller DataFrames if needed
            mon_jdls_df = broadcast(mon_jdls_df)

            logger.info("Writing data to Nessie main branch")
            spark.sql("USE REFERENCE main IN nessie")

            # Write to main branch for each table, handling schema evolution
            if not spark.catalog.tableExists("nessie.job_info"):
                logger.info("Creating new table nessie.job_info")
                job_info_df.writeTo("nessie.job_info").create()
            else:
                logger.info("Appending to nessie.job_info")
                job_info_df.writeTo("nessie.job_info").append()

            if not spark.catalog.tableExists("nessie.mon_jobs_data_v3"):
                logger.info("Creating new table nessie.mon_jobs_data_v3")
                mon_jobs_df.writeTo("nessie.mon_jobs_data_v3").create()
            else:
                logger.info("Appending to nessie.mon_jobs_data_v3")
                mon_jobs_df.writeTo("nessie.mon_jobs_data_v3").append()

            if not spark.catalog.tableExists("nessie.mon_jdls_parsed"):
                logger.info("Creating new table nessie.mon_jdls_parsed")
                mon_jdls_df.writeTo("nessie.mon_jdls_parsed").create()
            else:
                logger.info("Merging schema for nessie.mon_jdls_parsed and appending data")
                mon_jdls_df.writeTo("nessie.mon_jdls_parsed").option("mergeSchema", "true").append()

            if not spark.catalog.tableExists("nessie.trace"):
                logger.info("Creating new table nessie.trace")
                trace_df.writeTo("nessie.trace").create()
            else:
                logger.info("Appending to nessie.trace")
                trace_df.writeTo("nessie.trace").append()

            logger.info("Deleting processed records from PostgreSQL")
            batch_size = 100
            for j in range(0, len(job_ids), batch_size):
                batch_job_ids = job_ids[j:j + batch_size]
                cursor.execute(sql.SQL(f"DELETE FROM job_info WHERE job_id IN ({','.join(map(str, batch_job_ids))})"))
                cursor.execute(sql.SQL(f"DELETE FROM mon_jobs_data_v3 WHERE job_id IN ({','.join(map(str, batch_job_ids))})"))
                cursor.execute(sql.SQL(f"DELETE FROM mon_jdls WHERE job_id IN ({','.join(map(str, batch_job_ids))})"))
                cursor.execute(sql.SQL(f"DELETE FROM trace WHERE job_id IN ({','.join(map(str, batch_job_ids))})"))

            conn.commit()

        except Exception as e:
            logger.error(f"Error processing batch {i // limit + 1}: {e}", exc_info=True)
            conn.rollback()

    cursor.close()
    conn.close()
    logger.info("Closing Spark session")
    spark.stop()
