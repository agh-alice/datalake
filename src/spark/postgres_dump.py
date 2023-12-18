"""
Move data from PostgreSQL database to S3 bucket.
"""
from pyspark.sql import SparkSession
import psycopg2
from psycopg2 import sql


if __name__ == "__main__":
    spark = SparkSession.builder.appName("postgres_dump").getOrCreate()

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
    
    oldest_jobs_ids_query = f"SELECT job_id FROM job_info WHERE last_update < NOW() - INTERVAL '7 days' ORDER BY last_update ASC"
    
    oldest_jobs_ids =  spark.read\
            .format("jdbc")\
            .option("url", url)\
            .option("user", "mon_user")\
            .option("password", "cern")\
            .option("driver", "org.postgresql.Driver")\
            .option("query", oldest_jobs_ids_query).load()

    oldest_jobs_ids = oldest_jobs_ids.rdd.map(lambda row: row.job_id).collect()
    print(len(oldest_jobs_ids))

    # dump data in loop so worker doesnt run out of memeory
    for i in range(0, len(oldest_jobs_ids), limit):
        print(f'Batch {i // limit + 1}/{len(oldest_jobs_ids) // limit + 1}')
    
        job_ids = oldest_jobs_ids[i:min(i + limit, len(oldest_jobs_ids) - 1)]
        job_ids_str = ','.join([str(job_id) for job_id in job_ids])
    
        job_info_df = spark.read\
        .format("jdbc")\
        .option("url", url)\
        .option("user", "mon_user")\
        .option("password", "cern")\
        .option("driver", "org.postgresql.Driver")\
        .option("query", f"SELECT * FROM job_info WHERE job_id IN ({job_ids_str})").load()

        mon_jobs_df = spark.read\
            .format("jdbc")\
            .option("url", url)\
            .option("user", "mon_user")\
            .option("password", "cern")\
            .option("driver", "org.postgresql.Driver")\
            .option("query", f"SELECT * FROM mon_jobs_data_v3 WHERE job_id IN ({job_ids_str})").load()

        mon_jdls_df = spark.read\
            .format("jdbc")\
            .option("url", url)\
            .option("user", "mon_user")\
            .option("password", "cern")\
            .option("driver", "org.postgresql.Driver")\
            .option("query", f"SELECT * FROM mon_jdls WHERE job_id IN ({job_ids_str})").load()

        trace_df = spark.read\
            .format("jdbc")\
            .option("url", url)\
            .option("user", "mon_user")\
            .option("password", "cern")\
            .option("driver", "org.postgresql.Driver")\
            .option("query", f"SELECT * FROM trace WHERE job_id IN ({job_ids_str})").load()

        spark.sql("MERGE BRANCH main INTO temp IN nessie")
        spark.sql("USE REFERENCE temp IN nessie")
        
        if not spark.catalog.tableExists("nessie.job_info"):
            job_info_df.writeTo("nessie.job_info").create()
        else:
            job_info_df.writeTo("nessie.job_info").append() 
        
        if not spark.catalog.tableExists("nessie.mon_jobs_data_v3"):
            mon_jobs_df.writeTo("nessie.mon_jobs_data_v3").create()
        else:
            mon_jobs_df.writeTo("nessie.mon_jobs_data_v3").append()
        
        if not spark.catalog.tableExists("nessie.mon_jdls"):
            mon_jdls_df.writeTo("nessie.mon_jdls").create()
        else:
            mon_jdls_df.writeTo("nessie.mon_jdls").append()

        if not spark.catalog.tableExists("nessie.trace"):
            trace_df.writeTo("nessie.trace").create()
        else:
            trace_df.writeTo("nessie.trace").append()

        spark.sql("MERGE BRANCH temp INTO main IN nessie")
        spark.sql("USE REFERENCE main IN nessie")

        
        cursor.execute(sql.SQL(f"DELETE FROM job_info WHERE job_id IN ({job_ids_str})"))
        cursor.execute(sql.SQL(f"DELETE FROM mon_jobs_data_v3 WHERE job_id IN ({job_ids_str})"))
        cursor.execute(sql.SQL(f"DELETE FROM mon_jdls WHERE job_id IN ({job_ids_str})"))
        cursor.execute(sql.SQL(f"DELETE FROM trace WHERE job_id IN ({job_ids_str})"))
        
        conn.commit()

    # cleanup
    cursor.close()


    spark.stop()