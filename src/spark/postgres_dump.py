import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, coalesce, col, lit, broadcast, udf
from pyspark.sql.types import StructType, StringType
import psycopg2
from psycopg2 import sql
import json

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

    # Define UDF to decode/unescape JSON strings
    def unescape_json(json_str):
        """
        Unescape JSON string if it's escaped.
        Handles cases where JSON is stored with literal \\n, \\", \\\\ sequences.
        """
        if not json_str:
            return None
        try:
            # Check if string contains literal escape sequences (backslash followed by n, t, r, ", etc.)
            if '\\n' in json_str or '\\"' in json_str or '\\t' in json_str or '\\\\' in json_str:
                # Decode escape sequences using unicode-escape or string-escape codec
                # Use encode().decode() to properly handle escape sequences
                unescaped = json_str.encode('utf-8').decode('unicode-escape')
                return unescaped
            # If already valid JSON, return as-is
            return json_str
        except Exception as e:
            # If unescaping fails, return original
            logger.debug(f"Failed to unescape JSON: {e}")
            return json_str
    
    unescape_json_udf = udf(unescape_json, StringType())
    
    # Cache JSON schema for mon_jdls parsing with larger sample size
    logger.info("Inferring JSON schema for mon_jdls")
    sample_json = spark.read.jdbc(
        url=url, table="(SELECT full_jdl FROM mon_jdls WHERE full_jdl IS NOT NULL LIMIT 1000) AS mon_jdls_sample", properties=properties
    ).select("full_jdl")
    
    # Filter out NULL values before schema inference
    non_null_samples = sample_json.filter(col("full_jdl").isNotNull())
    sample_count = non_null_samples.count()
    logger.info(f"Sample size for schema inference: {sample_count} non-null records")
    
    if sample_count == 0:
        logger.error("No valid JSON samples found in mon_jdls table for schema inference")
        raise ValueError("Cannot infer schema: no valid JSON samples in mon_jdls")
    
    # Unescape JSON strings before schema inference
    unescaped_samples = non_null_samples.withColumn("unescaped_jdl", unescape_json_udf(col("full_jdl")))
    
    # Try to infer schema from unescaped JSON
    json_schema = spark.read.option("mode", "PERMISSIVE").json(unescaped_samples.rdd.map(lambda row: row.unescaped_jdl)).schema
    logger.info(f"Inferred JSON schema fields: {[field.name for field in json_schema.fields]}")
    
    # Check if schema inference failed (only _corrupt_record field)
    if len(json_schema.fields) == 1 and json_schema.fields[0].name == "_corrupt_record":
        logger.warning("Schema inference resulted in _corrupt_record only - JSON may be double-escaped")
        logger.warning("Trying direct JSON parsing without unescaping...")
        # Try without unescaping
        json_schema = spark.read.option("mode", "PERMISSIVE").json(non_null_samples.rdd.map(lambda row: row.full_jdl)).schema
        logger.info(f"Direct parsing schema fields: {[field.name for field in json_schema.fields]}")
        use_unescape = False
    else:
        use_unescape = True
        logger.info(f"Using unescaped JSON parsing")

    # Check if LPMPassName or LPMPASSNAME exists in the inferred schema
    schema_field_names = [field.name for field in json_schema.fields if field.name != "_corrupt_record"]
    lpm_passname_exists = "LPMPassName" in schema_field_names
    lpmpassname_uppercase_exists = "LPMPASSNAME" in schema_field_names
    
    logger.info(f"Schema has {len(schema_field_names)} fields (excluding _corrupt_record)")
    logger.info(f"LPMPassName field exists: {lpm_passname_exists}, LPMPASSNAME exists: {lpmpassname_uppercase_exists}")
    
    # Determine which field name to use
    lpm_field_name = None
    if lpm_passname_exists:
        lpm_field_name = "LPMPassName"
    elif lpmpassname_uppercase_exists:
        lpm_field_name = "LPMPASSNAME"
    
    if lpm_field_name:
        logger.info(f"Will extract field: {lpm_field_name}")

    # Aggregate all processed job IDs for deletion after all batches are processed
    all_processed_job_ids = []

    for i in range(0, len(oldest_jobs_ids), limit):
        logger.info(f"Processing batch {i // limit + 1} of {len(oldest_jobs_ids) // limit + 1}")
        job_ids = oldest_jobs_ids[i:min(i + limit, len(oldest_jobs_ids))]
        job_ids_str = ','.join([str(job_id) for job_id in job_ids])

        try:
            # Load job_info data
            logger.info(f"Loading job_info data for {len(job_ids)} job IDs in batch {i // limit + 1}")
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
            job_info_count = job_info_df.count()
            logger.info(f"job_info records loaded: {job_info_count}")

            # Load mon_jobs_data_v3 data
            logger.info(f"Loading mon_jobs_data_v3 data for batch {i // limit + 1}")
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
            mon_jobs_count = mon_jobs_df.count()
            logger.info(f"mon_jobs_data_v3 records loaded: {mon_jobs_count}")

            # Load and parse mon_jdls data with LPMPassName handling
            mon_jdls_raw = spark.read \
                .format("jdbc") \
                .option("url", url) \
                .option("dbtable", f"(SELECT * FROM mon_jdls WHERE job_id IN ({job_ids_str})) AS mon_jdls") \
                .option("user", "mon_user") \
                .option("password", "cern") \
                .option("driver", "org.postgresql.Driver") \
                .option("fetchsize", "1000") \
                .load()
            
            # Log data retrieval statistics
            mon_jdls_raw_count = mon_jdls_raw.count()
            logger.info(f"mon_jdls records fetched from PostgreSQL for batch {i // limit + 1}: {mon_jdls_raw_count}")
            
            # Check for NULL full_jdl values
            null_jdl_count = mon_jdls_raw.filter(col("full_jdl").isNull()).count()
            if null_jdl_count > 0:
                logger.warning(f"Found {null_jdl_count} records with NULL full_jdl values in batch {i // limit + 1}")
            
            # Filter out NULL values before parsing
            mon_jdls_valid = mon_jdls_raw.filter(col("full_jdl").isNotNull())
            valid_count = mon_jdls_valid.count()
            logger.info(f"Valid (non-NULL) mon_jdls records to parse: {valid_count}")
            
            # Unescape JSON if needed, then parse
            if use_unescape:
                mon_jdls_unescaped = mon_jdls_valid.withColumn("full_jdl_unescaped", unescape_json_udf(col("full_jdl")))
                mon_jdls_df = mon_jdls_unescaped.withColumn("jsonData", from_json(col("full_jdl_unescaped"), json_schema))
            else:
                mon_jdls_df = mon_jdls_valid.withColumn("jsonData", from_json("full_jdl", json_schema))
            
            # Check parsing success
            successful_parse_count = mon_jdls_df.filter(col("jsonData").isNotNull()).count()
            failed_parse_count = mon_jdls_df.filter(col("jsonData").isNull()).count()
            
            logger.info(f"Successfully parsed {successful_parse_count} JSON records, {failed_parse_count} failed")
            
            if failed_parse_count > 0:
                logger.warning(f"Failed to parse {failed_parse_count} JSON records in batch {i // limit + 1}")
                # Log sample of failed records (first 3)
                if use_unescape:
                    failed_samples = mon_jdls_df.filter(col("jsonData").isNull()).select("job_id", "full_jdl", "full_jdl_unescaped").limit(3).collect()
                    for idx, sample in enumerate(failed_samples):
                        logger.warning(f"Failed parse #{idx+1} for job_id {sample.job_id}")
                        logger.warning(f"  Original (first 150 chars): {sample.full_jdl[:150] if sample.full_jdl else 'NULL'}...")
                        logger.warning(f"  Unescaped (first 150 chars): {sample.full_jdl_unescaped[:150] if sample.full_jdl_unescaped else 'NULL'}...")
                else:
                    failed_samples = mon_jdls_df.filter(col("jsonData").isNull()).select("job_id", "full_jdl").limit(3).collect()
                    for idx, sample in enumerate(failed_samples):
                        logger.warning(f"Failed parse #{idx+1} for job_id {sample.job_id}")
                        logger.warning(f"  JSON (first 200 chars): {sample.full_jdl[:200] if sample.full_jdl else 'NULL'}...")
            
            # Log a sample of successful parses for verification
            if successful_parse_count > 0 and i == 0:  # Only log for first batch
                logger.info("Sample of successfully parsed records:")
                if lpm_field_name:
                    success_samples = mon_jdls_df.filter(col("jsonData").isNotNull()).select("job_id", f"jsonData.{lpm_field_name}").limit(3).collect()
                    for sample in success_samples:
                        lpm_value = sample[1] if len(sample) > 1 else None
                        logger.info(f"  job_id {sample.job_id}: {lpm_field_name} = '{lpm_value}'")

            # Flatten ALL JSON fields into separate columns
            if successful_parse_count > 0:
                # Extract jsonData struct into separate columns
                mon_jdls_df = mon_jdls_df.select(
                    col("job_id"),
                    col("jsonData.*")  # This flattens all fields from the struct
                )
                
                # Cast fields to prevent schema evolution errors
                # Convert potentially problematic numeric fields to strings for consistency
                if "HYXRunMergeID" in mon_jdls_df.columns:
                    mon_jdls_df = mon_jdls_df.withColumn("HYXRunMergeID", col("HYXRunMergeID").cast("string"))
                    logger.info("Cast HYXRunMergeID to string for schema compatibility")
                
                logger.info(f"Flattened {len(json_schema.fields)} JSON fields into separate columns")
            else:
                # If no successful parses, just keep job_id with empty LPMPASSNAME
                mon_jdls_df = mon_jdls_df.select(
                    col("job_id"),
                    lit('').alias("LPMPASSNAME")
                )
            
            # Log final count before write
            final_jdl_count = mon_jdls_df.count()
            logger.info(f"mon_jdls_parsed records to write: {final_jdl_count}")

            # Load trace data
            logger.info(f"Loading trace data for batch {i // limit + 1}")
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
            trace_count = trace_df.count()
            logger.info(f"trace records loaded: {trace_count}")

            # Use broadcast for smaller DataFrames if necessary
            mon_jdls_df = broadcast(mon_jdls_df)

            # Write data to Nessie main branch with optimizations
            logger.info("Writing data to Nessie main branch")
            spark.sql("USE REFERENCE main IN nessie")

            logger.info(f"Writing {job_info_count} job_info records to Nessie")
            job_info_df.coalesce(10).writeTo("nessie.job_info").option("numPartitions", 10).append()
            logger.info("job_info write completed")
            
            logger.info(f"Writing {mon_jobs_count} mon_jobs_data_v3 records to Nessie")
            mon_jobs_df.coalesce(10).writeTo("nessie.mon_jobs_data_v3").option("mergeSchema", "false").option("numPartitions", 10).append()
            logger.info("mon_jobs_data_v3 write completed")
            
            logger.info(f"Writing {final_jdl_count} mon_jdls_parsed records to Nessie")
            # Note: Different JDL records have different fields, so schema can vary between batches
            # We only insert columns that already exist in the table
            try:
                # Try to append first (for existing table)
                mon_jdls_df.coalesce(10).writeTo("nessie.mon_jdls_parsed").option("mergeSchema", "true").option("numPartitions", 10).append()
                logger.info("mon_jdls_parsed write completed (appended to existing table)")
            except Exception as e:
                error_msg = str(e)
                logger.info(f"Caught exception during append: {e.__class__.__name__}")
                
                if "TABLE_OR_VIEW_NOT_FOUND" in error_msg or "cannot be found" in error_msg:
                    # Table doesn't exist, create it
                    logger.info("✓ Table doesn't exist, creating new table mon_jdls_parsed")
                    mon_jdls_df.coalesce(10).writeTo("nessie.mon_jdls_parsed").option("numPartitions", 10).create()
                    logger.info("mon_jdls_parsed write completed (created new table)")
                elif "too many data columns" in error_msg or "too many columns" in error_msg or "Cannot write incompatible data" in error_msg:
                    # Schema mismatch - new batch has columns that table doesn't have or type incompatibility
                    # This happens because different JDLs have different fields and types
                    if "Cannot write incompatible data" in error_msg:
                        logger.warning("⚠️  Type incompatibility detected between new data and existing table")
                    else:
                        logger.warning("⚠️  Schema mismatch detected - new data has more columns than existing table")
                    logger.info("Selecting only columns that exist in the table and converting types to match")
                    
                    try:
                        # Get existing table schema
                        existing_table = spark.read.table("nessie.mon_jdls_parsed")
                        table_columns = existing_table.columns
                        logger.info(f"Existing table has {len(table_columns)} columns")
                        logger.info(f"New data has {len(mon_jdls_df.columns)} columns")
                        
                        # Find which columns from new data exist in the table
                        matching_columns = [c for c in table_columns if c in mon_jdls_df.columns]
                        missing_in_new_data = [c for c in table_columns if c not in mon_jdls_df.columns]
                        extra_in_new_data = [c for c in mon_jdls_df.columns if c not in table_columns]
                        
                        logger.info(f"Matching columns: {len(matching_columns)}")
                        logger.info(f"Columns in table but not in new data: {len(missing_in_new_data)}")
                        if missing_in_new_data:
                            logger.info(f"  Missing columns will be filled with NULL: {', '.join(missing_in_new_data[:10])}")
                        logger.info(f"Columns in new data but not in table (will be ignored): {len(extra_in_new_data)}")
                        if extra_in_new_data:
                            logger.info(f"  Ignored columns: {', '.join(extra_in_new_data[:10])}")
                        
                        # Get table schema to check data types
                        table_schema = {field.name: field.dataType for field in existing_table.schema.fields}
                        
                        # Select matching columns, add NULL for missing columns, and cast types to match table
                        selected_columns = []
                        type_mismatches = []
                        from pyspark.sql.functions import to_json, array as spark_array, when, size
                        
                        for column in table_columns:
                            if column in mon_jdls_df.columns:
                                # Get the data types
                                table_type = table_schema[column]
                                new_data_type = mon_jdls_df.schema[column].dataType
                                
                                # Check if types match
                                if str(table_type) != str(new_data_type):
                                    type_mismatches.append(f"{column}: {new_data_type} -> {table_type}")
                                    
                                    # Handle type conversion to match table schema
                                    table_type_str = str(table_type).lower()
                                    new_type_str = str(new_data_type).lower()
                                    
                                    if "array" in table_type_str and "array" not in new_type_str:
                                        # Table expects array but new data is not array (likely string)
                                        # Convert single value to single-element array, or empty array if null
                                        selected_columns.append(
                                            when(col(column).isNull(), lit(None))
                                            .otherwise(spark_array(col(column)))
                                            .alias(column)
                                        )
                                    elif "array" not in table_type_str and "array" in new_type_str:
                                        # Table expects non-array but new data is array
                                        # Convert array to JSON string representation
                                        selected_columns.append(to_json(col(column)).alias(column))
                                    elif "struct" in new_type_str or "map" in new_type_str:
                                        # Complex types - convert to JSON string
                                        selected_columns.append(to_json(col(column)).alias(column))
                                    else:
                                        # Try direct cast for simple types (string, int, long, etc.)
                                        selected_columns.append(col(column).cast(str(table_type)).alias(column))
                                else:
                                    selected_columns.append(col(column))
                            else:
                                # Column exists in table but not in new data, add as NULL with proper type
                                selected_columns.append(lit(None).cast(table_schema[column]).alias(column))
                        
                        if type_mismatches:
                            logger.warning(f"Type mismatches detected for {len(type_mismatches)} columns:")
                            for mismatch in type_mismatches[:10]:  # Show first 10
                                logger.warning(f"  {mismatch}")
                            logger.info("Converting mismatched types to match table schema")
                        
                        # Create filtered dataframe with only table columns
                        filtered_df = mon_jdls_df.select(selected_columns)
                        logger.info(f"Created filtered dataframe with {len(filtered_df.columns)} columns matching table schema")
                        
                        # Try to append again with filtered data
                        filtered_df.coalesce(10).writeTo("nessie.mon_jdls_parsed").option("mergeSchema", "false").option("numPartitions", 10).append()
                        logger.info("mon_jdls_parsed write completed (appended with filtered columns)")
                    except Exception as filter_error:
                        logger.error(f"Failed to filter and append: {filter_error}")
                        raise
                else:
                    # Some other error, re-raise it
                    logger.error(f"Different error occurred, re-raising: {error_msg[:200]}")
                    raise
            
            logger.info(f"Writing {trace_count} trace records to Nessie")
            trace_df.coalesce(10).writeTo("nessie.trace").append()
            logger.info("trace write completed")

            # Accumulate processed job IDs for batch deletion after processing
            all_processed_job_ids.extend(job_ids)
            
            # Log batch summary
            logger.info(f"Batch {i // limit + 1} summary: job_info={job_info_count}, mon_jobs_data_v3={mon_jobs_count}, mon_jdls_parsed={final_jdl_count}, trace={trace_count}")
            logger.info(f"Successfully processed batch {i // limit + 1}")

        except Exception as e:
            logger.error(f"Error processing batch {i // limit + 1}: {e}", exc_info=True)
            logger.error(f"Batch {i // limit + 1} FAILED - job IDs will NOT be deleted from PostgreSQL")
            conn.rollback()
            # Don't add failed job_ids to all_processed_job_ids so they won't be deleted

    try:
        # Delete all processed records from PostgreSQL after all batches are completed
        if all_processed_job_ids:
            logger.info(f"Deleting {len(all_processed_job_ids)} successfully processed job records from PostgreSQL")
            all_job_ids_str = ','.join([str(job_id) for job_id in all_processed_job_ids])
            
            logger.info("Deleting from job_info...")
            cursor.execute(sql.SQL(f"DELETE FROM job_info WHERE job_id IN ({all_job_ids_str})"))
            logger.info(f"Deleted {cursor.rowcount} records from job_info")
            
            logger.info("Deleting from mon_jobs_data_v3...")
            cursor.execute(sql.SQL(f"DELETE FROM mon_jobs_data_v3 WHERE job_id IN ({all_job_ids_str})"))
            logger.info(f"Deleted {cursor.rowcount} records from mon_jobs_data_v3")
            
            logger.info("Deleting from mon_jdls...")
            cursor.execute(sql.SQL(f"DELETE FROM mon_jdls WHERE job_id IN ({all_job_ids_str})"))
            logger.info(f"Deleted {cursor.rowcount} records from mon_jdls")
            
            logger.info("Deleting from trace...")
            cursor.execute(sql.SQL(f"DELETE FROM trace WHERE job_id IN ({all_job_ids_str})"))
            logger.info(f"Deleted {cursor.rowcount} records from trace")
            
            logger.info("Finished deleting records from PostgreSQL - committing transaction")
            conn.commit()
        else:
            logger.warning("No job IDs to delete - all batches may have failed")
    except Exception as e:
        logger.error(f"Error during batch deletion: {e}", exc_info=True)
        conn.rollback()

    cursor.close()
    conn.close()
    logger.info("Closing Spark session")
    spark.stop()
