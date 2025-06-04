# Databricks notebook source
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (col, avg, count, lit, current_timestamp, expr, when, sum as spark_sum, greatest, least, coalesce,percentile_approx)
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, LongType,FloatType, DecimalType, TimestampType)
import pytz
from datetime import datetime
import uuid

# --- Configuration - VERIFY ALL THESE PATHS AND CREDENTIALS ---
# S3 Paths
INPUT_S3_PATH = "s3a://daso-de-assignment-transaction-chunks/"
OUTPUT_S3_PATH = "s3a://daso-de-assignment-detection-outputs/" 
CUSTOMER_IMPORTANCE_PATH = "dbfs:/FileStore/CustomerImportance.csv" # Ensure this file is in DBFS
ARCHIVE_S3_PATH = "s3a://daso-de-assignment-transaction-chunks/archive/" # Should exist, even if cleanSource is off for now
CHECKPOINT_S3_PATH = "s3a://daso-de-assignment-auxiliary-data/checkpoints/final_run_v5_no_cleansource/" # <<<< NEW CHECKPOINT PATH

# PostgreSQL Connection Details
PG_HOST = "de-assignment-pg-db.c9g4wki6o511.ap-south-1.rds.amazonaws.com"  
PG_PORT = "5432"
PG_DATABASE = "transactions_db"
PG_USER = "postgres"
PG_PASSWORD = "*******" # <<<< ENSURE THIS IS YOUR CORRECT RDS PASSWORD

PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
PG_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder.appName("MechanismY_Final_Attempt").getOrCreate()
print("Spark Session Created.")

# --- Define Schemas ---
transaction_schema = StructType([ 
    StructField("step", IntegerType(), True), StructField("customer", StringType(), True),
    StructField("age", StringType(), True), StructField("gender", StringType(), True),
    StructField("zipcodeOri", StringType(), True), StructField("merchant", StringType(), True),
    StructField("zipMerchant", StringType(), True), StructField("category", StringType(), True),
    StructField("amount", FloatType(), True), StructField("fraud", IntegerType(), True)
])
importance_schema = StructType([ 
    StructField("Source", StringType(), True), StructField("Target", StringType(), True),
    StructField("Weight", FloatType(), True), StructField("typeTrans", StringType(), True),
    StructField("fraud", IntegerType(), True) # Assumes 'fraud' is the header in CustomerImportance.csv
])
schema_merchant_summary = StructType([ # For fallback if PG read fails
    StructField("merchant_id", StringType(), True), StructField("total_transactions", LongType(), True),
    StructField("last_updated", TimestampType(), True)
])
schema_customer_merchant_summary = StructType([ # For fallback
    StructField("customer_id", StringType(), True), StructField("merchant_id", StringType(), True),
    StructField("transaction_count", LongType(), True), StructField("total_amount_sum", DecimalType(18, 2), True),
    StructField("last_updated", TimestampType(), True)
])
schema_merchant_gender_summary = StructType([ # For fallback
    StructField("merchant_id", StringType(), True), StructField("male_transaction_count", LongType(), True),
    StructField("female_transaction_count", LongType(), True), StructField("last_updated", TimestampType(), True)
])
output_df_schema = StructType([ # For writing detections
    StructField("YStartTime", StringType(), True), StructField("DetectionTime", StringType(), True),
    StructField("PatternId", StringType(), True), StructField("ActionType", StringType(), True),
    StructField("CustomerName", StringType(), True), StructField("MerchantId", StringType(), True)
])

# --- Load CustomerImportance data ---
try:
    customer_importance_df = spark.read.format("csv").option("header","true").schema(importance_schema).load(CUSTOMER_IMPORTANCE_PATH)
    customer_importance_df = customer_importance_df.withColumnRenamed("fraud", "ci_fraud")
    customer_importance_df.cache()
    print("CustomerImportance DataFrame loaded and cached.")
    # customer_importance_df.show(5, False)
except Exception as e_cust_imp:
    print(f"FATAL ERROR loading CustomerImportance.csv: {e_cust_imp}")
    # Potentially stop execution if this critical data can't be loaded
    # For now, we'll let it proceed and it might fail later or produce incorrect results
    customer_importance_df = None 


# --- Pre-calculate 1st percentile weight from CustomerImportance ---
weight_percentiles_df = None # Initialize
if customer_importance_df is not None:
    try:
        weight_percentiles_df = customer_importance_df \
            .groupBy("Target", "typeTrans") \
            .agg(percentile_approx("Weight", 0.01).alias("p1_weight")) \
            .withColumnRenamed("Target", "merchant_key") \
            .withColumnRenamed("typeTrans", "category_key")
        weight_percentiles_df.cache()
        print("Calculated 1st percentile weights for CustomerImportance.")
        # weight_percentiles_df.show(5, False) 
    except Exception as e_perc:
        print(f"Warning: Could not calculate weight percentiles, PatId1 will use fixed fallback: {e_perc}")
        weight_percentiles_df = None
else:
    print("Warning: CustomerImportance DataFrame not loaded, cannot calculate weight percentiles. PatId1 will use fixed fallback.")


# --- Read Transaction Stream (cleanSource DISABLED) ---
transactions_stream_df = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .schema(transaction_schema) \
    .option("maxFilesPerTrigger", 1) \
    .load(INPUT_S3_PATH)
    # .option("cleanSource", "archive")  # <<<< KEPT DISABLED TO AVOID S3 ARCHIVER ISSUES
    # .option("sourceArchiveDir", ARCHIVE_S3_PATH) 
    
print("Transaction stream reader defined (cleanSource is DISABLED for this run).")


def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S")

detections_buffer = [] 
DETECTION_BATCH_SIZE = 50

# --- foreachBatch function ---
def process_batch(batch_df, epoch_id):
    global detections_buffer
    y_start_time_ist = get_ist_time()
    print(f"\n--- Processing Batch {epoch_id} starting at {y_start_time_ist} (IST) ---")

    if batch_df.isEmpty():
        print(f"Batch {epoch_id}: Empty. Skipping.")
        return

    batch_df.persist()
    batch_row_count = batch_df.count() # Action to materialize batch_df
    print(f"Batch {epoch_id} - Initial Row Count: {batch_row_count}")
    if batch_row_count == 0 : # Double check after count
        print(f"Batch {epoch_id}: Confirmed empty after count. Skipping actual processing.")
        batch_df.unpersist()
        return

    # ---- 1. UPDATE POSTGRESQL WITH BATCH AGGREGATES ----
    
    # == Upsert Merchant Transaction Summary ==
    target_table_mts = "merchant_transaction_summary"
    temp_table_mts = f"temp_mts_updates_batch_{epoch_id}" 
    
    batch_merchant_summary_df = batch_df.groupBy("merchant") \
        .agg(count("*").alias("current_batch_tx_count")) \
        .withColumn("current_batch_ts", current_timestamp()) \
        .withColumnRenamed("merchant", "merchant_id_src")

    batch_merchant_summary_df.write.mode("overwrite").jdbc(url=PG_JDBC_URL, table=temp_table_mts, properties=PG_PROPERTIES)
    print(f"Batch {epoch_id} - Wrote to {temp_table_mts}")

    conn_mts = None; stmt_mts = None
    try:
        conn_mts = spark.sparkContext._gateway.jvm.java.sql.DriverManager.getConnection(PG_JDBC_URL, PG_USER, PG_PASSWORD)
        conn_mts.setAutoCommit(True)
        stmt_sql_mts = f"""
        INSERT INTO {target_table_mts} (merchant_id, total_transactions, last_updated)
        SELECT source.merchant_id_src, source.current_batch_tx_count, source.current_batch_ts FROM {temp_table_mts} AS source
        ON CONFLICT (merchant_id) DO UPDATE SET
            total_transactions = {target_table_mts}.total_transactions + EXCLUDED.total_transactions,
            last_updated = EXCLUDED.last_updated;"""
        stmt_mts = conn_mts.createStatement(); rows_affected_mts = stmt_mts.executeUpdate(stmt_sql_mts)
        print(f"Batch {epoch_id} - Upserted {target_table_mts}. Rows: {rows_affected_mts}")
    except Exception as e: print_jdbc_error(e, target_table_mts, epoch_id)
    finally: close_jdbc_resources(stmt_mts, conn_mts)

    # == Upsert Customer-Merchant Summary ==
    target_table_cms = "customer_merchant_summary"; temp_table_cms = f"temp_cms_updates_batch_{epoch_id}"
    batch_customer_merchant_summary_df = batch_df.groupBy("customer", "merchant").agg(count("*").alias("c_b_tx_c"), spark_sum("amount").alias("c_b_s_a")).withColumn("c_b_ts", current_timestamp()).withColumnRenamed("customer", "c_id_src").withColumnRenamed("merchant", "m_id_src")
    batch_customer_merchant_summary_df.write.mode("overwrite").jdbc(url=PG_JDBC_URL, table=temp_table_cms, properties=PG_PROPERTIES)
    print(f"Batch {epoch_id} - Wrote to {temp_table_cms}")
    conn_cms = None; stmt_cms = None
    try:
        conn_cms = spark.sparkContext._gateway.jvm.java.sql.DriverManager.getConnection(PG_JDBC_URL, PG_USER, PG_PASSWORD); conn_cms.setAutoCommit(True)
        stmt_sql_cms = f"""
        INSERT INTO {target_table_cms} (customer_id, merchant_id, transaction_count, total_amount_sum, last_updated)
        SELECT source.c_id_src, source.m_id_src, source.c_b_tx_c, source.c_b_s_a, source.c_b_ts FROM {temp_table_cms} AS source
        ON CONFLICT (customer_id, merchant_id) DO UPDATE SET
            transaction_count = {target_table_cms}.transaction_count + EXCLUDED.transaction_count,
            total_amount_sum = COALESCE({target_table_cms}.total_amount_sum, 0.0) + COALESCE(EXCLUDED.total_amount_sum, 0.0),
            last_updated = EXCLUDED.last_updated;"""
        stmt_cms = conn_cms.createStatement(); rows_affected_cms = stmt_cms.executeUpdate(stmt_sql_cms)
        print(f"Batch {epoch_id} - Upserted {target_table_cms}. Rows: {rows_affected_cms}")
    except Exception as e: print_jdbc_error(e, target_table_cms, epoch_id)
    finally: close_jdbc_resources(stmt_cms, conn_cms)

    # == Upsert Merchant Gender Summary ==
    target_table_mgs = "merchant_gender_summary"; temp_table_mgs = f"temp_mgs_updates_batch_{epoch_id}"
    batch_merchant_gender_pivot_df = batch_df.groupBy("merchant").pivot("gender", ["M", "F"]).agg(count("*")).fillna(0)
    if 'M' not in batch_merchant_gender_pivot_df.columns: batch_merchant_gender_pivot_df = batch_merchant_gender_pivot_df.withColumn('M', lit(0))
    if 'F' not in batch_merchant_gender_pivot_df.columns: batch_merchant_gender_pivot_df = batch_merchant_gender_pivot_df.withColumn('F', lit(0))
    batch_merchant_gender_summary_df = batch_merchant_gender_pivot_df.select(col("merchant").alias("m_id_src"), col("M").alias("c_b_m_c"), col("F").alias("c_b_f_c")).withColumn("c_b_ts", current_timestamp())
    batch_merchant_gender_summary_df.write.mode("overwrite").jdbc(url=PG_JDBC_URL, table=temp_table_mgs, properties=PG_PROPERTIES)
    print(f"Batch {epoch_id} - Wrote to {temp_table_mgs}")
    conn_mgs = None; stmt_mgs = None
    try:
        conn_mgs = spark.sparkContext._gateway.jvm.java.sql.DriverManager.getConnection(PG_JDBC_URL, PG_USER, PG_PASSWORD); conn_mgs.setAutoCommit(True)
        stmt_sql_mgs = f"""
        INSERT INTO {target_table_mgs} (merchant_id, male_transaction_count, female_transaction_count, last_updated)
        SELECT source.m_id_src, source.c_b_m_c, source.c_b_f_c, source.c_b_ts FROM {temp_table_mgs} AS source
        ON CONFLICT (merchant_id) DO UPDATE SET
            male_transaction_count = {target_table_mgs}.male_transaction_count + EXCLUDED.male_transaction_count,
            female_transaction_count = {target_table_mgs}.female_transaction_count + EXCLUDED.female_transaction_count,
            last_updated = EXCLUDED.last_updated;"""
        stmt_mgs = conn_mgs.createStatement(); rows_affected_mgs = stmt_mgs.executeUpdate(stmt_sql_mgs)
        print(f"Batch {epoch_id} - Upserted {target_table_mgs}. Rows: {rows_affected_mgs}")
    except Exception as e: print_jdbc_error(e, target_table_mgs, epoch_id)
    finally: close_jdbc_resources(stmt_mgs, conn_mgs)

    # ---- 2. READ FULL STATE FROM POSTGRESQL ----
    try:
        pg_merchant_summary_df = spark.read.jdbc(url=PG_JDBC_URL, table="merchant_transaction_summary", properties=PG_PROPERTIES)
        pg_customer_merchant_summary_df = spark.read.jdbc(url=PG_JDBC_URL, table="customer_merchant_summary", properties=PG_PROPERTIES)
        pg_merchant_gender_summary_df = spark.read.jdbc(url=PG_JDBC_URL, table="merchant_gender_summary", properties=PG_PROPERTIES)
        print(f"Batch {epoch_id} - Successfully read summary tables from PostgreSQL.")
    except Exception as e_read_pg:
        print(f"ERROR Batch {epoch_id} - reading summary tables: {e_read_pg}. Creating empty fallback.")
        pg_merchant_summary_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema_merchant_summary)
        pg_customer_merchant_summary_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema_customer_merchant_summary)
        pg_merchant_gender_summary_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema_merchant_gender_summary)

    # ---- 3. ENRICH BATCH DATA ----
    enriched_df = batch_df.join(customer_importance_df, (batch_df.customer == customer_importance_df.Source) & (batch_df.merchant == customer_importance_df.Target) & (batch_df.category == customer_importance_df.typeTrans), "left_outer")

    # ---- 4. DETECT PATTERNS ----
    # TEST Thresholds
    PATID1_MERCHANT_TX_THRESHOLD = 5; PATID1_CUSTOMER_TX_THRESHOLD_PER_MERCHANT = 2
    PATID2_MIN_TRANSACTIONS = 3; PATID2_AVG_AMOUNT_THRESHOLD = 23.0
    PATID3_MIN_FEMALE_TRANSACTIONS = 2 

    enriched_df_persisted = enriched_df.persist() # Persist for reuse in PatId1 low weight check

    active_merchants_df = pg_merchant_summary_df.filter(col("total_transactions") > PATID1_MERCHANT_TX_THRESHOLD).select(col("merchant_id").alias("upg_mid"))
    high_tx_cust_df = pg_customer_merchant_summary_df.filter(col("transaction_count") > PATID1_CUSTOMER_TX_THRESHOLD_PER_MERCHANT).select(col("customer_id").alias("upg_cid"), col("merchant_id").alias("upg_mid_cust"))
        
    if weight_percentiles_df is not None and not weight_percentiles_df.rdd.isEmpty():
        low_weight_df = enriched_df_persisted.join(weight_percentiles_df, (enriched_df_persisted.merchant == weight_percentiles_df.merchant_key) & (enriched_df_persisted.category == weight_percentiles_df.category_key),"inner").filter(col("Weight") < col("p1_weight")).select("customer", "merchant").distinct().withColumnRenamed("customer", "lw_cid").withColumnRenamed("merchant", "lw_mid")
    else: 
        low_weight_df = enriched_df_persisted.filter(col("Weight").isNotNull() & (col("Weight") < 2.0)).select("customer", "merchant").distinct().withColumnRenamed("customer", "lw_cid").withColumnRenamed("merchant", "lw_mid")

    patid1_detections = active_merchants_df.join(high_tx_cust_df, active_merchants_df.upg_mid == high_tx_cust_df.upg_mid_cust, "inner").join(low_weight_df,(col("upg_mid") == col("lw_mid")) & (col("upg_cid") == col("lw_cid")),"inner").select(lit(y_start_time_ist).alias("YStartTime"),lit(get_ist_time()).alias("DetectionTime"),lit("PatId1").alias("PatternId"),lit("UPGRADE").alias("ActionType"),col("upg_cid").alias("CustomerName"),col("upg_mid").alias("MerchantId")).distinct()
    
    enriched_df_persisted.unpersist()

    patid2_detections = pg_customer_merchant_summary_df.withColumn("avg_tx_val", coalesce(col("total_amount_sum"), lit(0.0)) / coalesce(col("transaction_count"), lit(1.0))).filter((col("transaction_count") >= PATID2_MIN_TRANSACTIONS) & (col("avg_tx_val") < PATID2_AVG_AMOUNT_THRESHOLD)).select(lit(y_start_time_ist).alias("YStartTime"),lit(get_ist_time()).alias("DetectionTime"),lit("PatId2").alias("PatternId"),lit("CHILD").alias("ActionType"),col("customer_id").alias("CustomerName"),col("merchant_id").alias("MerchantId"))
    patid3_detections = pg_merchant_gender_summary_df.filter((col("female_transaction_count") < col("male_transaction_count")) & (col("female_transaction_count") > PATID3_MIN_FEMALE_TRANSACTIONS)).select(lit(y_start_time_ist).alias("YStartTime"),lit(get_ist_time()).alias("DetectionTime"),lit("PatId3").alias("PatternId"),lit("DEI-NEEDED").alias("ActionType"),lit("").alias("CustomerName"),col("merchant_id").alias("MerchantId"))

    # ---- 5. COMBINE AND WRITE DETECTIONS ----
    all_detections_list = [patid1_detections, patid2_detections, patid3_detections]
    final_detection_columns = ["YStartTime", "DetectionTime", "PatternId", "ActionType", "CustomerName", "MerchantId"]
    
    # Initialize an empty DataFrame with the target schema for unioning
    # This ensures a base schema if all individual detection DFs are empty
    unioned_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), output_df_schema)

    for df_to_union in all_detections_list:
        if df_to_union is not None and not df_to_union.rdd.isEmpty():
            # Ensure the df_to_union also conforms to the final_detection_columns or schema before union
            # Our construction of patidX_detections should make them conformant.
             unioned_df = unioned_df.unionByName(df_to_union.select(final_detection_columns).fillna("")) # Select and fillna before union

    all_detections_df_final = unioned_df # Now unioned_df has all detections or is empty with correct schema
            
    collected_detections = all_detections_df_final.collect()
    
    batch_df.unpersist() # enriched_df was unpersisted earlier

    if collected_detections:
        print(f"Batch {epoch_id} - Collected {len(collected_detections)} detections.")
        detections_buffer.extend(collected_detections) 
        while len(detections_buffer) >= DETECTION_BATCH_SIZE:
            to_write_list = detections_buffer[:DETECTION_BATCH_SIZE]
            detections_buffer = detections_buffer[DETECTION_BATCH_SIZE:]
            if to_write_list:
                output_df = spark.createDataFrame(to_write_list, schema=output_df_schema) 
                output_subdir_name = f"detections_batch_{epoch_id}_{str(uuid.uuid4())[:8]}"
                full_output_path = OUTPUT_S3_PATH + output_subdir_name
                print(f"Batch {epoch_id} - Writing {len(to_write_list)} detections to {full_output_path}")
                output_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(full_output_path)
    else:
        print(f"Batch {epoch_id} - No detections in this batch.")
        
    print(f"--- Finished processing Batch {epoch_id} ---")

# Helper for JDBC error printing
def print_jdbc_error(e, table_name, epoch_id):
    print(f"FATAL ERROR upserting {table_name} in Batch {epoch_id}: {e}")
    if hasattr(e, 'java_exception') and e.java_exception is not None:
        java_ex = e.java_exception
        print(f"  JAVA EXCEPTION Type: {type(java_ex)}")
        print(f"  JAVA EXCEPTION Msg: {java_ex.getMessage()}")
        if hasattr(java_ex, 'getSQLState'): print(f"  JAVA EXCEPTION SQLState: {java_ex.getSQLState()}")
        if hasattr(java_ex, 'getErrorCode'): print(f"  JAVA EXCEPTION ErrorCode: {java_ex.getErrorCode()}")
        java_ex.printStackTrace()

# Helper for closing JDBC resources
def close_jdbc_resources(stmt, conn):
    if stmt is not None:
        try: stmt.close()
        except Exception as e_cls_stmt: print(f"WARN: Error closing statement: {e_cls_stmt}")
    if conn is not None:
        try: conn.close()
        except Exception as e_cls_conn: print(f"WARN: Error closing connection: {e_cls_conn}")

# --- Start the Streaming Query ---
print("Starting FINAL Corrected streaming query (with explicit INSERT ON CONFLICT and cleanSource DISABLED)...")
query = transactions_stream_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_S3_PATH) \
    .trigger(processingTime='30 seconds') \
    .start()

print(f"Streaming query '{query.name}' (id: {query.id}) started.")
query.awaitTermination()