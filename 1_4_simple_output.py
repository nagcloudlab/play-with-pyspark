# 1_4_simple_outputs.py
# Step 1.4: Simple Outputs - Multiple Sinks and Error Handling
# Purpose: Learn different output formats, sinks, and error handling patterns

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# ============================================================================
# LEARNING OBJECTIVES FOR STEP 1.4:
# 1. Multiple output sinks (files, Kafka, Cassandra, console)
# 2. Different file formats (JSON, Parquet, CSV)
# 3. Error handling and dead letter queues
# 4. Checkpoint management and recovery
# 5. Output modes and their use cases
# 6. Partitioning strategies for outputs
# ============================================================================

def create_spark_session():
    """Create Spark session with checkpoint configuration"""
    return SparkSession.builder \
        .appName("FinancialOutputs") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
        .getOrCreate()

spark = create_spark_session()
spark.sparkContext.setLogLevel("WARN")

# Create checkpoint and output directories
os.makedirs("/tmp/financial-outputs", exist_ok=True)
os.makedirs("/tmp/spark-checkpoints", exist_ok=True)
os.makedirs("/tmp/financial-outputs/valid-transactions", exist_ok=True)
os.makedirs("/tmp/financial-outputs/invalid-transactions", exist_ok=True)
os.makedirs("/tmp/financial-outputs/high-risk-transactions", exist_ok=True)

# ============================================================================
# TRANSACTION SCHEMA (reusing from previous steps)
# ============================================================================
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("processing_date", StringType(), True),
    StructField("location", StructType([
        StructField("country", StringType(), True),
        StructField("city", StringType(), True)
    ]), True),
    StructField("channel", StringType(), True),
    StructField("status", StringType(), True),
    StructField("metadata", StructType([
        StructField("ip_address", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("session_id", StringType(), True)
    ]), True)
])

# ============================================================================
# REUSE TRANSFORMATION FUNCTIONS FROM STEP 1.3
# ============================================================================
def read_and_parse_transactions():
    """Read and parse transaction stream"""
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    return kafka_df \
        .select(
            col("key").cast("string").alias("partition_key"),
            col("timestamp").alias("kafka_timestamp"),
            col("topic"), col("partition"), col("offset"),
            from_json(col("value").cast("string"), transaction_schema).alias("transaction")
        ) \
        .select(
            col("partition_key"), col("kafka_timestamp"), 
            col("topic"), col("partition"), col("offset"),
            col("transaction.*"),
            to_timestamp(col("transaction.event_time")).alias("event_timestamp")
        )

def apply_all_transformations(df):
    """Apply all transformations from Step 1.3 in one function"""
    return df \
        .withColumn("is_valid_amount", (col("amount") > 0) & (col("amount") <= 100000)) \
        .withColumn("is_valid_currency", col("currency").rlike("^[A-Z]{3}$")) \
        .withColumn("is_valid_account", col("account_id").rlike("^ACC[0-9]{6}$")) \
        .withColumn("is_valid_transaction_type", 
                   col("transaction_type").isin(["purchase", "refund", "transfer", "withdrawal", "deposit"])) \
        .withColumn("overall_data_quality",
                   col("is_valid_amount") & col("is_valid_currency") & 
                   col("is_valid_account") & col("is_valid_transaction_type")) \
        .withColumn("transaction_fee",
                   when(col("transaction_type") == "transfer", 
                        when(col("amount") <= 1000, 2.50).otherwise(5.00))
                   .when(col("transaction_type") == "withdrawal", 2.00)
                   .otherwise(0.00)) \
        .withColumn("amount_usd",
                   when(col("currency") == "USD", col("amount"))
                   .when(col("currency") == "EUR", col("amount") * 1.10)
                   .when(col("currency") == "GBP", col("amount") * 1.25)
                   .otherwise(col("amount"))) \
        .withColumn("risk_score",
                   when(col("amount_usd") >= 5000, 25).otherwise(0) +
                   when(col("location.country") != "US", 20).otherwise(0) +
                   when(col("channel") == "online", 10).otherwise(0)) \
        .withColumn("risk_level",
                   when(col("risk_score") >= 40, "HIGH")
                   .when(col("risk_score") >= 20, "MEDIUM")
                   .otherwise("LOW"))

# ============================================================================
# OUTPUT SINK 1: FILE OUTPUTS WITH DIFFERENT FORMATS
# Purpose: Learn file-based outputs for data lake storage
# ============================================================================
def setup_file_outputs(valid_txns, invalid_txns, high_risk_txns):
    """Setup file-based outputs in different formats"""
    
    print("📁 Setting up file-based outputs...")
    
    # OUTPUT 1: Valid transactions to Parquet (efficient columnar format)
    valid_parquet_query = valid_txns \
        .select(
            "transaction_id", "account_id", "transaction_type", 
            "amount", "amount_usd", "currency", "transaction_fee",
            "location.country", "location.city", "channel", 
            "risk_score", "risk_level", "event_timestamp"
        ) \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/tmp/financial-outputs/valid-transactions") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/valid-parquet") \
        .partitionBy("country")  \
        .trigger(processingTime='30 seconds') \
        .start()
    
    # OUTPUT 2: Invalid transactions to JSON (for debugging)
    invalid_json_query = invalid_txns \
        .select(
            "transaction_id", "account_id", "amount", "currency",
            "is_valid_amount", "is_valid_currency", "is_valid_account",
            "overall_data_quality", "event_timestamp"
        ) \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", "/tmp/financial-outputs/invalid-transactions") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/invalid-json") \
        .trigger(processingTime='60 seconds') \
        .start()
    
    # OUTPUT 3: High-risk transactions to CSV (for compliance team)
    high_risk_csv_query = high_risk_txns \
        .select(
            "transaction_id", "account_id", "transaction_type",
            "amount_usd", "risk_score", "risk_level", 
            "location.country", "channel", "event_timestamp"
        ) \
        .writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("header", "true") \
        .option("path", "/tmp/financial-outputs/high-risk-transactions") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/high-risk-csv") \
        .trigger(processingTime='15 seconds') \
        .start()
    
    print("✅ File outputs configured!")
    print(f"📁 Valid transactions: /tmp/financial-outputs/valid-transactions (Parquet)")
    print(f"📁 Invalid transactions: /tmp/financial-outputs/invalid-transactions (JSON)")
    print(f"📁 High-risk transactions: /tmp/financial-outputs/high-risk-transactions (CSV)")
    
    return valid_parquet_query, invalid_json_query, high_risk_csv_query

# ============================================================================
# OUTPUT SINK 2: KAFKA OUTPUTS FOR DOWNSTREAM SYSTEMS
# Purpose: Send processed data to other Kafka topics for real-time consumption
# ============================================================================
def setup_kafka_outputs(valid_txns, high_risk_txns):
    """Setup Kafka outputs for downstream real-time processing"""
    
    print("📤 Setting up Kafka outputs...")
    
    # OUTPUT 4: Send high-risk transactions to fraud-alerts topic
    fraud_alerts = high_risk_txns \
        .select(
            col("account_id").alias("key"),
            to_json(struct(
                col("transaction_id"),
                col("account_id"), 
                col("amount_usd"),
                col("risk_score"),
                col("risk_level"),
                col("location.country"),
                col("event_timestamp").cast("string").alias("alert_time")
            )).alias("value")
        ) \
        .writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "fraud-alerts") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/fraud-alerts-kafka") \
        .trigger(processingTime='5 seconds') \
        .start()
    
    # OUTPUT 5: Send account summaries to account-updates topic
    account_summaries = valid_txns \
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("account_id")
        ) \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount_usd").alias("total_amount_usd"),
            avg("risk_score").alias("avg_risk_score"),
            max("event_timestamp").alias("last_transaction_time")
        ) \
        .select(
            col("account_id").alias("key"),
            to_json(struct(
                col("account_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("transaction_count"),
                col("total_amount_usd"),
                round(col("avg_risk_score"), 2).alias("avg_risk_score"),
                col("last_transaction_time").cast("string")
            )).alias("value")
        ) \
        .writeStream \
        .outputMode("update") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "account-updates") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/account-updates-kafka") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print("✅ Kafka outputs configured!")
    print(f"📤 Fraud alerts: fraud-alerts topic")
    print(f"📤 Account summaries: account-updates topic")
    
    return fraud_alerts, account_summaries

# ============================================================================
# OUTPUT SINK 3: CONSOLE OUTPUTS FOR MONITORING
# Purpose: Real-time monitoring and debugging outputs
# ============================================================================
def setup_console_outputs(valid_txns, invalid_txns):
    """Setup console outputs for real-time monitoring"""
    
    print("🖥️  Setting up console monitoring outputs...")
    
    # OUTPUT 6: Summary statistics every 30 seconds
    transaction_stats = valid_txns \
        .groupBy(
            window(col("event_timestamp"), "30 seconds"),
            col("transaction_type"),
            col("risk_level")
        ) \
        .agg(
            count("*").alias("count"),
            avg("amount_usd").alias("avg_amount"),
            sum("amount_usd").alias("total_amount")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("transaction_type"),
            col("risk_level"),
            col("count"),
            round(col("avg_amount"), 2).alias("avg_amount_usd"),
            round(col("total_amount"), 2).alias("total_amount_usd")
        ) \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "20") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/stats-console") \
        .trigger(processingTime='30 seconds') \
        .start()
    
    # OUTPUT 7: Data quality monitoring
    quality_monitor = invalid_txns \
        .groupBy(
            window(col("event_timestamp"), "1 minute")
        ) \
        .agg(
            count("*").alias("invalid_count"),
            sum(when(~col("is_valid_amount"), 1).otherwise(0)).alias("invalid_amounts"),
            sum(when(~col("is_valid_currency"), 1).otherwise(0)).alias("invalid_currencies"),
            sum(when(~col("is_valid_account"), 1).otherwise(0)).alias("invalid_accounts")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("invalid_count"),
            col("invalid_amounts"),
            col("invalid_currencies"), 
            col("invalid_accounts")
        ) \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "10") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/quality-console") \
        .trigger(processingTime='60 seconds') \
        .start()
    
    print("✅ Console monitoring configured!")
    print(f"🖥️  Transaction statistics: Every 30 seconds")
    print(f"🖥️  Data quality alerts: Every 60 seconds")
    
    return transaction_stats, quality_monitor

# ============================================================================
# MAIN OUTPUT PIPELINE
# ============================================================================
def main():
    """Execute the complete output pipeline with multiple sinks"""
    
    print("🚀 Starting Step 1.4: Simple Outputs Pipeline")
    print("=" * 60)
    
    # Step 1: Read and transform data
    print("\n📖 Step 1: Reading and transforming transactions...")
    transactions = read_and_parse_transactions()
    enriched_transactions = apply_all_transformations(transactions)
    
    # Step 2: Filter into different streams
    print("\n🔄 Step 2: Filtering transaction streams...")
    valid_txns = enriched_transactions.filter(col("overall_data_quality") == True)
    invalid_txns = enriched_transactions.filter(col("overall_data_quality") == False)
    high_risk_txns = valid_txns.filter(col("risk_level") == "HIGH")
    
    # Step 3: Setup file outputs
    print("\n📁 Step 3: Setting up file outputs...")
    file_queries = setup_file_outputs(valid_txns, invalid_txns, high_risk_txns)
    
    # Step 4: Setup Kafka outputs  
    print("\n📤 Step 4: Setting up Kafka outputs...")
    kafka_queries = setup_kafka_outputs(valid_txns, high_risk_txns)
    
    # Step 5: Setup console monitoring
    print("\n🖥️  Step 5: Setting up console monitoring...")
    console_queries = setup_console_outputs(valid_txns, invalid_txns)
    
    # Collect all queries
    all_queries = list(file_queries) + list(kafka_queries) + list(console_queries)
    
    print(f"\n✅ All {len(all_queries)} output streams started!")
    print("\n📊 What to observe:")
    print("• File outputs being written to /tmp/financial-outputs/")
    print("• Kafka topics receiving processed data")
    print("• Console showing real-time statistics")
    print("• Different output modes: append, update")
    print("• Checkpoint recovery capabilities")
    print("\n🛑 Press Ctrl+C to stop all streams")
    
    try:
        # Wait for all queries to finish
        for query in all_queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping all output streams...")
        for i, query in enumerate(all_queries):
            print(f"Stopping stream {i+1}/{len(all_queries)}...")
            query.stop()
        spark.stop()
        print("✅ All streams stopped successfully!")
        print("\n📁 Check output files:")
        print("ls -la /tmp/financial-outputs/*/")

if __name__ == "__main__":
    main()

# ============================================================================
# KEY LEARNING POINTS FROM STEP 1.4:
# 
# 1. MULTIPLE SINKS: Send same data to different destinations
# 2. FILE FORMATS: Parquet (efficient), JSON (readable), CSV (compatible)
# 3. KAFKA OUTPUTS: Real-time downstream processing
# 4. PARTITIONING: Organize data by business dimensions
# 5. CHECKPOINTS: Fault tolerance and exactly-once processing
# 6. OUTPUT MODES: append (new data), update (changed data)
# 7. MONITORING: Real-time visibility into stream health
# 8. ERROR HANDLING: Separate good data from bad data
# 
# NEXT STEP 1.5: We'll add basic aggregations and grouping operations
# ============================================================================