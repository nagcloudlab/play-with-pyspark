# Basic Financial Transaction Stream Reader
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Create Spark session with Kafka support
def create_spark_session():
    return SparkSession.builder \
        .appName("FinancialStreamProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

# Initialize Spark
spark = create_spark_session()
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark session created successfully!")
print(f"Spark version: {spark.version}")

# Define schema for financial transactions
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

print("✅ Transaction schema defined!")

# Read from Kafka
def read_transaction_stream():
    """Read raw transaction stream from Kafka"""
    
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
        
        
    
    print("✅ Kafka stream created!")
    print("Kafka DataFrame Schema:")
    kafka_df.printSchema()
    
    return kafka_df

# Parse JSON data from Kafka
def parse_transactions(kafka_df):
    """Parse JSON transaction data from Kafka messages"""
    
    parsed_df = kafka_df \
        .select(
            # Kafka metadata
            col("key").cast("string").alias("partition_key"),
            col("topic"),
            col("partition"),
            col("offset"), 
            col("timestamp").alias("kafka_timestamp"),
            
            # Parse JSON payload
            col("value").cast("string").alias("json_data")
        ) \
        .select(
            col("partition_key"),
            col("kafka_timestamp"),
            col("topic"),
            col("partition"),
            col("offset"),
            
            # Extract transaction data from JSON
            from_json(col("json_data"), transaction_schema).alias("transaction")
        ) \
        .select(
            # Kafka metadata
            col("partition_key"),
            col("kafka_timestamp"),
            col("topic"),
            col("partition"),
            col("offset"),
            
            # Transaction fields (flattened)
            col("transaction.*"),
            
            # Convert event_time string to timestamp
            to_timestamp(col("transaction.event_time")).alias("event_timestamp")
        )
    
    print("✅ Transaction parsing configured!")
    print("Parsed DataFrame Schema:")
    parsed_df.printSchema()
    
    return parsed_df

# Create the streaming pipeline
print("\n🚀 Setting up streaming pipeline...")

# Step 1: Read from Kafka
raw_stream = read_transaction_stream()

# Step 2: Parse transactions
transactions = parse_transactions(raw_stream)

# Step 3: Display parsed transactions
print("\n📊 Starting transaction stream display...")
print("This will show incoming transactions in real-time...")

# Write to console for debugging
query = transactions \
    .select(
        "transaction_id",
        "account_id", 
        "transaction_type",
        "amount",
        "currency",
        "merchant",
        "event_timestamp",
        "location.country",
        "channel",
        "status"
    ) \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", "10") \
    .trigger(processingTime='5 seconds') \
    .start()

print("✅ Stream started! Waiting for data...")
print("💡 Run the transaction producer in another terminal to see data flowing...")
print("🛑 Press Ctrl+C to stop the stream")

# Keep the stream running
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n🛑 Stopping stream...")
    query.stop()
    spark.stop()
    print("✅ Stream stopped successfully!")