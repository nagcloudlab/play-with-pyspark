# 1_5_first_aggregations_fixed.py
# Step 1.5: First Aggregations - Complete Fixed Version
# Purpose: Basic aggregations with streaming-compatible functions

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("FinancialAggregations") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
        .getOrCreate()

spark = create_spark_session()
spark.sparkContext.setLogLevel("WARN")
os.makedirs("/tmp/spark-checkpoints", exist_ok=True)

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("location", StructType([
        StructField("country", StringType(), True),
        StructField("city", StringType(), True)
    ]), True),
    StructField("channel", StringType(), True),
    StructField("status", StringType(), True)
])

def get_enriched_transaction_stream():
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
            from_json(col("value").cast("string"), transaction_schema).alias("transaction")
        ) \
        .select(
            col("partition_key"), col("kafka_timestamp"),
            col("transaction.*"),
            to_timestamp(col("transaction.event_time")).alias("event_timestamp")
        ) \
        .filter(col("amount") > 0) \
        .withColumn("amount_usd",
                   when(col("currency") == "USD", col("amount"))
                   .when(col("currency") == "EUR", col("amount") * 1.10)
                   .when(col("currency") == "GBP", col("amount") * 1.25)
                   .otherwise(col("amount"))) \
        .withColumn("transaction_hour", hour(col("event_timestamp"))) \
        .withColumn("is_weekend", dayofweek(col("event_timestamp")).isin([1, 7])) \
        .withColumn("risk_score",
                   when(col("amount_usd") >= 5000, 25).otherwise(0) +
                   when(col("location.country") != "US", 20).otherwise(0))

def setup_basic_aggregations(transactions):
    print("🔢 Setting up basic aggregations...")
    
    # Basic aggregations with streaming-compatible functions
    basic_aggregations = transactions \
        .groupBy(window(col("event_timestamp"), "1 minute")) \
        .agg(
            count("*").alias("total_transactions"),
            sum("amount_usd").alias("total_volume_usd"),
            avg("amount_usd").alias("avg_amount_usd"),
            min("amount_usd").alias("min_amount_usd"),
            max("amount_usd").alias("max_amount_usd"),
            approx_count_distinct("account_id").alias("unique_accounts")  # Streaming-compatible
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_transactions"),
            round(col("total_volume_usd"), 2).alias("total_volume_usd"),
            round(col("avg_amount_usd"), 2).alias("avg_amount_usd"),
            round(col("min_amount_usd"), 2).alias("min_amount_usd"),
            round(col("max_amount_usd"), 2).alias("max_amount_usd"),
            col("unique_accounts")
        )
    
    query = basic_aggregations \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "10") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/basic-agg") \
        .trigger(processingTime='15 seconds') \
        .start()
    
    print("✅ Basic aggregations started!")
    return query

def setup_grouped_aggregations(transactions):
    print("📊 Setting up grouped aggregations...")
    
    grouped_aggregations = transactions \
        .groupBy(
            window(col("event_timestamp"), "2 minutes"),
            col("transaction_type"),
            col("location.country")
        ) \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount_usd").alias("total_amount"),
            avg("amount_usd").alias("avg_amount"),
            approx_count_distinct("account_id").alias("unique_customers")  # Streaming-compatible
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("transaction_type"),
            col("country"),
            col("transaction_count"),
            round(col("total_amount"), 2).alias("total_amount"),
            round(col("avg_amount"), 2).alias("avg_amount"),
            col("unique_customers")
        ) \
        .orderBy(col("window_start").desc(), col("total_amount").desc())
    
    query = grouped_aggregations \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "8") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/grouped-agg") \
        .trigger(processingTime='20 seconds') \
        .start()
    
    print("✅ Grouped aggregations started!")
    return query

def setup_windowed_aggregations(transactions):
    print("⏰ Setting up windowed aggregations...")
    
    # Different window sizes for comparison
    windowed_aggs = transactions \
        .groupBy(
            window(col("event_timestamp"), "30 seconds"),
            col("channel")
        ) \
        .agg(
            count("*").alias("txn_count"),
            sum("amount_usd").alias("volume"),
            avg("risk_score").alias("avg_risk")
        ) \
        .select(
            col("window.start").alias("window_start"),
            date_format(col("window.start"), "HH:mm:ss").alias("start_time"),
            col("channel"),
            col("txn_count"),
            round(col("volume"), 2).alias("volume"),
            round(col("avg_risk"), 1).alias("avg_risk")
        ) \
        .orderBy(col("window_start").desc(), col("volume").desc())
    
    query = windowed_aggs \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "12") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/windowed-agg") \
        .trigger(processingTime='15 seconds') \
        .start()
    
    print("✅ Windowed aggregations started!")
    return query

def setup_running_totals(transactions):
    print("🏃 Setting up running totals...")
    
    # Running totals by transaction type (no time window)
    running_totals = transactions \
        .groupBy("transaction_type") \
        .agg(
            count("*").alias("total_count"),
            sum("amount_usd").alias("total_volume"),
            avg("amount_usd").alias("avg_amount"),
            approx_count_distinct("account_id").alias("unique_customers")  # Streaming-compatible
        ) \
        .select(
            col("transaction_type"),
            col("total_count"),
            round(col("total_volume"), 2).alias("total_volume"),
            round(col("avg_amount"), 2).alias("avg_amount"),
            col("unique_customers")
        ) \
        .orderBy(col("total_volume").desc())
    
    query = running_totals \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "10") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/running-totals") \
        .trigger(processingTime='30 seconds') \
        .start()
    
    print("✅ Running totals started!")
    return query

def main():
    print("🚀 Starting Step 1.5: First Aggregations (Complete Fixed)")
    print("=" * 55)
    
    # Get enriched transaction stream
    print("\n📖 Step 1: Getting enriched transaction stream...")
    transactions = get_enriched_transaction_stream()
    
    # Setup different aggregation patterns
    print("\n🔢 Step 2: Setting up basic aggregations...")
    basic_query = setup_basic_aggregations(transactions)
    
    print("\n📊 Step 3: Setting up grouped aggregations...")
    grouped_query = setup_grouped_aggregations(transactions)
    
    print("\n⏰ Step 4: Setting up windowed aggregations...")
    windowed_query = setup_windowed_aggregations(transactions)
    
    print("\n🏃 Step 5: Setting up running totals...")
    running_query = setup_running_totals(transactions)
    
    all_queries = [basic_query, grouped_query, windowed_query, running_query]
    
    print(f"\n✅ All {len(all_queries)} aggregation streams started!")
    print("\n📊 What to observe:")
    print("• Basic aggregations: Count, sum, avg, min, max per minute")
    print("• Grouped aggregations: By transaction type and country")
    print("• Windowed aggregations: 30-second windows by channel")
    print("• Running totals: Cumulative since stream start (complete mode)")
    print("\n🎯 Key Learning:")
    print("• approx_count_distinct() for streaming (not countDistinct)")
    print("• Different window sizes for different analysis needs")
    print("• Update vs Complete output modes")
    print("• State management in aggregations")
    print("\n💡 Streaming Constraints:")
    print("• Use approx_count_distinct() instead of countDistinct()")
    print("• Exact distinct counting not supported in streaming")
    print("• HyperLogLog algorithm provides ~1-2% accuracy")
    print("\n🛑 Press Ctrl+C to stop")
    
    try:
        for query in all_queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping streams...")
        for query in all_queries:
            query.stop()
        spark.stop()
        print("✅ Stopped! Phase 1 Complete!")
        print("\n🎉 You've mastered:")
        print("✅ Basic stream operations")
        print("✅ Data transformations")
        print("✅ Multiple outputs")
        print("✅ Aggregations and grouping")

if __name__ == "__main__":
    main()
