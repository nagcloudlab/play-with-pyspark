# 2_4_watermarking.py
# Step 2.4: Watermarking - Handling Late Data and State Management
# Purpose: Master watermarking to handle late-arriving data and manage streaming state

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# ============================================================================
# LEARNING OBJECTIVES FOR STEP 2.4:
# 1. Watermarking concept and necessity
# 2. Late data handling strategies
# 3. State store management and cleanup
# 4. Watermark thresholds and business trade-offs
# 5. Impact on aggregation results
# 6. Memory management in streaming
# 7. Production watermarking strategies
# ============================================================================

def create_spark_session():
    """Create Spark session optimized for watermarking operations"""
    return SparkSession.builder \
        .appName("WatermarkingAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s") \
        .getOrCreate()

spark = create_spark_session()
spark.sparkContext.setLogLevel("WARN")

# Create checkpoint directories
os.makedirs("/tmp/spark-checkpoints", exist_ok=True)

# ============================================================================
# TRANSACTION SCHEMA
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
# BASE TRANSACTION STREAM WITH LATE DATA SIMULATION
# ============================================================================
def get_transaction_stream_with_late_data():
    """Get transaction stream with late data analysis"""
    
    print("📖 Setting up transaction stream with late data analysis...")
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse and enrich with time analysis
    transactions = kafka_df \
        .select(
            col("key").cast("string").alias("partition_key"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), transaction_schema).alias("transaction"),
            current_timestamp().alias("processing_timestamp")
        ) \
        .select(
            col("partition_key"), 
            col("kafka_timestamp"),
            col("processing_timestamp"),
            col("transaction.*"),
            to_timestamp(col("transaction.event_time")).alias("event_timestamp")
        ) \
        .filter(col("event_timestamp").isNotNull() & (col("amount") > 0)) \
        .withColumn("amount_usd",
                   when(col("currency") == "USD", col("amount"))
                   .when(col("currency") == "EUR", col("amount") * 1.10)
                   .when(col("currency") == "GBP", col("amount") * 1.25)
                   .otherwise(col("amount"))) \
        .withColumn("delay_seconds",
                   # Calculate how late the data is
                   unix_timestamp(col("processing_timestamp")) - unix_timestamp(col("event_timestamp"))) \
        .withColumn("is_late_data",
                   # Flag data arriving more than 60 seconds late
                   col("delay_seconds") > 60) \
        .withColumn("is_very_late_data",
                   # Flag data arriving more than 5 minutes late
                   col("delay_seconds") > 300) \
        .withColumn("delay_category",
                   when(col("delay_seconds") <= 5, "IMMEDIATE")
                   .when(col("delay_seconds") <= 30, "QUICK")
                   .when(col("delay_seconds") <= 60, "NORMAL")
                   .when(col("delay_seconds") <= 300, "LATE")
                   .otherwise("VERY_LATE"))
    
    print("✅ Transaction stream with late data analysis configured!")
    return transactions

# ============================================================================
# WATERMARKING EXAMPLE 1: NO WATERMARK (DANGEROUS)
# Purpose: Show what happens without watermarking
# ============================================================================
def setup_no_watermark_example(transactions):
    """Show aggregation without watermarking - memory will grow indefinitely"""
    
    print("⚠️  Setting up NO WATERMARK example (dangerous)...")
    
    # Aggregation WITHOUT watermark - state grows forever!
    no_watermark_agg = transactions \
        .groupBy(
            window(col("event_timestamp"), "2 minutes"),
            col("transaction_type")
        ) \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount_usd").alias("total_volume"),
            avg("delay_seconds").alias("avg_delay_seconds"),
            sum(when(col("is_late_data"), 1).otherwise(0)).alias("late_data_count"),
            max("delay_seconds").alias("max_delay_seconds")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("transaction_type"),
            col("transaction_count"),
            round(col("total_volume"), 2).alias("total_volume"),
            round(col("avg_delay_seconds"), 1).alias("avg_delay_seconds"),
            col("late_data_count"),
            col("max_delay_seconds"),
            lit("NO_WATERMARK").alias("watermark_strategy")
        ) \
        .orderBy(col("window_start").desc(), col("total_volume").desc())
    
    # Output no watermark aggregation
    no_watermark_query = no_watermark_agg \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "8") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/no-watermark") \
        .trigger(processingTime='30 seconds') \
        .start()
    
    print("⚠️  NO WATERMARK aggregation started - MEMORY WILL GROW!")
    return no_watermark_query

# ============================================================================
# WATERMARKING EXAMPLE 2: STRICT WATERMARK (10 SECONDS)
# Purpose: Very strict watermark for low-latency applications
# ============================================================================
def setup_strict_watermark_example(transactions):
    """Show aggregation with strict 10-second watermark"""
    
    print("🔒 Setting up STRICT WATERMARK (10 seconds) example...")
    
    # Strict watermark - only wait 10 seconds for late data
    strict_watermark_agg = transactions \
        .withWatermark("event_timestamp", "10 seconds") \
        .groupBy(
            window(col("event_timestamp"), "2 minutes"),
            col("transaction_type")
        ) \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount_usd").alias("total_volume"),
            avg("delay_seconds").alias("avg_delay_seconds"),
            sum(when(col("is_late_data"), 1).otherwise(0)).alias("late_data_count"),
            sum(when(col("is_very_late_data"), 1).otherwise(0)).alias("very_late_data_count"),
            max("delay_seconds").alias("max_delay_seconds"),
            min("event_timestamp").alias("earliest_event"),
            max("event_timestamp").alias("latest_event")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("transaction_type"),
            col("transaction_count"),
            round(col("total_volume"), 2).alias("total_volume"),
            round(col("avg_delay_seconds"), 1).alias("avg_delay_seconds"),
            col("late_data_count"),
            col("very_late_data_count"),
            col("max_delay_seconds"),
            lit("STRICT_10s").alias("watermark_strategy")
        ) \
        .orderBy(col("window_start").desc(), col("total_volume").desc())
    
    # Output strict watermark aggregation
    strict_watermark_query = strict_watermark_agg \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "8") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/strict-watermark") \
        .trigger(processingTime='20 seconds') \
        .start()
    
    print("✅ STRICT WATERMARK aggregation started!")
    return strict_watermark_query

# ============================================================================
# WATERMARKING EXAMPLE 3: BALANCED WATERMARK (5 MINUTES)
# Purpose: Balanced approach for most business applications
# ============================================================================
def setup_balanced_watermark_example(transactions):
    """Show aggregation with balanced 5-minute watermark"""
    
    print("⚖️  Setting up BALANCED WATERMARK (5 minutes) example...")
    
    # Balanced watermark - wait 5 minutes for late data
    balanced_watermark_agg = transactions \
        .withWatermark("event_timestamp", "5 minutes") \
        .groupBy(
            window(col("event_timestamp"), "2 minutes"),
            col("location.country"),
            col("channel")
        ) \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount_usd").alias("total_volume"),
            avg("amount_usd").alias("avg_amount"),
            countDistinct("account_id").alias("unique_customers"),
            
            # Late data analysis
            sum(when(col("is_late_data"), 1).otherwise(0)).alias("late_data_count"),
            sum(when(col("is_very_late_data"), 1).otherwise(0)).alias("very_late_data_count"),
            avg("delay_seconds").alias("avg_delay_seconds"),
            
            # Completeness indicators
            min("event_timestamp").alias("window_first_event"),
            max("event_timestamp").alias("window_last_event"),
            count("*").alias("total_events_processed")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            concat(
                date_format(col("window.start"), "HH:mm:ss"),
                lit(" - "),
                date_format(col("window.end"), "HH:mm:ss")
            ).alias("time_range"),
            col("country"),
            col("channel"),
            
            # Core metrics
            col("transaction_count"),
            round(col("total_volume"), 2).alias("total_volume"),
            round(col("avg_amount"), 2).alias("avg_amount"),
            col("unique_customers"),
            
            # Late data metrics
            col("late_data_count"),
            col("very_late_data_count"),
            round(col("avg_delay_seconds"), 1).alias("avg_delay_seconds"),
            
            # Data completeness percentage
            round((col("late_data_count") / col("transaction_count") * 100), 1).alias("late_data_pct"),
            
            lit("BALANCED_5min").alias("watermark_strategy")
        ) \
        .orderBy(col("window_start").desc(), col("total_volume").desc())
    
    # Output balanced watermark aggregation
    balanced_watermark_query = balanced_watermark_agg \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "10") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/balanced-watermark") \
        .trigger(processingTime='25 seconds') \
        .start()
    
    print("✅ BALANCED WATERMARK aggregation started!")
    return balanced_watermark_query

# ============================================================================
# WATERMARKING EXAMPLE 4: RELAXED WATERMARK (1 HOUR)
# Purpose: Very relaxed for batch-like processing with high completeness
# ============================================================================
def setup_relaxed_watermark_example(transactions):
    """Show aggregation with relaxed 1-hour watermark"""
    
    print("🌊 Setting up RELAXED WATERMARK (1 hour) example...")
    
    # Relaxed watermark - wait 1 hour for late data (very complete results)
    relaxed_watermark_agg = transactions \
        .withWatermark("event_timestamp", "1 hour") \
        .groupBy(
            window(col("event_timestamp"), "10 minutes")
        ) \
        .agg(
            count("*").alias("total_transactions"),
            sum("amount_usd").alias("total_volume"),
            countDistinct("account_id").alias("unique_customers"),
            countDistinct("location.country").alias("countries_served"),
            
            # Delay analysis
            sum(when(col("delay_category") == "IMMEDIATE", 1).otherwise(0)).alias("immediate_count"),
            sum(when(col("delay_category") == "QUICK", 1).otherwise(0)).alias("quick_count"),
            sum(when(col("delay_category") == "NORMAL", 1).otherwise(0)).alias("normal_count"),
            sum(when(col("delay_category") == "LATE", 1).otherwise(0)).alias("late_count"),
            sum(when(col("delay_category") == "VERY_LATE", 1).otherwise(0)).alias("very_late_count"),
            
            # Statistical analysis
            avg("delay_seconds").alias("avg_delay"),
            expr("percentile_approx(delay_seconds, 0.5)").alias("median_delay"),
            expr("percentile_approx(delay_seconds, 0.95)").alias("p95_delay"),
            expr("percentile_approx(delay_seconds, 0.99)").alias("p99_delay"),
            
            # Completeness tracking
            min("processing_timestamp").alias("first_processed"),
            max("processing_timestamp").alias("last_processed")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            date_format(col("window.start"), "HH:mm").alias("start_time"),
            
            # Core metrics
            col("total_transactions"),
            round(col("total_volume"), 2).alias("total_volume"),
            col("unique_customers"),
            col("countries_served"),
            
            # Delay distribution percentages
            round((col("immediate_count") / col("total_transactions") * 100), 1).alias("immediate_pct"),
            round((col("quick_count") / col("total_transactions") * 100), 1).alias("quick_pct"),
            round((col("normal_count") / col("total_transactions") * 100), 1).alias("normal_pct"),
            round((col("late_count") / col("total_transactions") * 100), 1).alias("late_pct"),
            round((col("very_late_count") / col("total_transactions") * 100), 1).alias("very_late_pct"),
            
            # Delay statistics
            round(col("avg_delay"), 1).alias("avg_delay_sec"),
            round(col("median_delay"), 1).alias("median_delay_sec"),
            round(col("p95_delay"), 1).alias("p95_delay_sec"),
            
            lit("RELAXED_1hr").alias("watermark_strategy")
        ) \
        .orderBy(col("window_start").desc())
    
    # Output relaxed watermark aggregation
    relaxed_watermark_query = relaxed_watermark_agg \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "6") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/relaxed-watermark") \
        .trigger(processingTime='45 seconds') \
        .start()
    
    print("✅ RELAXED WATERMARK aggregation started!")
    return relaxed_watermark_query

# ============================================================================
# WATERMARK MONITORING AND COMPARISON
# Purpose: Monitor watermark effectiveness and compare strategies
# ============================================================================
def setup_watermark_monitoring(transactions):
    """Setup monitoring to compare watermark strategies"""
    
    print("📊 Setting up watermark monitoring and comparison...")
    
    # Monitor late data patterns to inform watermark decisions
    late_data_monitoring = transactions \
        .groupBy(
            window(col("processing_timestamp"), "1 minute"),  # Processing time window
            col("delay_category")
        ) \
        .agg(
            count("*").alias("event_count"),
            avg("delay_seconds").alias("avg_delay"),
            min("delay_seconds").alias("min_delay"),
            max("delay_seconds").alias("max_delay"),
            sum("amount_usd").alias("total_volume"),
            
            # Business impact analysis
            sum(when(col("amount_usd") >= 1000, 1).otherwise(0)).alias("high_value_late_count"),
            sum(when(col("amount_usd") >= 1000, col("amount_usd")).otherwise(0)).alias("high_value_late_volume")
        ) \
        .select(
            col("window.start").alias("processing_window_start"),
            col("delay_category"),
            col("event_count"),
            round(col("avg_delay"), 1).alias("avg_delay_seconds"),
            col("min_delay"),
            col("max_delay"),
            round(col("total_volume"), 2).alias("total_volume"),
            col("high_value_late_count"),
            round(col("high_value_late_volume"), 2).alias("high_value_late_volume"),
            
            # Impact assessment
            round((col("high_value_late_volume") / col("total_volume") * 100), 1).alias("high_value_impact_pct")
        ) \
        .orderBy(col("processing_window_start").desc(), col("delay_category"))
    
    # Output late data monitoring
    monitoring_query = late_data_monitoring \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "12") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/watermark-monitoring") \
        .trigger(processingTime='60 seconds') \
        .start()
    
    print("✅ Watermark monitoring started!")
    return monitoring_query

# ============================================================================
# MAIN WATERMARKING PIPELINE
# ============================================================================
def main():
    """Execute the complete watermarking analysis pipeline"""
    
    print("🚀 Starting Step 2.4: Watermarking Pipeline")
    print("=" * 60)
    
    # Step 1: Get transaction stream with late data analysis
    print("\n📖 Step 1: Setting up transaction stream with late data analysis...")
    transactions = get_transaction_stream_with_late_data()
    
    # Step 2: No watermark example (dangerous!)
    print("\n⚠️  Step 2: Setting up NO watermark example...")
    no_watermark_query = setup_no_watermark_example(transactions)
    
    # Step 3: Strict watermark (10 seconds)
    print("\n🔒 Step 3: Setting up STRICT watermark...")
    strict_query = setup_strict_watermark_example(transactions)
    
    # Step 4: Balanced watermark (5 minutes)
    print("\n⚖️  Step 4: Setting up BALANCED watermark...")
    balanced_query = setup_balanced_watermark_example(transactions)
    
    # Step 5: Relaxed watermark (1 hour)
    print("\n🌊 Step 5: Setting up RELAXED watermark...")
    relaxed_query = setup_relaxed_watermark_example(transactions)
    
    # Step 6: Watermark monitoring
    print("\n📊 Step 6: Setting up watermark monitoring...")
    monitoring_query = setup_watermark_monitoring(transactions)
    
    # Collect all queries
    all_queries = [no_watermark_query, strict_query, balanced_query, relaxed_query, monitoring_query]
    
    print(f"\n✅ All {len(all_queries)} watermarking streams started!")
    print("\n📊 What to observe:")
    print("• NO WATERMARK: Memory grows indefinitely, all late data accepted")
    print("• STRICT (10s): Fast processing, may miss late data")
    print("• BALANCED (5min): Good trade-off for most business cases")
    print("• RELAXED (1hr): High completeness, higher latency")
    print("• MONITORING: Late data patterns and business impact")
    print("• Compare transaction counts across strategies")
    print("• Notice state management differences")
    print("\n🎯 Key Learning:")
    print("• withWatermark(timestamp_column, threshold)")
    print("• Watermark = how long to wait for late data")
    print("• Lower threshold = faster processing, less complete")
    print("• Higher threshold = more complete, higher latency")
    print("• State store cleanup depends on watermark")
    print("• Business requirements drive watermark choice")
    print("\n⚖️  Trade-offs:")
    print("• Latency vs Completeness")
    print("• Memory usage vs Data accuracy")
    print("• Real-time insights vs Historical correctness")
    print("\n🛑 Press Ctrl+C to stop all streams")
    
    try:
        # Wait for all queries to finish
        for query in all_queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping all watermarking streams...")
        for i, query in enumerate(all_queries):
            print(f"Stopping watermarking stream {i+1}/{len(all_queries)}...")
            query.stop()
        spark.stop()
        print("✅ All watermarking streams stopped successfully!")
        print("\n🎯 Next: Phase 3 - Advanced Aggregations!")

if __name__ == "__main__":
    main()

# ============================================================================
# KEY LEARNING POINTS FROM STEP 2.4:
# 
# 1. WATERMARKING: Controls how long to wait for late-arriving data
# 2. STATE MANAGEMENT: Watermarks enable state store cleanup
# 3. TRADE-OFFS: Latency vs Completeness vs Memory usage
# 4. BUSINESS IMPACT: Choose watermark based on business requirements
# 5. NO WATERMARK: Memory grows forever (dangerous in production)
# 6. STRICT WATERMARK: Fast processing, may miss late data
# 7. RELAXED WATERMARK: High completeness, higher latency
# 8. MONITORING: Track late data patterns to optimize watermarks
# 
# WATERMARK STRATEGIES:
# • Real-time dashboards: 30 seconds - 2 minutes
# • Fraud detection: 5 - 15 minutes
# • Business reporting: 1 - 6 hours
# • Compliance/regulatory: 24 - 48 hours
# 
# Phase 2 Complete! Next: Phase 3 - Advanced Aggregations
# ============================================================================