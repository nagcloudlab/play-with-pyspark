# 2_3_sliding_windows.py
# Step 2.3: Sliding Windows - Overlapping Time Interval Analysis
# Purpose: Master sliding windows for smooth continuous analysis and trend detection

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# ============================================================================
# LEARNING OBJECTIVES FOR STEP 2.3:
# 1. Sliding window concept vs tumbling windows
# 2. Window size vs slide interval configuration
# 3. Overlapping time interval analysis
# 4. Smooth trend detection and moving averages
# 5. Real-time anomaly detection patterns
# 6. Performance implications of sliding windows
# 7. Business use cases for continuous monitoring
# ============================================================================

def create_spark_session():
    """Create Spark session optimized for sliding window operations"""
    return SparkSession.builder \
        .appName("SlidingWindowAnalysis") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
        .config("spark.sql.session.timeZone", "UTC") \
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
# BASE TRANSACTION STREAM SETUP
# ============================================================================
def get_enriched_transaction_stream():
    """Get enriched transaction stream for sliding window analysis"""
    
    print("📖 Setting up enriched transaction stream...")
    
    # Read from Kafka and enrich
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse and enrich transactions
    transactions = kafka_df \
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
        .filter(col("event_timestamp").isNotNull() & (col("amount") > 0)) \
        .withColumn("amount_usd",
                   when(col("currency") == "USD", col("amount"))
                   .when(col("currency") == "EUR", col("amount") * 1.10)
                   .when(col("currency") == "GBP", col("amount") * 1.25)
                   .when(col("currency") == "CAD", col("amount") * 0.75)
                   .otherwise(col("amount"))) \
        .withColumn("transaction_fee",
                   when(col("transaction_type") == "transfer", 5.00)
                   .when(col("transaction_type") == "withdrawal", 2.00)
                   .otherwise(0.00)) \
        .withColumn("is_high_value", col("amount_usd") >= 1000) \
        .withColumn("risk_score",
                   when(col("is_high_value"), 25).otherwise(0) +
                   when(col("location.country") != "US", 20).otherwise(0) +
                   when(col("channel") == "online", 10).otherwise(0))
    
    print("✅ Enriched transaction stream configured!")
    return transactions

# ============================================================================
# SLIDING WINDOW 1: MOVING AVERAGES
# Purpose: Smooth trend analysis with overlapping windows
# ============================================================================
def setup_moving_averages(transactions):
    """Setup sliding windows for moving averages and trend analysis"""
    
    print("📈 Setting up moving averages with sliding windows...")
    
    # 5-minute windows sliding every 1 minute = moving average
    moving_averages = transactions \
        .groupBy(
            window(col("event_timestamp"), "5 minutes", "1 minute"),  # 5min window, 1min slide
            col("transaction_type")
        ) \
        .agg(
            # Moving average metrics
            count("*").alias("transaction_count"),
            avg("amount_usd").alias("moving_avg_amount"),
            sum("amount_usd").alias("total_volume"),
            min("amount_usd").alias("min_amount"),
            max("amount_usd").alias("max_amount"),
            stddev("amount_usd").alias("amount_stddev"),
            
            # Moving counts
            countDistinct("account_id").alias("unique_customers"),
            avg("risk_score").alias("avg_risk_score"),
            
            # Trend indicators
            first(col("event_timestamp")).alias("first_event"),
            last(col("event_timestamp")).alias("last_event")
        ) \
        .select(
            # Window information
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            date_format(col("window.start"), "HH:mm:ss").alias("start_time"),
            date_format(col("window.end"), "HH:mm:ss").alias("end_time"),
            
            # Transaction type
            col("transaction_type"),
            
            # Moving metrics
            col("transaction_count"),
            round(col("moving_avg_amount"), 2).alias("moving_avg_amount"),
            round(col("total_volume"), 2).alias("total_volume"),
            round(col("amount_stddev"), 2).alias("amount_stddev"),
            col("unique_customers"),
            round(col("avg_risk_score"), 1).alias("avg_risk_score"),
            
            # Volatility indicator
            round((col("amount_stddev") / col("moving_avg_amount") * 100), 1).alias("volatility_pct")
        ) \
        .orderBy(col("window_start").desc(), col("total_volume").desc())
    
    # Output moving averages
    moving_avg_query = moving_averages \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "12") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/moving-averages") \
        .trigger(processingTime='30 seconds') \
        .start()
    
    print("✅ Moving averages started!")
    return moving_avg_query

# ============================================================================
# SLIDING WINDOW 2: REAL-TIME FRAUD DETECTION
# Purpose: Continuous monitoring for suspicious patterns
# ============================================================================
def setup_fraud_detection_sliding_windows(transactions):
    """Setup sliding windows for real-time fraud detection"""
    
    print("🚨 Setting up fraud detection with sliding windows...")
    
    # 10-minute windows sliding every 30 seconds for fraud detection
    fraud_detection = transactions \
        .groupBy(
            window(col("event_timestamp"), "10 minutes", "30 seconds"),  # 10min window, 30sec slide
            col("account_id")
        ) \
        .agg(
            # Transaction velocity (frequency-based fraud)
            count("*").alias("transaction_count_10min"),
            sum("amount_usd").alias("total_amount_10min"),
            countDistinct("location.country").alias("country_count"),
            countDistinct("location.city").alias("city_count"),
            countDistinct("merchant").alias("merchant_count"),
            countDistinct("channel").alias("channel_count"),
            
            # Risk indicators
            max("amount_usd").alias("max_amount_10min"),
            avg("risk_score").alias("avg_risk_score"),
            sum(when(col("is_high_value"), 1).otherwise(0)).alias("high_value_count"),
            
            # Time patterns
            min("event_timestamp").alias("first_transaction"),
            max("event_timestamp").alias("last_transaction"),
            
            # Geographic spread
            collect_set("location.country").alias("countries_list"),
            collect_set("channel").alias("channels_list")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("account_id"),
            
            # Velocity metrics
            col("transaction_count_10min"),
            round(col("total_amount_10min"), 2).alias("total_amount_10min"),
            round((col("total_amount_10min") / col("transaction_count_10min")), 2).alias("avg_amount_per_txn"),
            
            # Diversity indicators (fraud often shows low diversity)
            col("country_count"),
            col("city_count"),
            col("merchant_count"),
            col("channel_count"),
            
            # Risk flags
            col("high_value_count"),
            round(col("avg_risk_score"), 1).alias("avg_risk_score"),
            round(col("max_amount_10min"), 2).alias("max_amount_10min"),
            
            # Fraud scoring
            (when(col("transaction_count_10min") >= 10, 30).otherwise(0) +  # High velocity
             when(col("country_count") >= 3, 25).otherwise(0) +             # Geographic spread
             when(col("total_amount_10min") >= 10000, 20).otherwise(0) +    # High volume
             when(col("high_value_count") >= 3, 15).otherwise(0) +          # Multiple high-value
             when(col("channel_count") >= 3, 10).otherwise(0)               # Channel diversity
            ).alias("fraud_score"),
            
            # Time span analysis
            round(
                (unix_timestamp(col("last_transaction")) - unix_timestamp(col("first_transaction"))) / 60, 1
            ).alias("activity_span_minutes")
        ) \
        .withColumn("fraud_level",
                   when(col("fraud_score") >= 80, "CRITICAL")
                   .when(col("fraud_score") >= 60, "HIGH")
                   .when(col("fraud_score") >= 40, "MEDIUM")
                   .when(col("fraud_score") >= 20, "LOW")
                   .otherwise("MINIMAL")) \
        .filter(col("fraud_score") >= 20) \
        .orderBy(col("window_start").desc(), col("fraud_score").desc())
    
    # Output fraud detection
    fraud_query = fraud_detection \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "8") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/fraud-detection") \
        .trigger(processingTime='15 seconds') \
        .start()
    
    print("✅ Fraud detection started!")
    return fraud_query

# ============================================================================
# SLIDING WINDOW 3: VELOCITY MONITORING
# Purpose: Monitor transaction velocity and rate changes
# ============================================================================
def setup_velocity_monitoring(transactions):
    """Setup sliding windows for transaction velocity monitoring"""
    
    print("⚡ Setting up velocity monitoring with sliding windows...")
    
    # 3-minute windows sliding every 30 seconds for velocity monitoring
    velocity_monitoring = transactions \
        .groupBy(
            window(col("event_timestamp"), "3 minutes", "30 seconds"),  # 3min window, 30sec slide
            col("location.country"),
            col("channel")
        ) \
        .agg(
            # Velocity metrics
            count("*").alias("txn_count_3min"),
            sum("amount_usd").alias("volume_3min"),
            countDistinct("account_id").alias("unique_accounts_3min"),
            
            # Rate calculations (per minute)
            (count("*") / 3.0).alias("txn_rate_per_minute"),
            (sum("amount_usd") / 3.0).alias("volume_rate_per_minute"),
            (countDistinct("account_id") / 3.0).alias("customer_rate_per_minute"),
            
            # Peak detection
            max("amount_usd").alias("peak_amount"),
            min("amount_usd").alias("min_amount"),
            
            # Time distribution
            collect_list(minute(col("event_timestamp"))).alias("minute_distribution")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            concat(
                date_format(col("window.start"), "HH:mm"), 
                lit("-"), 
                date_format(col("window.end"), "HH:mm")
            ).alias("time_range"),
            
            col("country"),
            col("channel"),
            
            # Velocity metrics
            col("txn_count_3min"),
            round(col("volume_3min"), 2).alias("volume_3min"),
            col("unique_accounts_3min"),
            
            # Rates (per minute)
            round(col("txn_rate_per_minute"), 1).alias("txn_per_minute"),
            round(col("volume_rate_per_minute"), 2).alias("volume_per_minute"),
            round(col("customer_rate_per_minute"), 1).alias("customers_per_minute"),
            
            # Peak analysis
            round(col("peak_amount"), 2).alias("peak_amount"),
            round((col("peak_amount") / (col("volume_3min") / col("txn_count_3min"))), 1).alias("peak_to_avg_ratio"),
            
            # Velocity indicators
            when(col("txn_rate_per_minute") >= 50, "HIGH")
            .when(col("txn_rate_per_minute") >= 20, "MEDIUM")
            .when(col("txn_rate_per_minute") >= 5, "NORMAL")
            .otherwise("LOW").alias("velocity_level")
        ) \
        .orderBy(col("window_start").desc(), col("txn_per_minute").desc())
    
    # Output velocity monitoring
    velocity_query = velocity_monitoring \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "10") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/velocity-monitoring") \
        .trigger(processingTime='20 seconds') \
        .start()
    
    print("✅ Velocity monitoring started!")
    return velocity_query

# ============================================================================
# SLIDING WINDOW 4: TREND ANALYSIS
# Purpose: Detect trends and patterns using overlapping windows
# ============================================================================
def setup_trend_analysis(transactions):
    """Setup sliding windows for trend analysis and pattern detection"""
    
    print("📊 Setting up trend analysis with sliding windows...")
    
    # 15-minute windows sliding every 5 minutes for trend analysis
    trend_analysis = transactions \
        .groupBy(
            window(col("event_timestamp"), "15 minutes", "5 minutes")  # 15min window, 5min slide
        ) \
        .agg(
            # Volume trends
            count("*").alias("total_transactions"),
            sum("amount_usd").alias("total_volume"),
            avg("amount_usd").alias("avg_amount"),
            
            # Transaction type distribution
            sum(when(col("transaction_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            sum(when(col("transaction_type") == "withdrawal", 1).otherwise(0)).alias("withdrawal_count"),
            sum(when(col("transaction_type") == "transfer", 1).otherwise(0)).alias("transfer_count"),
            sum(when(col("transaction_type") == "deposit", 1).otherwise(0)).alias("deposit_count"),
            
            # Channel trends
            sum(when(col("channel") == "online", col("amount_usd")).otherwise(0)).alias("online_volume"),
            sum(when(col("channel") == "mobile", col("amount_usd")).otherwise(0)).alias("mobile_volume"),
            sum(when(col("channel") == "atm", col("amount_usd")).otherwise(0)).alias("atm_volume"),
            
            # Risk trends
            avg("risk_score").alias("avg_risk_score"),
            sum(when(col("is_high_value"), 1).otherwise(0)).alias("high_value_count"),
            
            # Geographic trends
            countDistinct("location.country").alias("countries_active"),
            countDistinct("account_id").alias("unique_customers")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            date_format(col("window.start"), "HH:mm").alias("start_time"),
            
            # Volume metrics
            col("total_transactions"),
            round(col("total_volume"), 2).alias("total_volume"),
            round(col("avg_amount"), 2).alias("avg_amount"),
            
            # Transaction type percentages
            round((col("purchase_count") / col("total_transactions") * 100), 1).alias("purchase_pct"),
            round((col("withdrawal_count") / col("total_transactions") * 100), 1).alias("withdrawal_pct"),
            round((col("transfer_count") / col("total_transactions") * 100), 1).alias("transfer_pct"),
            
            # Channel percentages
            round((col("online_volume") / col("total_volume") * 100), 1).alias("online_volume_pct"),
            round((col("mobile_volume") / col("total_volume") * 100), 1).alias("mobile_volume_pct"),
            round((col("atm_volume") / col("total_volume") * 100), 1).alias("atm_volume_pct"),
            
            # Risk metrics
            round(col("avg_risk_score"), 1).alias("avg_risk_score"),
            round((col("high_value_count") / col("total_transactions") * 100), 1).alias("high_value_pct"),
            
            # Activity metrics
            col("countries_active"),
            col("unique_customers"),
            round((col("total_transactions") / col("unique_customers")), 1).alias("txn_per_customer")
        ) \
        .orderBy(col("window_start").desc())
    
    # Output trend analysis
    trend_query = trend_analysis \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "6") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/trend-analysis") \
        .trigger(processingTime='45 seconds') \
        .start()
    
    print("✅ Trend analysis started!")
    return trend_query

# ============================================================================
# MAIN SLIDING WINDOWS PIPELINE
# ============================================================================
def main():
    """Execute the complete sliding windows analysis pipeline"""
    
    print("🚀 Starting Step 2.3: Sliding Windows Pipeline")
    print("=" * 60)
    
    # Step 1: Get enriched transaction stream
    print("\n📖 Step 1: Setting up enriched transaction stream...")
    transactions = get_enriched_transaction_stream()
    
    # Step 2: Moving averages
    print("\n📈 Step 2: Setting up moving averages...")
    moving_avg_query = setup_moving_averages(transactions)
    
    # Step 3: Fraud detection
    print("\n🚨 Step 3: Setting up fraud detection...")
    fraud_query = setup_fraud_detection_sliding_windows(transactions)
    
    # Step 4: Velocity monitoring
    print("\n⚡ Step 4: Setting up velocity monitoring...")
    velocity_query = setup_velocity_monitoring(transactions)
    
    # Step 5: Trend analysis
    print("\n📊 Step 5: Setting up trend analysis...")
    trend_query = setup_trend_analysis(transactions)
    
    # Collect all queries
    all_queries = [moving_avg_query, fraud_query, velocity_query, trend_query]
    
    print(f"\n✅ All {len(all_queries)} sliding window streams started!")
    print("\n📊 What to observe:")
    print("• Moving averages: 5-min windows sliding every 1-min")
    print("• Fraud detection: 10-min windows sliding every 30-sec")
    print("• Velocity monitoring: 3-min windows sliding every 30-sec")
    print("• Trend analysis: 15-min windows sliding every 5-min")
    print("• Overlapping windows creating smooth continuous analysis")
    print("• Same events appearing in multiple windows")
    print("• Fraud scores and velocity levels")
    print("\n🎯 Key Learning:")
    print("• window(timestamp, window_size, slide_interval)")
    print("• Slide interval < window size = overlapping windows")
    print("• Smaller slide = smoother trends, more computation")
    print("• Events appear in multiple overlapping windows")
    print("• Better for trend detection and anomaly monitoring")
    print("• Higher memory usage than tumbling windows")
    print("\n📈 Window Patterns:")
    print("• Tumbling: [0-5] [5-10] [10-15] ← No overlap")
    print("• Sliding:  [0-5]  [1-6]  [2-7] ← Overlap!")
    print("\n🛑 Press Ctrl+C to stop all streams")
    
    try:
        # Wait for all queries to finish
        for query in all_queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping all sliding window streams...")
        for i, query in enumerate(all_queries):
            print(f"Stopping sliding window stream {i+1}/{len(all_queries)}...")
            query.stop()
        spark.stop()
        print("✅ All sliding window streams stopped successfully!")
        print("\n🎯 Next: Step 2.4 - Watermarking (handling late data)")

if __name__ == "__main__":
    main()

# ============================================================================
# KEY LEARNING POINTS FROM STEP 2.3:
# 
# 1. SLIDING WINDOWS: Overlapping time intervals for smooth analysis
# 2. WINDOW vs SLIDE: window("5 minutes", "1 minute") = 5min window, 1min slide
# 3. OVERLAPPING EVENTS: Same transaction appears in multiple windows
# 4. SMOOTH TRENDS: Better for moving averages and trend detection
# 5. REAL-TIME MONITORING: Continuous fraud detection and velocity tracking
# 6. PERFORMANCE COST: Higher memory usage than tumbling windows
# 7. USE CASES: Fraud detection, trend analysis, anomaly monitoring
# 8. TRADE-OFFS: Smoother analysis vs computational overhead
# 
# NEXT: Step 2.4 - Watermarking (handling late-arriving data)
# ============================================================================