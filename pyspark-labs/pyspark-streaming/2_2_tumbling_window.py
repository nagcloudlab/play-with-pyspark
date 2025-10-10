# 2_2_tumbling_windows.py
# Step 2.2: Tumbling Windows - Fixed Time Interval Analysis
# Purpose: Master tumbling windows for time-based aggregations in financial data

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# ============================================================================
# LEARNING OBJECTIVES FOR STEP 2.2:
# 1. Tumbling window concept and implementation
# 2. Different window sizes for various business needs
# 3. Window start/end time handling
# 4. Non-overlapping time interval analysis
# 5. Business hour vs 24/7 windowing strategies
# 6. Window alignment and boundary handling
# 7. Performance considerations for different window sizes
# ============================================================================

def create_spark_session():
    """Create Spark session optimized for tumbling window operations"""
    return SparkSession.builder \
        .appName("TumblingWindowAnalysis") \
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
def get_base_transaction_stream():
    """Get base enriched transaction stream for windowing analysis"""
    
    print("📖 Setting up base transaction stream...")
    
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
        .withColumn("is_international", col("location.country") != "US")
    
    print("✅ Base transaction stream configured!")
    return transactions

# ============================================================================
# TUMBLING WINDOW 1: SHORT INTERVALS (30 SECONDS)
# Purpose: Real-time monitoring and immediate alerts
# ============================================================================
def setup_short_interval_windows(transactions):
    """Setup 30-second tumbling windows for real-time monitoring"""
    
    print("⚡ Setting up short interval (30-second) tumbling windows...")
    
    # 30-second windows for real-time monitoring
    short_windows = transactions \
        .groupBy(
            window(col("event_timestamp"), "30 seconds"),  # 30-second tumbling windows
            col("transaction_type"),
            col("channel")
        ) \
        .agg(
            # Volume metrics
            count("*").alias("transaction_count"),
            sum("amount_usd").alias("total_volume_usd"),
            avg("amount_usd").alias("avg_amount_usd"),
            max("amount_usd").alias("max_amount_usd"),
            
            # Risk metrics
            sum(when(col("is_high_value"), 1).otherwise(0)).alias("high_value_count"),
            sum(when(col("is_international"), 1).otherwise(0)).alias("international_count"),
            
            # Customer metrics
            countDistinct("account_id").alias("unique_customers"),
            
            # Revenue metrics
            sum("transaction_fee").alias("total_fees")
        ) \
        .select(
            # Window information
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            
            # Grouping dimensions
            col("transaction_type"),
            col("channel"),
            
            # Metrics
            col("transaction_count"),
            round(col("total_volume_usd"), 2).alias("total_volume_usd"),
            round(col("avg_amount_usd"), 2).alias("avg_amount_usd"),
            round(col("max_amount_usd"), 2).alias("max_amount_usd"),
            col("high_value_count"),
            col("international_count"),
            col("unique_customers"),
            round(col("total_fees"), 2).alias("total_fees"),
            
            # Calculate percentages
            round((col("high_value_count") / col("transaction_count") * 100), 1).alias("high_value_pct"),
            round((col("international_count") / col("transaction_count") * 100), 1).alias("international_pct")
        ) \
        .orderBy(col("window_start").desc(), col("total_volume_usd").desc())
    
    # Output short interval windows
    short_window_query = short_windows \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "10") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/short-windows") \
        .trigger(processingTime='15 seconds') \
        .start()
    
    print("✅ Short interval windows started!")
    return short_window_query

# ============================================================================
# TUMBLING WINDOW 2: MEDIUM INTERVALS (5 MINUTES)
# Purpose: Operational monitoring and business metrics
# ============================================================================
def setup_medium_interval_windows(transactions):
    """Setup 5-minute tumbling windows for operational monitoring"""
    
    print("📊 Setting up medium interval (5-minute) tumbling windows...")
    
    # 5-minute windows for business operations
    medium_windows = transactions \
        .groupBy(
            window(col("event_timestamp"), "5 minutes"),  # 5-minute tumbling windows
            col("location.country")
        ) \
        .agg(
            # Transaction metrics
            count("*").alias("transaction_count"),
            sum("amount_usd").alias("total_volume_usd"),
            avg("amount_usd").alias("avg_transaction_size"),
            min("amount_usd").alias("min_amount"),
            max("amount_usd").alias("max_amount"),
            stddev("amount_usd").alias("amount_stddev"),
            
            # Transaction type breakdown
            sum(when(col("transaction_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            sum(when(col("transaction_type") == "withdrawal", 1).otherwise(0)).alias("withdrawal_count"),
            sum(when(col("transaction_type") == "transfer", 1).otherwise(0)).alias("transfer_count"),
            sum(when(col("transaction_type") == "deposit", 1).otherwise(0)).alias("deposit_count"),
            
            # Channel breakdown
            sum(when(col("channel") == "online", 1).otherwise(0)).alias("online_count"),
            sum(when(col("channel") == "atm", 1).otherwise(0)).alias("atm_count"),
            sum(when(col("channel") == "pos", 1).otherwise(0)).alias("pos_count"),
            sum(when(col("channel") == "mobile", 1).otherwise(0)).alias("mobile_count"),
            
            # Business metrics
            countDistinct("account_id").alias("unique_customers"),
            countDistinct("merchant").alias("unique_merchants"),
            sum("transaction_fee").alias("total_fees_generated")
        ) \
        .select(
            # Window information
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            concat(
                date_format(col("window.start"), "HH:mm:ss"), 
                lit(" - "), 
                date_format(col("window.end"), "HH:mm:ss")
            ).alias("time_range"),
            
            # Geography
            col("country"),
            
            # Core metrics
            col("transaction_count"),
            round(col("total_volume_usd"), 2).alias("total_volume_usd"),
            round(col("avg_transaction_size"), 2).alias("avg_transaction_size"),
            round(col("amount_stddev"), 2).alias("amount_stddev"),
            
            # Transaction type percentages
            round((col("purchase_count") / col("transaction_count") * 100), 1).alias("purchase_pct"),
            round((col("withdrawal_count") / col("transaction_count") * 100), 1).alias("withdrawal_pct"),
            round((col("transfer_count") / col("transaction_count") * 100), 1).alias("transfer_pct"),
            
            # Business metrics
            col("unique_customers"),
            col("unique_merchants"),
            round(col("total_fees_generated"), 2).alias("total_fees_generated")
        ) \
        .orderBy(col("window_start").desc(), col("total_volume_usd").desc())
    
    # Output medium interval windows
    medium_window_query = medium_windows \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "8") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/medium-windows") \
        .trigger(processingTime='30 seconds') \
        .start()
    
    print("✅ Medium interval windows started!")
    return medium_window_query

# ============================================================================
# TUMBLING WINDOW 3: HOURLY INTERVALS
# Purpose: Business intelligence and reporting
# ============================================================================
def setup_hourly_windows(transactions):
    """Setup hourly tumbling windows for business intelligence"""
    
    print("📈 Setting up hourly tumbling windows...")
    
    # Hourly windows for business intelligence
    hourly_windows = transactions \
        .groupBy(
            window(col("event_timestamp"), "1 hour")  # 1-hour tumbling windows
        ) \
        .agg(
            # Volume and count metrics
            count("*").alias("total_transactions"),
            sum("amount_usd").alias("total_volume_usd"),
            avg("amount_usd").alias("avg_transaction_size"),
            
            # Statistical metrics
            expr("percentile_approx(amount_usd, 0.5)").alias("median_amount"),
            expr("percentile_approx(amount_usd, 0.95)").alias("p95_amount"),
            expr("percentile_approx(amount_usd, 0.99)").alias("p99_amount"),
            
            # Customer analysis
            countDistinct("account_id").alias("unique_customers"),
            count("account_id").alias("total_transactions_check"),
            
            # Geographic distribution
            countDistinct("location.country").alias("countries_served"),
            countDistinct("location.city").alias("cities_served"),
            
            # Channel performance
            sum(when(col("channel") == "online", col("amount_usd")).otherwise(0)).alias("online_volume"),
            sum(when(col("channel") == "mobile", col("amount_usd")).otherwise(0)).alias("mobile_volume"),
            sum(when(col("channel") == "atm", col("amount_usd")).otherwise(0)).alias("atm_volume"),
            sum(when(col("channel") == "pos", col("amount_usd")).otherwise(0)).alias("pos_volume"),
            
            # Risk indicators
            sum(when(col("is_high_value"), 1).otherwise(0)).alias("high_value_transactions"),
            sum(when(col("is_international"), 1).otherwise(0)).alias("international_transactions"),
            
            # Revenue metrics
            sum("transaction_fee").alias("total_fee_revenue"),
            
            # Time-based metrics
            min("event_timestamp").alias("first_transaction_time"),
            max("event_timestamp").alias("last_transaction_time")
        ) \
        .select(
            # Window information with readable format
            col("window.start").alias("hour_start"),
            col("window.end").alias("hour_end"),
            date_format(col("window.start"), "yyyy-MM-dd HH:00").alias("hour_label"),
            hour(col("window.start")).alias("hour_of_day"),
            
            # Volume metrics
            col("total_transactions"),
            round(col("total_volume_usd"), 2).alias("total_volume_usd"),
            round(col("avg_transaction_size"), 2).alias("avg_transaction_size"),
            
            # Statistical distribution
            round(col("median_amount"), 2).alias("median_amount"),
            round(col("p95_amount"), 2).alias("p95_amount"), 
            round(col("p99_amount"), 2).alias("p99_amount"),
            
            # Customer metrics
            col("unique_customers"),
            round((col("total_transactions") / col("unique_customers")), 2).alias("transactions_per_customer"),
            
            # Geographic reach
            col("countries_served"),
            col("cities_served"),
            
            # Channel split (percentages)
            round((col("online_volume") / col("total_volume_usd") * 100), 1).alias("online_volume_pct"),
            round((col("mobile_volume") / col("total_volume_usd") * 100), 1).alias("mobile_volume_pct"),
            round((col("atm_volume") / col("total_volume_usd") * 100), 1).alias("atm_volume_pct"),
            
            # Risk percentages
            round((col("high_value_transactions") / col("total_transactions") * 100), 1).alias("high_value_pct"),
            round((col("international_transactions") / col("total_transactions") * 100), 1).alias("international_pct"),
            
            # Revenue
            round(col("total_fee_revenue"), 2).alias("total_fee_revenue")
        ) \
        .orderBy(col("hour_start").desc())
    
    # Output hourly windows
    hourly_window_query = hourly_windows \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "6") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/hourly-windows") \
        .trigger(processingTime='60 seconds') \
        .start()
    
    print("✅ Hourly windows started!")
    return hourly_window_query

# ============================================================================
# TUMBLING WINDOW 4: BUSINESS DAY WINDOWS
# Purpose: Daily business reporting and reconciliation
# ============================================================================
def setup_business_day_windows(transactions):
    """Setup business day (9 AM - 5 PM) tumbling windows"""
    
    print("💼 Setting up business day tumbling windows...")
    
    # Filter for business hours (9 AM - 5 PM) and create daily business windows
    business_hours_transactions = transactions \
        .withColumn("transaction_hour", hour(col("event_timestamp"))) \
        .filter((col("transaction_hour") >= 9) & (col("transaction_hour") < 17)) \
        .withColumn("business_date", to_date(col("event_timestamp")))
    
    # Daily business metrics
    business_day_windows = business_hours_transactions \
        .groupBy(
            col("business_date"),
            col("location.country")
        ) \
        .agg(
            # Daily volume metrics
            count("*").alias("daily_transaction_count"),
            sum("amount_usd").alias("daily_volume_usd"),
            avg("amount_usd").alias("daily_avg_amount"),
            
            # Peak hour analysis
            max(struct(col("transaction_hour"), col("amount_usd"))).alias("peak_hour_info"),
            
            # Daily unique metrics
            countDistinct("account_id").alias("daily_unique_customers"),
            countDistinct("merchant").alias("daily_unique_merchants"),
            
            # Daily revenue
            sum("transaction_fee").alias("daily_fee_revenue"),
            
            # Daily risk metrics
            sum(when(col("is_high_value"), 1).otherwise(0)).alias("daily_high_value_count"),
            max("amount_usd").alias("daily_max_transaction"),
            
            # Business hour distribution
            sum(when(col("transaction_hour").between(9, 11), 1).otherwise(0)).alias("morning_transactions"),
            sum(when(col("transaction_hour").between(12, 14), 1).otherwise(0)).alias("lunch_transactions"),
            sum(when(col("transaction_hour").between(15, 17), 1).otherwise(0)).alias("afternoon_transactions"),
            
            # First and last transactions of business day
            min("event_timestamp").alias("first_business_transaction"),
            max("event_timestamp").alias("last_business_transaction")
        ) \
        .select(
            col("business_date"),
            col("country"),
            
            # Daily metrics
            col("daily_transaction_count"),
            round(col("daily_volume_usd"), 2).alias("daily_volume_usd"),
            round(col("daily_avg_amount"), 2).alias("daily_avg_amount"),
            round(col("daily_max_transaction"), 2).alias("daily_max_transaction"),
            
            # Customer metrics
            col("daily_unique_customers"),
            col("daily_unique_merchants"),
            round((col("daily_transaction_count") / col("daily_unique_customers")), 2).alias("transactions_per_customer"),
            
            # Revenue
            round(col("daily_fee_revenue"), 2).alias("daily_fee_revenue"),
            
            # Risk metrics
            col("daily_high_value_count"),
            round((col("daily_high_value_count") / col("daily_transaction_count") * 100), 1).alias("high_value_pct"),
            
            # Time distribution
            round((col("morning_transactions") / col("daily_transaction_count") * 100), 1).alias("morning_pct"),
            round((col("lunch_transactions") / col("daily_transaction_count") * 100), 1).alias("lunch_pct"),
            round((col("afternoon_transactions") / col("daily_transaction_count") * 100), 1).alias("afternoon_pct")
        ) \
        .orderBy(col("business_date").desc(), col("daily_volume_usd").desc())
    
    # Output business day windows
    business_day_query = business_day_windows \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "5") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/business-day-windows") \
        .trigger(processingTime='120 seconds') \
        .start()
    
    print("✅ Business day windows started!")
    return business_day_query

# ============================================================================
# MAIN TUMBLING WINDOWS PIPELINE
# ============================================================================
def main():
    """Execute the complete tumbling windows analysis pipeline"""
    
    print("🚀 Starting Step 2.2: Tumbling Windows Pipeline")
    print("=" * 60)
    
    # Step 1: Get base transaction stream
    print("\n📖 Step 1: Setting up base transaction stream...")
    transactions = get_base_transaction_stream()
    
    # Step 2: Short interval windows (30 seconds)
    print("\n⚡ Step 2: Setting up short interval windows...")
    short_query = setup_short_interval_windows(transactions)
    
    # Step 3: Medium interval windows (5 minutes)
    print("\n📊 Step 3: Setting up medium interval windows...")
    medium_query = setup_medium_interval_windows(transactions)
    
    # Step 4: Hourly windows
    print("\n📈 Step 4: Setting up hourly windows...")
    hourly_query = setup_hourly_windows(transactions)
    
    # Step 5: Business day windows
    print("\n💼 Step 5: Setting up business day windows...")
    business_query = setup_business_day_windows(transactions)
    
    # Collect all queries
    all_queries = [short_query, medium_query, hourly_query, business_query]
    
    print(f"\n✅ All {len(all_queries)} tumbling window streams started!")
    print("\n📊 What to observe:")
    print("• 30-second windows: Real-time monitoring")
    print("• 5-minute windows: Operational metrics by country")
    print("• Hourly windows: Business intelligence with percentiles")
    print("• Business day windows: Daily reconciliation (9 AM - 5 PM)")
    print("• Non-overlapping time intervals")
    print("• Window start/end boundaries")
    print("• Different aggregation levels")
    print("\n🎯 Key Learning:")
    print("• window() function creates tumbling windows")
    print("• Window size determines granularity")
    print("• Windows are non-overlapping (tumbling)")
    print("• Each event belongs to exactly one window")
    print("• Larger windows = more complete data, higher latency")
    print("• Smaller windows = faster insights, less complete")
    print("\n🛑 Press Ctrl+C to stop all streams")
    
    try:
        # Wait for all queries to finish
        for query in all_queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping all tumbling window streams...")
        for i, query in enumerate(all_queries):
            print(f"Stopping tumbling window stream {i+1}/{len(all_queries)}...")
            query.stop()
        spark.stop()
        print("✅ All tumbling window streams stopped successfully!")
        print("\n🎯 Next: Step 2.3 - Sliding Windows (overlapping intervals)")

if __name__ == "__main__":
    main()

# ============================================================================
# KEY LEARNING POINTS FROM STEP 2.2:
# 
# 1. TUMBLING WINDOWS: Non-overlapping fixed time intervals
# 2. WINDOW SIZES: 30s (real-time), 5min (operational), 1hr (BI), daily (reconciliation)
# 3. WINDOW BOUNDARIES: Clear start/end times, no overlap
# 4. BUSINESS ALIGNMENT: Windows aligned to business needs
# 5. AGGREGATION LEVELS: Different metrics for different windows
# 6. PERFORMANCE: Larger windows = higher latency but more complete data
# 7. USE CASES: Real-time monitoring vs business reporting
# 8. EVENT ASSIGNMENT: Each event belongs to exactly one window
# 
# NEXT: Step 2.3 - Sliding Windows (overlapping time intervals)
# ============================================================================