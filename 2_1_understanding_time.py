# 2_1_understanding_time.py
# Step 2.1: Understanding Time - Event Time vs Processing Time Concepts
# Purpose: Master time concepts fundamental to streaming analytics

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# ============================================================================
# LEARNING OBJECTIVES FOR STEP 2.1:
# 1. Event time vs Processing time concepts
# 2. Timestamp extraction and parsing
# 3. Timezone handling in financial systems
# 4. Late-arriving data scenarios
# 5. Clock skew and time synchronization issues
# 6. Time-based data quality checks
# 7. Temporal data visualization patterns
# ============================================================================

def create_spark_session():
    """Create Spark session optimized for time-based processing"""
    return SparkSession.builder \
        .appName("TimeProcessingConcepts") \
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
# TRANSACTION SCHEMA WITH TIME FOCUS
# ============================================================================
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("event_time", StringType(), True),  # When transaction actually occurred
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
# STEP 1: READ AND PARSE WITH TIME FOCUS
# Purpose: Extract all time-related information for analysis
# ============================================================================
def read_transactions_with_time_analysis():
    """Read transactions and extract comprehensive time information"""
    
    print("📖 Reading transactions with comprehensive time analysis...")
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse and add comprehensive time information
    transactions = kafka_df \
        .select(
            # Kafka metadata with timestamps
            col("key").cast("string").alias("partition_key"),
            col("timestamp").alias("kafka_ingestion_time"),  # When Kafka received the message
            col("topic"), col("partition"), col("offset"),
            
            # Parse transaction data
            from_json(col("value").cast("string"), transaction_schema).alias("transaction"),
            
            # Processing timestamp
            current_timestamp().alias("spark_processing_time")  # When Spark processes the message
        ) \
        .select(
            # Kafka metadata
            col("partition_key"),
            col("kafka_ingestion_time"),
            col("spark_processing_time"),
            col("topic"), col("partition"), col("offset"),
            
            # Transaction data
            col("transaction.*"),
            
            # Parse event time from transaction
            to_timestamp(col("transaction.event_time")).alias("event_timestamp")  # When transaction occurred
        ) \
        .filter(col("event_timestamp").isNotNull())  # Filter invalid timestamps
    
    print("✅ Transaction stream with time analysis configured!")
    return transactions

# ============================================================================
# STEP 2: TIME ANALYSIS AND CALCULATIONS
# Purpose: Calculate time differences and identify time-related patterns
# ============================================================================
def add_time_analysis(df):
    """Add comprehensive time analysis columns"""
    
    print("⏰ Adding time analysis transformations...")
    
    time_analyzed_df = df \
        .withColumn("ingestion_delay_seconds",
                   # Time between event and Kafka ingestion
                   unix_timestamp(col("kafka_ingestion_time")) - unix_timestamp(col("event_timestamp"))) \
        .withColumn("processing_delay_seconds", 
                   # Time between event and Spark processing
                   unix_timestamp(col("spark_processing_time")) - unix_timestamp(col("event_timestamp"))) \
        .withColumn("kafka_to_spark_delay_seconds",
                   # Time between Kafka ingestion and Spark processing
                   unix_timestamp(col("spark_processing_time")) - unix_timestamp(col("kafka_ingestion_time"))) \
        .withColumn("is_late_arriving",
                   # Flag transactions arriving more than 30 seconds late
                   col("ingestion_delay_seconds") > 30) \
        .withColumn("is_very_late",
                   # Flag transactions arriving more than 5 minutes late
                   col("ingestion_delay_seconds") > 300) \
        .withColumn("is_future_event",
                   # Flag events with future timestamps (clock skew)
                   col("event_timestamp") > col("spark_processing_time")) \
        .withColumn("event_hour_utc", hour(col("event_timestamp"))) \
        .withColumn("event_day_of_week", dayofweek(col("event_timestamp"))) \
        .withColumn("processing_hour_utc", hour(col("spark_processing_time"))) \
        .withColumn("time_bucket_5min",
                   # 5-minute time buckets for event time
                   date_trunc("hour", col("event_timestamp")) + 
                   expr("INTERVAL " + (floor(minute(col("event_timestamp")) / 5) * 5).cast("string") + " MINUTES")) \
        .withColumn("delay_category",
                   # Categorize delays for analysis
                   when(col("ingestion_delay_seconds") < 0, "FUTURE")
                   .when(col("ingestion_delay_seconds") <= 5, "IMMEDIATE")
                   .when(col("ingestion_delay_seconds") <= 30, "QUICK")
                   .when(col("ingestion_delay_seconds") <= 300, "DELAYED")
                   .otherwise("VERY_LATE"))
    
    print("✅ Time analysis columns added!")
    return time_analyzed_df

# ============================================================================
# STEP 3: TIME-BASED DATA QUALITY MONITORING
# Purpose: Monitor data quality issues related to time
# ============================================================================
def setup_time_quality_monitoring(transactions):
    """Setup monitoring for time-related data quality issues"""
    
    print("🔍 Setting up time-based data quality monitoring...")
    
    # Monitor 1: Delay distribution analysis
    delay_analysis = transactions \
        .groupBy(
            window(col("spark_processing_time"), "1 minute"),
            col("delay_category")
        ) \
        .agg(
            count("*").alias("transaction_count"),
            avg("ingestion_delay_seconds").alias("avg_delay_seconds"),
            min("ingestion_delay_seconds").alias("min_delay_seconds"),
            max("ingestion_delay_seconds").alias("max_delay_seconds"),
            stddev("ingestion_delay_seconds").alias("stddev_delay_seconds")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("delay_category"),
            col("transaction_count"),
            round(col("avg_delay_seconds"), 2).alias("avg_delay_seconds"),
            col("min_delay_seconds"),
            col("max_delay_seconds"),
            round(col("stddev_delay_seconds"), 2).alias("stddev_delay_seconds")
        ) \
        .orderBy(col("window_start").desc(), col("delay_category"))
    
    delay_query = delay_analysis \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "15") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/delay-analysis") \
        .trigger(processingTime='20 seconds') \
        .start()
    
    print("✅ Delay analysis monitoring started!")
    return delay_query

# ============================================================================
# STEP 4: EVENT TIME VS PROCESSING TIME COMPARISON
# Purpose: Demonstrate the difference between event time and processing time
# ============================================================================
def setup_time_comparison_analysis(transactions):
    """Setup analysis comparing event time vs processing time patterns"""
    
    print("📊 Setting up event time vs processing time comparison...")
    
    # Analysis 1: Event time based aggregation
    event_time_stats = transactions \
        .groupBy(
            window(col("event_timestamp"), "2 minutes"),  # Based on when transaction occurred
            col("transaction_type")
        ) \
        .agg(
            count("*").alias("count_by_event_time"),
            sum("amount").alias("volume_by_event_time"),
            avg("ingestion_delay_seconds").alias("avg_delay")
        ) \
        .select(
            col("window.start").alias("event_window_start"),
            col("transaction_type"),
            col("count_by_event_time"),
            round(col("volume_by_event_time"), 2).alias("volume_by_event_time"),
            round(col("avg_delay"), 1).alias("avg_delay_seconds"),
            lit("EVENT_TIME").alias("aggregation_type")
        )
    
    # Analysis 2: Processing time based aggregation
    processing_time_stats = transactions \
        .groupBy(
            window(col("spark_processing_time"), "2 minutes"),  # Based on when Spark processes
            col("transaction_type")
        ) \
        .agg(
            count("*").alias("count_by_processing_time"),
            sum("amount").alias("volume_by_processing_time")
        ) \
        .select(
            col("window.start").alias("processing_window_start"),
            col("transaction_type"),
            col("count_by_processing_time").alias("count_by_event_time"),  # Rename for union
            round(col("volume_by_processing_time"), 2).alias("volume_by_event_time"),
            lit(0).alias("avg_delay_seconds"),  # Placeholder for union
            lit("PROCESSING_TIME").alias("aggregation_type")
        ) \
        .withColumnRenamed("processing_window_start", "event_window_start")  # Rename for union
    
    # Union both for comparison
    time_comparison = event_time_stats.union(processing_time_stats) \
        .orderBy(col("event_window_start").desc(), col("aggregation_type"), col("transaction_type"))
    
    time_comparison_query = time_comparison \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "20") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/time-comparison") \
        .trigger(processingTime='30 seconds') \
        .start()
    
    print("✅ Time comparison analysis started!")
    return time_comparison_query

# ============================================================================
# STEP 5: LATE DATA IMPACT ANALYSIS
# Purpose: Understand how late data affects aggregation results
# ============================================================================
def setup_late_data_analysis(transactions):
    """Setup analysis to understand late data impact"""
    
    print("🚨 Setting up late data impact analysis...")
    
    # Track how results change as late data arrives
    late_data_impact = transactions \
        .filter(col("is_late_arriving") == True) \
        .groupBy(
            window(col("event_timestamp"), "1 minute"),  # Window based on event time
            col("location.country")
        ) \
        .agg(
            count("*").alias("late_transaction_count"),
            sum("amount").alias("late_transaction_volume"),
            avg("ingestion_delay_seconds").alias("avg_lateness_seconds"),
            max("ingestion_delay_seconds").alias("max_lateness_seconds"),
            min("event_timestamp").alias("earliest_event_in_window"),
            max("spark_processing_time").alias("latest_processing_time")
        ) \
        .select(
            col("window.start").alias("event_window_start"),
            col("window.end").alias("event_window_end"),
            col("country"),
            col("late_transaction_count"),
            round(col("late_transaction_volume"), 2).alias("late_transaction_volume"),
            round(col("avg_lateness_seconds"), 1).alias("avg_lateness_seconds"),
            col("max_lateness_seconds"),
            col("earliest_event_in_window"),
            col("latest_processing_time")
        ) \
        .orderBy(col("event_window_start").desc(), col("late_transaction_count").desc())
    
    late_data_query = late_data_impact \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "10") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/late-data-analysis") \
        .trigger(processingTime='45 seconds') \
        .start()
    
    print("✅ Late data impact analysis started!")
    return late_data_query

# ============================================================================
# STEP 6: TIMEZONE AND BUSINESS HOURS ANALYSIS
# Purpose: Handle timezone complexities in global financial systems
# ============================================================================
def setup_timezone_analysis(transactions):
    """Setup timezone-aware business analysis"""
    
    print("🌍 Setting up timezone and business hours analysis...")
    
    # Convert UTC to major financial market timezones
    timezone_analysis = transactions \
        .withColumn("event_time_ny", 
                   from_utc_timestamp(col("event_timestamp"), "America/New_York")) \
        .withColumn("event_time_london",
                   from_utc_timestamp(col("event_timestamp"), "Europe/London")) \
        .withColumn("event_time_tokyo", 
                   from_utc_timestamp(col("event_timestamp"), "Asia/Tokyo")) \
        .withColumn("ny_business_hours",
                   # NYSE business hours: 9:30 AM - 4:00 PM EST/EDT
                   (hour(col("event_time_ny")) >= 9) & (hour(col("event_time_ny")) <= 16) &
                   (dayofweek(col("event_time_ny")).between(2, 6))) \
        .withColumn("london_business_hours", 
                   # LSE business hours: 8:00 AM - 4:30 PM GMT/BST
                   (hour(col("event_time_london")) >= 8) & (hour(col("event_time_london")) <= 16) &
                   (dayofweek(col("event_time_london")).between(2, 6))) \
        .withColumn("tokyo_business_hours",
                   # TSE business hours: 9:00 AM - 3:00 PM JST
                   (hour(col("event_time_tokyo")) >= 9) & (hour(col("event_time_tokyo")) <= 15) &
                   (dayofweek(col("event_time_tokyo")).between(2, 6))) \
        .groupBy(
            window(col("event_timestamp"), "5 minutes")
        ) \
        .agg(
            count("*").alias("total_transactions"),
            sum(when(col("ny_business_hours"), 1).otherwise(0)).alias("ny_business_hour_count"),
            sum(when(col("london_business_hours"), 1).otherwise(0)).alias("london_business_hour_count"),
            sum(when(col("tokyo_business_hours"), 1).otherwise(0)).alias("tokyo_business_hour_count"),
            sum("amount").alias("total_volume")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("total_transactions"),
            col("ny_business_hour_count"),
            col("london_business_hour_count"), 
            col("tokyo_business_hour_count"),
            round(col("total_volume"), 2).alias("total_volume"),
            round((col("ny_business_hour_count") / col("total_transactions") * 100), 1).alias("ny_business_pct"),
            round((col("london_business_hour_count") / col("total_transactions") * 100), 1).alias("london_business_pct"),
            round((col("tokyo_business_hour_count") / col("total_transactions") * 100), 1).alias("tokyo_business_pct")
        ) \
        .orderBy(col("window_start").desc())
    
    timezone_query = timezone_analysis \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "8") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/timezone-analysis") \
        .trigger(processingTime='60 seconds') \
        .start()
    
    print("✅ Timezone analysis started!")
    return timezone_query

# ============================================================================
# MAIN TIME ANALYSIS PIPELINE
# ============================================================================
def main():
    """Execute the complete time analysis pipeline"""
    
    print("🚀 Starting Step 2.1: Understanding Time Pipeline")
    print("=" * 60)
    
    # Step 1: Read transactions with time focus
    print("\n📖 Step 1: Reading transactions with time analysis...")
    transactions = read_transactions_with_time_analysis()
    
    # Step 2: Add comprehensive time analysis
    print("\n⏰ Step 2: Adding time analysis...")
    time_analyzed = add_time_analysis(transactions)
    
    # Step 3: Setup time quality monitoring
    print("\n🔍 Step 3: Setting up time quality monitoring...")
    delay_query = setup_time_quality_monitoring(time_analyzed)
    
    # Step 4: Setup time comparison analysis
    print("\n📊 Step 4: Setting up time comparison analysis...")
    comparison_query = setup_time_comparison_analysis(time_analyzed)
    
    # Step 5: Setup late data analysis
    print("\n🚨 Step 5: Setting up late data analysis...")
    late_data_query = setup_late_data_analysis(time_analyzed)
    
    # Step 6: Setup timezone analysis
    print("\n🌍 Step 6: Setting up timezone analysis...")
    timezone_query = setup_timezone_analysis(time_analyzed)
    
    # Collect all queries
    all_queries = [delay_query, comparison_query, late_data_query, timezone_query]
    
    print(f"\n✅ All {len(all_queries)} time analysis streams started!")
    print("\n📊 What to observe:")
    print("• Delay distribution: IMMEDIATE, QUICK, DELAYED, VERY_LATE")
    print("• Event time vs Processing time aggregation differences")
    print("• Late arriving data impact on windows")
    print("• Business hours across different timezones")
    print("• Clock skew and future events")
    print("• Kafka ingestion vs Spark processing delays")
    print("\n🎯 Key Learning:")
    print("• Event time is when something happened")
    print("• Processing time is when we process it")
    print("• Late data can change historical results")
    print("• Timezone handling is critical for global systems")
    print("• Delays must be monitored and handled")
    print("\n🛑 Press Ctrl+C to stop all streams")
    
    try:
        # Wait for all queries to finish
        for query in all_queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping all time analysis streams...")
        for i, query in enumerate(all_queries):
            print(f"Stopping time analysis stream {i+1}/{len(all_queries)}...")
            query.stop()
        spark.stop()
        print("✅ All time analysis streams stopped successfully!")
        print("\n🎯 Next: Step 2.2 - Tumbling Windows (fixed time intervals)")

if __name__ == "__main__":
    main()

# ============================================================================
# KEY LEARNING POINTS FROM STEP 2.1:
# 
# 1. EVENT TIME: When events actually occurred (business time)
# 2. PROCESSING TIME: When system processes events (system time)
# 3. INGESTION TIME: When data enters the streaming system
# 4. LATE DATA: Events arriving after their event time window
# 5. CLOCK SKEW: Time differences between distributed systems
# 6. TIMEZONE HANDLING: Critical for global financial operations
# 7. DELAY MONITORING: Essential for system health
# 8. BUSINESS HOURS: Timezone-aware operational windows
# 
# FOUNDATION SET! Next: Tumbling Windows for fixed time intervals
# ============================================================================