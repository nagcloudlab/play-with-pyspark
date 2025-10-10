# 1_3_basic_transformations.py
# Step 1.3: Basic Transformations - Financial Data Processing
# Purpose: Learn core DataFrame transformations for financial data validation and enrichment

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# ============================================================================
# LEARNING OBJECTIVES FOR STEP 1.3:
# 1. Data validation techniques for financial data
# 2. Adding calculated fields (fees, taxes, derived columns)
# 3. Filtering and cleansing streaming data
# 4. Currency conversion and normalization
# 5. Adding business logic to streaming transformations
# ============================================================================

def create_spark_session():
    """Create Spark session optimized for streaming transformations"""
    return SparkSession.builder \
        .appName("FinancialTransformations") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

spark = create_spark_session()
spark.sparkContext.setLogLevel("WARN")

# ============================================================================
# TRANSACTION SCHEMA (Same as previous step)
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
# STEP 1: READ AND PARSE TRANSACTIONS (from previous step)
# ============================================================================
def read_and_parse_transactions():
    """Read and parse transaction stream - foundation for transformations"""
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON and flatten structure
    transactions = kafka_df \
        .select(
            col("key").cast("string").alias("partition_key"),
            col("timestamp").alias("kafka_timestamp"),
            col("topic"),
            col("partition"),
            col("offset"),
            from_json(col("value").cast("string"), transaction_schema).alias("transaction")
        ) \
        .select(
            col("partition_key"),
            col("kafka_timestamp"), 
            col("topic"),
            col("partition"),
            col("offset"),
            col("transaction.*"),
            to_timestamp(col("transaction.event_time")).alias("event_timestamp")
        )
    
    return transactions

# ============================================================================
# STEP 2: DATA VALIDATION TRANSFORMATIONS
# Purpose: Ensure data quality and identify problematic records
# ============================================================================
def add_data_validation(df):
    """Add validation flags to identify data quality issues"""
    
    print("🔍 Adding data validation transformations...")
    
    validated_df = df \
        .withColumn("is_valid_amount", 
                   # Amount should be positive and reasonable (< $100,000)
                   (col("amount") > 0) & (col("amount") <= 100000)) \
        .withColumn("is_valid_currency",
                   # Currency should be 3-character code
                   col("currency").rlike("^[A-Z]{3}$")) \
        .withColumn("is_valid_account", 
                   # Account ID should follow pattern ACC######
                   col("account_id").rlike("^ACC[0-9]{6}$")) \
        .withColumn("is_valid_transaction_type",
                   # Transaction type should be one of allowed values
                   col("transaction_type").isin(["purchase", "refund", "transfer", "withdrawal", "deposit"])) \
        .withColumn("is_recent_transaction",
                   # Transaction should be within last 24 hours (detect very old transactions)
                   col("event_timestamp") >= (current_timestamp() - expr("INTERVAL 24 HOURS"))) \
        .withColumn("overall_data_quality",
                   # Overall quality score (all validations must pass)
                   col("is_valid_amount") & 
                   col("is_valid_currency") & 
                   col("is_valid_account") & 
                   col("is_valid_transaction_type") & 
                   col("is_recent_transaction"))
    
    print("✅ Data validation columns added!")
    return validated_df

# ============================================================================
# STEP 3: BUSINESS LOGIC TRANSFORMATIONS  
# Purpose: Add calculated fields that business users need
# ============================================================================
def add_business_calculations(df):
    """Add business-specific calculated fields"""
    
    print("💰 Adding business calculation transformations...")
    
    enriched_df = df \
        .withColumn("transaction_fee",
                   # Calculate fees based on transaction type and amount
                   when(col("transaction_type") == "transfer", 
                        when(col("amount") <= 1000, 2.50)
                        .when(col("amount") <= 5000, 5.00)
                        .otherwise(10.00))
                   .when(col("transaction_type") == "withdrawal", 
                        when(col("channel") == "atm", 2.00)
                        .otherwise(0.00))
                   .when(col("transaction_type") == "purchase",
                        when(col("location.country") != "US", col("amount") * 0.03)
                        .otherwise(0.00))
                   .otherwise(0.00)) \
        .withColumn("tax_amount",
                   # Calculate tax for purchases (varies by country)
                   when((col("transaction_type") == "purchase"),
                        when(col("location.country") == "US", col("amount") * 0.08)  # 8% sales tax
                        .when(col("location.country") == "CA", col("amount") * 0.13)  # 13% HST
                        .when(col("location.country") == "UK", col("amount") * 0.20)  # 20% VAT
                        .otherwise(col("amount") * 0.10))  # Default 10%
                   .otherwise(0.00)) \
        .withColumn("net_amount",
                   # Total amount including fees and taxes
                   col("amount") + col("transaction_fee") + col("tax_amount")) \
        .withColumn("amount_usd",
                   # Convert all amounts to USD (simplified conversion rates)
                   when(col("currency") == "USD", col("amount"))
                   .when(col("currency") == "EUR", col("amount") * 1.10)
                   .when(col("currency") == "GBP", col("amount") * 1.25) 
                   .when(col("currency") == "CAD", col("amount") * 0.75)
                   .when(col("currency") == "AUD", col("amount") * 0.68)
                   .otherwise(col("amount")))  # Default to original amount
    
    print("✅ Business calculations added!")
    return enriched_df

# ============================================================================
# STEP 4: TIME-BASED DERIVED FIELDS
# Purpose: Add time-related fields for analysis and fraud detection
# ============================================================================
def add_time_derived_fields(df):
    """Add time-based derived fields for temporal analysis"""
    
    print("🕒 Adding time-based derived fields...")
    
    time_enriched_df = df \
        .withColumn("transaction_hour", hour(col("event_timestamp"))) \
        .withColumn("transaction_day_of_week", dayofweek(col("event_timestamp"))) \
        .withColumn("transaction_month", month(col("event_timestamp"))) \
        .withColumn("is_weekend", 
                   dayofweek(col("event_timestamp")).isin([1, 7])) \
        .withColumn("is_business_hours",
                   # Business hours: 9 AM to 5 PM on weekdays
                   (~col("is_weekend")) & 
                   (col("transaction_hour") >= 9) & 
                   (col("transaction_hour") <= 17)) \
        .withColumn("is_late_night",
                   # Late night: 11 PM to 6 AM  
                   (col("transaction_hour") >= 23) | (col("transaction_hour") <= 6)) \
        .withColumn("processing_delay_seconds",
                   # How long between event time and processing time
                   unix_timestamp(col("kafka_timestamp")) - unix_timestamp(col("event_timestamp")))
    
    print("✅ Time-based fields added!")
    return time_enriched_df

# ============================================================================
# STEP 5: RISK AND FRAUD INDICATORS
# Purpose: Add basic risk flags for fraud detection
# ============================================================================
def add_risk_indicators(df):
    """Add basic risk and fraud indicator flags"""
    
    print("🚨 Adding risk indicator transformations...")
    
    risk_df = df \
        .withColumn("is_high_amount",
                   # Flag high-value transactions
                   col("amount_usd") >= 5000) \
        .withColumn("is_international",
                   # Flag international transactions
                   col("location.country") != "US") \
        .withColumn("is_unusual_time",
                   # Flag transactions at unusual times
                   col("is_late_night") | col("is_weekend")) \
        .withColumn("is_cash_equivalent",
                   # Flag cash-like transactions (higher risk)
                   col("transaction_type").isin(["withdrawal", "transfer"])) \
        .withColumn("risk_score",
                   # Simple risk scoring (0-100 scale)
                   (when(col("is_high_amount"), 25).otherwise(0) +
                    when(col("is_international"), 20).otherwise(0) +
                    when(col("is_unusual_time"), 15).otherwise(0) +
                    when(col("is_cash_equivalent"), 10).otherwise(0) +
                    when(col("channel") == "online", 10).otherwise(0) +
                    when(~col("overall_data_quality"), 30).otherwise(0))) \
        .withColumn("risk_level",
                   # Categorize risk levels
                   when(col("risk_score") >= 70, "HIGH")
                   .when(col("risk_score") >= 40, "MEDIUM")
                   .when(col("risk_score") >= 20, "LOW")
                   .otherwise("MINIMAL"))
    
    print("✅ Risk indicators added!")
    return risk_df

# ============================================================================
# STEP 6: DATA FILTERING
# Purpose: Separate valid from invalid transactions
# ============================================================================
def filter_transactions(df):
    """Filter transactions into valid and invalid streams"""
    
    print("🔄 Creating filtered transaction streams...")
    
    # Valid transactions (good data quality)
    valid_transactions = df.filter(col("overall_data_quality") == True)
    
    # Invalid transactions (for dead letter queue)
    invalid_transactions = df.filter(col("overall_data_quality") == False)
    
    # High-risk transactions (for fraud investigation)
    high_risk_transactions = df.filter(col("risk_level") == "HIGH")
    
    print("✅ Transaction filtering configured!")
    return valid_transactions, invalid_transactions, high_risk_transactions

# ============================================================================
# MAIN TRANSFORMATION PIPELINE
# ============================================================================
def main():
    """Execute the complete transformation pipeline"""
    
    print("🚀 Starting Step 1.3: Basic Transformations Pipeline")
    print("=" * 60)
    
    # Step 1: Read base transaction stream
    print("\n📖 Step 1: Reading transaction stream...")
    transactions = read_and_parse_transactions()
    
    # Step 2: Add validation
    print("\n🔍 Step 2: Adding data validation...")
    validated_transactions = add_data_validation(transactions)
    
    # Step 3: Add business calculations  
    print("\n💰 Step 3: Adding business calculations...")
    business_enriched = add_business_calculations(validated_transactions)
    
    # Step 4: Add time-based fields
    print("\n🕒 Step 4: Adding time-based fields...")
    time_enriched = add_time_derived_fields(business_enriched)
    
    # Step 5: Add risk indicators
    print("\n🚨 Step 5: Adding risk indicators...")
    risk_enriched = add_risk_indicators(time_enriched)
    
    # Step 6: Filter transactions
    print("\n🔄 Step 6: Filtering transactions...")
    valid_txns, invalid_txns, high_risk_txns = filter_transactions(risk_enriched)
    
    # Display enriched valid transactions
    print("\n📊 Displaying enriched valid transactions...")
    
    query = valid_txns \
        .select(
            "transaction_id",
            "account_id",
            "transaction_type", 
            "amount",
            "currency",
            "amount_usd",
            "transaction_fee",
            "tax_amount",
            "net_amount",
            "risk_score",
            "risk_level",
            "transaction_hour",
            "is_business_hours",
            "location.country"
        ) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "5") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print("✅ Transformation pipeline started!")
    print("💡 You should see enriched transactions with calculated fields...")
    print("🔍 Look for: fees, taxes, risk scores, time-based flags")
    print("🛑 Press Ctrl+C to stop")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping transformation pipeline...")
        query.stop()
        spark.stop()
        print("✅ Pipeline stopped successfully!")

if __name__ == "__main__":
    main()

# ============================================================================
# KEY LEARNING POINTS FROM STEP 1.3:
# 
# 1. DATA VALIDATION: Always validate streaming data quality
# 2. BUSINESS LOGIC: Add domain-specific calculations (fees, taxes)
# 3. DERIVED FIELDS: Create useful fields from existing data
# 4. RISK SCORING: Implement basic risk assessment in real-time
# 5. FILTERING: Separate good from bad data for different processing paths
# 6. CURRENCY CONVERSION: Handle multi-currency financial data
# 7. TIME ANALYSIS: Extract temporal patterns for business insights
# 
# NEXT STEP 1.4: We'll add different output sinks (files, multiple formats)
# ============================================================================