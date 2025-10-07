# ============================================================================
# PySpark Example 4: Advanced DataFrameReader - Schema Management & Multi-Format Sources
# ============================================================================
# INTENT: Demonstrate advanced data ingestion patterns with explicit schema definition
# CONCEPTS: Schema enforcement, multiple data formats, error handling modes, performance optimization
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, when, isnan, isnull

# --------------------------------------------
# 1. Spark Session Setup
# --------------------------------------------
spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("lab_04") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()


# ============================================================================
# SCHEMA DEFINITION STRATEGIES
# ============================================================================

# --------------------------------------------
# Strategy 1: Programmatic Schema (StructType) - Most Flexible
# --------------------------------------------
# Benefits: Type safety, IDE support, reusable, version control friendly
flightSchemaStruct = StructType([
    StructField("FL_DATE", DateType(), nullable=True),
    StructField("OP_CARRIER", StringType(), nullable=False),           # Required field
    StructField("OP_CARRIER_FL_NUM", IntegerType(), nullable=True),
    StructField("ORIGIN", StringType(), nullable=False),               # Required field
    StructField("ORIGIN_CITY_NAME", StringType(), nullable=True),
    StructField("DEST", StringType(), nullable=False),                 # Required field
    StructField("DEST_CITY_NAME", StringType(), nullable=True),
    StructField("CRS_DEP_TIME", IntegerType(), nullable=True),
    StructField("DEP_TIME", IntegerType(), nullable=True),
    StructField("WHEELS_ON", IntegerType(), nullable=True),
    StructField("TAXI_IN", IntegerType(), nullable=True),
    StructField("CRS_ARR_TIME", IntegerType(), nullable=True),
    StructField("ARR_TIME", IntegerType(), nullable=True),
    StructField("CANCELLED", IntegerType(), nullable=True),
    StructField("DISTANCE", IntegerType(), nullable=True)
])


# --------------------------------------------
# Strategy 2: DDL Schema (String) - SQL-Like Syntax
# --------------------------------------------
# Benefits: Familiar to SQL developers, compact syntax
flightSchemaDDL = """
    FL_DATE DATE, 
    OP_CARRIER STRING NOT NULL, 
    OP_CARRIER_FL_NUM INT, 
    ORIGIN STRING NOT NULL, 
    ORIGIN_CITY_NAME STRING, 
    DEST STRING NOT NULL, 
    DEST_CITY_NAME STRING, 
    CRS_DEP_TIME INT, 
    DEP_TIME INT, 
    WHEELS_ON INT, 
    TAXI_IN INT, 
    CRS_ARR_TIME INT, 
    ARR_TIME INT, 
    CANCELLED INT, 
    DISTANCE INT
"""


# ============================================================================
# DATA SOURCE 1: CSV FILES WITH ADVANCED OPTIONS
# ============================================================================

print("=== Reading CSV with Explicit Schema ===")

# Advanced CSV reading with schema enforcement
flightTimeCsvDF = spark.read \
    .format("csv") \
    .schema(flightSchemaStruct) \
    .option("path", "./source/flight-time.csv") \
    .option("header", "true") \
    .option("dateFormat", "M/d/y") \
    .option("timestampFormat", "M/d/y H:m") \
    .option("mode", "FAILFAST") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .option("multiline", "false") \
    .option("escape", '"') \
    .option("encoding", "UTF-8") \
    .load()
    
 
# Schema enforcement benefits:
# 1. Prevents runtime errors from type mismatches
# 2. Improves performance (no schema inference overhead)
# 3. Ensures data consistency across pipeline runs
   
   
print("CSV Schema:")
flightTimeCsvDF.printSchema()
print(f"CSV Record Count: {flightTimeCsvDF.count()}")
# flightTimeCsvDF.show(5, truncate=False)
   
   

# ============================================================================
# DATA SOURCE 2: JSON FILES WITH DDL SCHEMA
# ============================================================================

print("\n=== Reading JSON with DDL Schema ===")

# First, let's diagnose the JSON file structure
print("Diagnosing JSON file structure...")
flightTimeJsonDF = None  # Initialize variable

try:
    with open("source/flight-time.json", "r") as f:
        content = f.read()
        lines = content.splitlines()
        print(f"JSON file size: {len(content)} characters")
        print(f"Number of lines: {len(lines)}")
        print(f"First line preview: {lines[0][:100] if lines else 'Empty file'}...")
        
        # Check if it's a JSON array or JSON lines
        if content.strip().startswith('['):
            print("Format: JSON Array - use multiLine=true")
        elif lines and all(line.strip().startswith('{') for line in lines[:5] if line.strip()):
            print("Format: JSON Lines (JSONL) - use multiLine=false")
        else:
            print("Format: Unknown or malformed")
    
    # JSON reading with DDL schema - configured for JSON Lines format
    flightTimeJsonDF = spark.read \
        .format("json") \
        .schema(flightSchemaDDL) \
        .option("dateFormat", "M/d/y") \
        .option("timestampFormat", "M/d/y H:m") \
        .option("mode", "FAILFAST") \
        .option("multiLine", "false") \
        .load("source/flight-time.json")
            
except FileNotFoundError:
    print("JSON file not found - skipping JSON processing")
except Exception as e:
    print(f"Error reading JSON file: {e}")

if flightTimeJsonDF is None:
    print("Creating empty DataFrame for demonstration...")
    flightTimeJsonDF = spark.createDataFrame([], schema=flightSchemaDDL)

print("JSON Schema:")
flightTimeJsonDF.printSchema()
print(f"JSON Record Count: {flightTimeJsonDF.count()}")
# flightTimeJsonDF.show(5, truncate=False)
   
   

# ============================================================================
# DATA SOURCE 3: PARQUET FILES (SELF-DESCRIBING FORMAT)
# ============================================================================

print("\n=== Reading Parquet (Schema-Aware Format) ===")

# Parquet files contain schema metadata - no need for explicit schema
flightTimeParquetDF = spark.read \
    .format("parquet") \
    .option("mode", "FAILFAST") \
    .option("mergeSchema", "true") \
    .option("pathGlobFilter", "*.parquet") \
    .load("source/flight-time.parquet")

print("Parquet Schema (Auto-detected):")
flightTimeParquetDF.printSchema()
print(f"Parquet Record Count: {flightTimeParquetDF.count()}")
flightTimeParquetDF.show(5, truncate=False)   



# ============================================================================
# ADVANCED DATA QUALITY AND VALIDATION
# ============================================================================

print("\n=== Data Quality Checks ===")

# Check for data quality issues across all formats
def data_quality_report(df, source_name):
    """Generate comprehensive data quality report"""
    print(f"\n--- {source_name} Data Quality Report ---")
    
    # Basic statistics
    total_records = df.count()
    print(f"Total Records: {total_records}")
    
    # Check for nulls in critical fields
    critical_fields = ["OP_CARRIER", "ORIGIN", "DEST"]
    for field in critical_fields:
        null_count = df.filter(col(field).isNull()).count()
        print(f"Null values in {field}: {null_count}")
    
    # Check for cancelled flights
    if "CANCELLED" in df.columns:
        cancelled_count = df.filter(col("CANCELLED") == 1).count()
        cancellation_rate = (cancelled_count / total_records) * 100 if total_records > 0 else 0
        print(f"Cancelled Flights: {cancelled_count} ({cancellation_rate:.2f}%)")
    
    # Check date range
    if "FL_DATE" in df.columns:
        # First check for null dates
        null_dates = df.filter(col("FL_DATE").isNull()).count()
        total_dates = df.count()
        
        if null_dates > 0:
            print(f"Null FL_DATE values: {null_dates} out of {total_dates}")
        
        # Get actual date range (excluding nulls) - fixed aggregation
        try:
            date_stats = df.filter(col("FL_DATE").isNotNull()) \
                          .agg({"FL_DATE": "min"}, {"FL_DATE": "max"}) \
                          .collect()
            
            if date_stats and len(date_stats) > 0:
                # Get min and max separately
                min_date_result = df.filter(col("FL_DATE").isNotNull()).agg({"FL_DATE": "min"}).collect()
                max_date_result = df.filter(col("FL_DATE").isNotNull()).agg({"FL_DATE": "max"}).collect()
                
                if min_date_result and max_date_result:
                    min_date = min_date_result[0][0]
                    max_date = max_date_result[0][0]
                    print(f"Date range: {min_date} to {max_date}")
                else:
                    print("No valid dates found in FL_DATE column")
            else:
                print("No valid dates found in FL_DATE column")
        except Exception as e:
            print(f"Error analyzing dates: {e}")
            # Alternative approach - just show a sample
            try:
                sample_dates = df.select("FL_DATE").filter(col("FL_DATE").isNotNull()).limit(5).collect()
                if sample_dates:
                    print(f"Sample dates: {[row[0] for row in sample_dates]}")
                else:
                    print("All FL_DATE values appear to be null")
            except:
                print("Unable to analyze FL_DATE column")

# Run quality checks on all datasets
data_quality_report(flightTimeCsvDF, "CSV")
data_quality_report(flightTimeJsonDF, "JSON") 
data_quality_report(flightTimeParquetDF, "Parquet")

# ============================================================================
# PERFORMANCE COMPARISON AND OPTIMIZATION
# ============================================================================

print("\n=== Performance Analysis ===")

# Compare read performance across formats
import time

def benchmark_read_performance():
    """Benchmark reading performance across different formats"""
    formats = [
        ("CSV", lambda: spark.read.schema(flightSchemaStruct).csv("source/flight-time.csv", header=True)),
        ("JSON", lambda: spark.read.schema(flightSchemaDDL).json("source/flight-time.json")),
        ("Parquet", lambda: spark.read.parquet("source/flight-time.parquet"))
    ]
    
    for format_name, read_func in formats:
        start_time = time.time()
        df = read_func()
        count = df.count()  # Trigger action to measure actual read time
        end_time = time.time()
        
        print(f"{format_name}: {count} records in {end_time - start_time:.3f} seconds")

# benchmark_read_performance()  # Uncomment to run benchmark

# ============================================================================
# SCHEMA EVOLUTION AND COMPATIBILITY
# ============================================================================

print("\n=== Schema Evolution Demonstration ===")

# Show how to handle schema evolution scenarios
def demonstrate_schema_evolution():
    """Show techniques for handling schema changes"""
    
    # Original schema
    original_schema = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("ORIGIN", StringType()),
        StructField("DEST", StringType())
    ])
    
    # Evolved schema (added new field)
    evolved_schema = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("ORIGIN", StringType()),
        StructField("DEST", StringType()),
        StructField("AIRCRAFT_TYPE", StringType())  # New field
    ])
    
    print("Schema evolution strategies:")
    print("1. Add nullable fields for backward compatibility")
    print("2. Use schema merging for Parquet files")
    print("3. Version your schemas in production")
    print("4. Implement graceful degradation for missing fields")

demonstrate_schema_evolution()



print("\n=== Error Handling Modes ===")

# Demonstrate different error handling approaches
error_modes = {
    "FAILFAST": "Fail immediately on any malformed record (default for production)",
    "DROPMALFORMED": "Drop malformed records and continue processing",
    "PERMISSIVE": "Set malformed records to null and continue (stores corrupt data in _corrupt_record column)"
}

for mode, description in error_modes.items():
    print(f"{mode}: {description}")


# Example of using PERMISSIVE mode for data exploration
print("\nExample with PERMISSIVE mode:")
try:
    permissive_df = spark.read \
        .format("csv") \
        .schema(flightSchemaStruct) \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .option("header", "true") \
        .csv("source/flight-time.csv")
    
    # Check if corrupt record column exists
    if "_corrupt_record" in permissive_df.columns:
        corrupt_count = permissive_df.filter(col("_corrupt_record").isNotNull()).count()
        print(f"Corrupt records found: {corrupt_count}")
    else:
        print("No corrupt records found - all data parsed successfully")
    
except Exception as e:
    print(f"Error: {e}")



# ============================================================================
# BEST PRACTICES SUMMARY
# ============================================================================

print("\n=== Best Practices Summary ===")
best_practices = [
    "✓ Always define explicit schemas in production (avoid inferSchema=True)",
    "✓ Use appropriate data types to minimize storage and improve performance", 
    "✓ Choose Parquet for analytics workloads (columnar, compressed, schema-aware)",
    "✓ Use CSV for data exchange, JSON for semi-structured data",
    "✓ Implement data quality checks as part of your ingestion pipeline",
    "✓ Use FAILFAST mode in production to catch data issues early",
    "✓ Consider partitioning large datasets for better performance",
    "✓ Version your schemas and plan for evolution",
    "✓ Monitor performance and adjust based on data characteristics"
]

for practice in best_practices:
    print(practice)

# --------------------------------------------
# 6. Cleanup
# --------------------------------------------
spark.stop()

# ============================================================================
# SUMMARY: Advanced DataFrameReader capabilities demonstrating:
# - Multiple schema definition strategies (StructType vs DDL)
# - Format-specific options and optimizations
# - Error handling modes for production robustness  
# - Data quality validation techniques
# - Performance considerations across formats
# - Schema evolution and compatibility planning
# ============================================================================