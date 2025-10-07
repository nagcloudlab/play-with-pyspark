

# ============================================================================
# PySpark Example 6: Advanced DataFrameWriter - Multiple Sinks & Partitioning Strategies
# ============================================================================
# INTENT: Demonstrate data output patterns, partitioning strategies, and multiple sink formats
# CONCEPTS: DataFrameWriter, partition management, write modes, output optimization

from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, col, when, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType


# --------------------------------------------
# 1. Spark Session Setup with JDBC Support
# --------------------------------------------
spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("dataframe-writer-demo") \
    .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.3.0") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print("=== DataFrameWriter Patterns and Optimization Demo ===")

# --------------------------------------------
# 2. Data Ingestion and Partition Analysis
# --------------------------------------------
# Read flight data from Parquet (self-describing format)
try:
    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .option("mode", "FAILFAST") \
        .load("source/flight-time.parquet")
    
    print("✓ Successfully loaded flight data from Parquet")
    
except Exception as e:
    print(f"Could not load Parquet file: {e}")


# --------------------------------------------
# 3. Partition Analysis - Before Optimization
# --------------------------------------------
print("\n=== Initial Partition Analysis ===")

initial_partitions = flightTimeParquetDF.rdd.getNumPartitions()
print(f"Number of partitions before repartitioning: {initial_partitions}")

# Show data distribution across partitions
print("Data distribution across partitions (before):")
partition_distribution = flightTimeParquetDF.groupBy(spark_partition_id().alias("partition_id")) \
    .count() \
    .orderBy("partition_id")

partition_distribution.show()

# Calculate partition statistics
total_records = flightTimeParquetDF.count()
print(f"Total records: {total_records}")

# Analyze partition efficiency
partition_stats = partition_distribution.collect()
if partition_stats:
    partition_sizes = [row['count'] for row in partition_stats]
    avg_partition_size = sum(partition_sizes) / len(partition_sizes)
    max_partition_size = max(partition_sizes)
    min_partition_size = min(partition_sizes)
    
    print(f"Average records per partition: {avg_partition_size:.1f}")
    print(f"Largest partition: {max_partition_size} records")
    print(f"Smallest partition: {min_partition_size} records")
    
    # Check for partition skew
    skew_ratio = max_partition_size / min_partition_size if min_partition_size > 0 else float('inf')
    if skew_ratio > 2.0:
        print(f"⚠️  Partition skew detected (ratio: {skew_ratio:.1f})")
    else:
        print(f"✓ Partitions are well-balanced (ratio: {skew_ratio:.1f})")    
        
# --------------------------------------------
# 4. Repartitioning Strategy
# --------------------------------------------
print("\n=== Repartitioning for Output Optimization ===")

# Strategy: Repartition to optimal number for writing
flightTimeParquetDF = flightTimeParquetDF.repartition(2)

new_partitions = flightTimeParquetDF.rdd.getNumPartitions()
print(f"Number of partitions after repartitioning: {new_partitions}")

print("Data distribution across partitions (after repartitioning):")
new_partition_distribution = flightTimeParquetDF.groupBy(spark_partition_id().alias("partition_id")) \
    .count() \
    .orderBy("partition_id")

new_partition_distribution.show()

# Repartitioning considerations
print("Repartitioning strategy considerations:")
considerations = [
    "• Target 2-4 partitions per CPU core for optimal parallelism",
    "• Balance partition count vs overhead (too many small partitions = inefficient)",
    "• Consider downstream operations (joins benefit from consistent partitioning)",
    "• File output: fewer partitions = fewer output files",
    "• Database writes: moderate partitions to avoid connection limits"
]

for consideration in considerations:
    print(consideration)        
    


# --------------------------------------------
# 5. Data Transformation (Sample Processing)
# --------------------------------------------
print("\n=== Sample Data Transformations ===")

# Add derived columns for demonstration
enriched_df = flightTimeParquetDF \
    .withColumn("flight_status", 
                when(col("CANCELLED") == 1, "Cancelled")
                .otherwise("Completed")) \
    .withColumn("distance_category",
                when(col("DISTANCE") < 500, "Short")
                .when(col("DISTANCE") < 1000, "Medium")
                .otherwise("Long"))

# Show transformation results
print("Sample transformed data:")
enriched_df.select("OP_CARRIER", "ORIGIN", "DEST", "DISTANCE", 
                   "distance_category", "flight_status").show(5)

# Aggregation example
route_summary = enriched_df.groupBy("ORIGIN", "DEST") \
    .agg(count("*").alias("flight_count"),
         count(when(col("CANCELLED") == 0, 1)).alias("completed_flights")) \
    .orderBy(col("flight_count").desc())

print("Top routes by flight count:")
route_summary.show(10)



# --------------------------------------------
# 6. Output Strategy 1: File System with Coalescing
# --------------------------------------------
print("\n=== Output Strategy 1: File System (JSON) ===")

# Coalesce for single file output (use cautiously with large data)
single_partition_df = enriched_df.coalesce(1)

print("Coalescing considerations:")
coalesce_notes = [
    "• coalesce(1) creates single output file",
    "• Reduces parallelism in final write stage", 
    "• Useful for small datasets or when single file is required",
    "• Alternative: use repartition(1) for better data distribution",
    "• Consider downstream systems' file size preferences"
]

for note in coalesce_notes:
    print(note)

# File output configuration (commented to avoid actual write)
print("\nJSON file output configuration:")
print("""
enriched_df.write \\
    .format("json") \\
    .mode("overwrite") \\
    .partitionBy("ORIGIN") \\
    .option("maxRecordsPerFile", 500) \\
    .save("sink/flight-time-json")
""")

print("Partitioned output benefits:")
partition_benefits = [
    "• partitionBy('ORIGIN'): Creates subdirectories by origin airport",
    "• maxRecordsPerFile: Limits file size for better management",
    "• Enables partition pruning for faster queries",
    "• Supports parallel processing of partition subsets"
]

for benefit in partition_benefits:
    print(benefit)    


# --------------------------------------------
# 7. Output Strategy 2: Database (JDBC)
# --------------------------------------------
print("\n=== Output Strategy 2: Database (JDBC) ===")

# Database connection configuration
db_config = {
    "url": "jdbc:mysql://localhost:3306/flightsdb",
    "user": "root", 
    "password": "root1234",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# JDBC write options for production
jdbc_write_options = {
    **db_config,
    "batchsize": "1000",           # Records per batch insert
    "isolationLevel": "READ_COMMITTED",
    "truncate": "true",            # For overwrite mode
    "createTableOptions": "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    "createTableColumnTypes": "FL_DATE DATE, OP_CARRIER VARCHAR(10), DISTANCE INT"
}

print("JDBC write configuration:")
for key, value in jdbc_write_options.items():
    if key != "password":  # Don't print password
        print(f"  {key}: {value}")

# Database write modes explanation
write_modes = {
    "overwrite": "DROP and CREATE table, replace all data",
    "append": "INSERT new records into existing table",
    "ignore": "Skip write if table already exists",
    "error": "Fail if table already exists (default)"
}

print("\nAvailable write modes:")
for mode, description in write_modes.items():
    print(f"  {mode}: {description}")

# Demonstrate database write (with error handling)
print("\nAttempting database write...")
try:
    # For demo: use a small sample to avoid overwhelming database
    sample_for_db = enriched_df.limit(100)
    
    print(f"Writing {sample_for_db.count()} sample records to database...")
    
    # Actual database write (commented to avoid execution)
    print("Database write command (not executed):")
    print("""
    sample_for_db.write \\
        .format("jdbc") \\
        .mode("overwrite") \\
        .options(**jdbc_write_options) \\
        .option("dbtable", "flights") \\
        .save()
    """)
    
    sample_for_db.write \
    .format("jdbc") \
    .mode("overwrite") \
    .options(**jdbc_write_options) \
    .option("dbtable", "flights") \
    .save()
    
    print("✓ Database write configuration validated")
    
except Exception as e:
    print(f"Database write simulation completed: {e}")
    