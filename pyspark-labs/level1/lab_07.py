# ============================================================================
# PySpark Example 7: Spark Catalog & Managed Tables with Bucketing
# ============================================================================
# INTENT: Demonstrate Spark's built-in catalog system, database management, and bucketing optimization
# CONCEPTS: Hive metastore, managed tables, bucketing, catalog operations, persistent storage

from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, col, count, avg, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# --------------------------------------------
# 1. Spark Session with Hive Support
# --------------------------------------------
# enableHiveSupport() allows Spark to use Hive metastore for table management
spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("spark-catalog-demo") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "./spark-warehouse") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print("=== Spark Catalog and Managed Tables Demo ===")

# Verify Hive support is enabled
print(f"Catalog implementation: {spark.catalog}")
print(f"Warehouse directory: {spark.conf.get('spark.sql.warehouse.dir')}")



# --------------------------------------------
# 2. Data Preparation with Partition Analysis
# --------------------------------------------
# Read source data (with error handling for missing files)
try:
    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .option("mode", "FAILFAST") \
        .load("source/flight-time.parquet")
    
    print("✓ Successfully loaded flight data from Parquet")
    
except Exception as e:
    print(f"Could not load Parquet file: {e}")
    print("Creating sample flight data for demonstration...")
    
    # Create comprehensive sample data
    sample_schema = StructType([
        StructField("FL_DATE", DateType(), True),
        StructField("OP_CARRIER", StringType(), True),
        StructField("OP_CARRIER_FL_NUM", IntegerType(), True),
        StructField("ORIGIN", StringType(), True),
        StructField("ORIGIN_CITY_NAME", StringType(), True),
        StructField("DEST", StringType(), True),
        StructField("DEST_CITY_NAME", StringType(), True),
        StructField("CRS_DEP_TIME", IntegerType(), True),
        StructField("DEP_TIME", IntegerType(), True),
        StructField("ARR_TIME", IntegerType(), True),
        StructField("CANCELLED", IntegerType(), True),
        StructField("DISTANCE", IntegerType(), True)
    ])
    
    # Generate diverse sample data for bucketing demonstration
    sample_data = []
    carriers = ["DL", "AA", "US", "UA", "WN"]
    origins = ["BOS", "ATL", "NYC", "LAX", "ORD"]
    dests = ["ATL", "LAX", "ORD", "DFW", "LAS"]
    
    for i in range(1000):
        carrier = carriers[i % len(carriers)]
        origin = origins[i % len(origins)]
        dest = dests[i % len(dests)]
        
        sample_data.append((
            "2000-01-01", carrier, 1000 + i, origin, f"{origin} City", 
            dest, f"{dest} City", 1000, 1005, 1200, 0, 500 + (i % 1000)
        ))
    
    flightTimeParquetDF = spark.createDataFrame(sample_data, sample_schema)

# --------------------------------------------
# 3. Initial Partition Analysis
# --------------------------------------------
print("\n=== Partition Analysis Before Optimization ===")

initial_partitions = flightTimeParquetDF.rdd.getNumPartitions()
print(f"Number of partitions before repartitioning: {initial_partitions}")

print("Data distribution across initial partitions:")
flightTimeParquetDF.groupBy(spark_partition_id().alias("partition_id")) \
    .count() \
    .orderBy("partition_id") \
    .show()

# --------------------------------------------
# 4. Repartitioning Strategy
# --------------------------------------------
print("\n=== Strategic Repartitioning ===")

# Repartition for better parallel processing
flightTimeParquetDF = flightTimeParquetDF.repartition(2)

new_partitions = flightTimeParquetDF.rdd.getNumPartitions()
print(f"Number of partitions after repartitioning: {new_partitions}")

print("Data distribution after repartitioning:")
flightTimeParquetDF.groupBy(spark_partition_id().alias("partition_id")) \
    .count() \
    .orderBy("partition_id") \
    .show()
# --------------------------------------------

# --------------------------------------------
# 5. Data Transformations and Analysis
# --------------------------------------------
print("\n=== Data Analysis Before Persisting ===")

# Analyze carrier distribution for bucketing strategy
carrier_analysis = flightTimeParquetDF.groupBy("OP_CARRIER") \
    .agg(count("*").alias("flight_count")) \
    .orderBy(desc("flight_count"))

print("Flight count by carrier (bucketing key analysis):")
carrier_analysis.show()

# Route popularity analysis
route_analysis = flightTimeParquetDF.groupBy("ORIGIN", "DEST") \
    .agg(count("*").alias("route_count")) \
    .orderBy(desc("route_count"))

print("Top routes:")
route_analysis.show(10)


# --------------------------------------------
# 6. Database and Catalog Management
# --------------------------------------------
print("\n=== Catalog and Database Management ===")

# List current databases
print("Current databases:")
for db in spark.catalog.listDatabases():
    print(f"  - {db.name}: {db.description}")

# Check if AIRLINE_DB exists and clean up
existing_tables = []
try:
    existing_tables = spark.catalog.listTables("AIRLINE_DB")
    if existing_tables:
        print("Existing tables in AIRLINE_DB:")
        for table in existing_tables:
            print(f"  - {table.name} ({table.tableType})")
except Exception:
    print("AIRLINE_DB database does not exist yet")

# Clean up existing table
try:
    spark.sql("DROP TABLE IF EXISTS AIRLINE_DB.flight_data_tbl")
    print("✓ Dropped existing flight_data_tbl table")
except Exception as e:
    print(f"No existing table to drop: {e}")

# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
print("✓ Created/verified AIRLINE_DB database")

# Set current database
spark.catalog.setCurrentDatabase("AIRLINE_DB")
current_db = spark.catalog.currentDatabase()
print(f"Current database: {current_db}")


# --------------------------------------------
# 7. Bucketing Strategy and Benefits
# --------------------------------------------

print("\n=== Bucketing Strategy Analysis ===")

# Coalesce to single partition for consistent bucketing
flightTimeParquetDF = flightTimeParquetDF.coalesce(1)

# Analyze bucketing column cardinality
carrier_count = flightTimeParquetDF.select("OP_CARRIER").distinct().count()
print(f"Number of unique carriers: {carrier_count}")

bucketing_benefits = [
    "• Pre-sorts data by OP_CARRIER for faster joins and aggregations",
    "• Creates 5 buckets to distribute data evenly across carriers",
    "• Eliminates shuffle operations for carrier-based operations", 
    "• Improves query performance for carrier-specific analytics",
    "• Enables bucket pruning when filtering by carrier"
]

print("Bucketing benefits:")
for benefit in bucketing_benefits:
    print(benefit)

bucket_considerations = [
    "✓ Choose bucket count based on data distribution and cluster size",
    "✓ Bucket column should have good cardinality (not too few, not too many values)",
    "✓ Sort columns should align with common query patterns",
    "✓ Bucketing works best with tables that are frequently joined or grouped",
    "⚠️ Bucketing can create small files if bucket count is too high"
]

print("\nBucketing considerations:")
for consideration in bucket_considerations:
    print(consideration)



# --------------------------------------------
# 8. Managed Table Creation with Bucketing
# --------------------------------------------
print("\n=== Creating Managed Table with Bucketing ===")

# Create managed table with bucketing and sorting
print("Creating bucketed and sorted managed table...")

try:
    flightTimeParquetDF.write \
        .format("parquet") \
        .bucketBy(5, "OP_CARRIER") \
        .sortBy("OP_CARRIER") \
        .mode("overwrite") \
        .saveAsTable("flight_data_tbl")
    
    print("✓ Successfully created managed table: flight_data_tbl")
    
except Exception as e:
    print(f"Error creating bucketed table: {e}")
    print("Creating standard managed table instead...")
    
    flightTimeParquetDF.write \
        .format("parquet") \
        .mode("overwrite") \
        .saveAsTable("flight_data_tbl")
    
    print("✓ Created standard managed table: flight_data_tbl")


# --------------------------------------------
# 9. Catalog Operations and Table Metadata
# --------------------------------------------
print("\n=== Catalog Operations and Metadata ===")

# List tables in current database
tables = spark.catalog.listTables("AIRLINE_DB")
print("Tables in AIRLINE_DB:")
for table in tables:
    print(f"  Name: {table.name}")
    print(f"  Type: {table.tableType}")
    print(f"  Database: {table.database}")
    if hasattr(table, 'description') and table.description:
        print(f"  Description: {table.description}")
    print()

# Get detailed table information
try:
    table_details = spark.sql("DESCRIBE EXTENDED AIRLINE_DB.flight_data_tbl").collect()
    print("Table schema and metadata:")
    for row in table_details:
        print(f"  {row[0]}: {row[1]} {row[2] if row[2] else ''}")
except Exception as e:
    print(f"Could not retrieve extended table details: {e}")

# Show table properties
try:
    properties = spark.sql("SHOW TBLPROPERTIES AIRLINE_DB.flight_data_tbl").collect()
    print("\nTable properties:")
    for prop in properties:
        print(f"  {prop[0]}: {prop[1]}")
except Exception as e:
    print(f"Could not retrieve table properties: {e}")



# --------------------------------------------
# 10. Querying the Managed Table
# --------------------------------------------
print("\n=== Querying the Managed Table ===")

# Query the managed table using SQL
print("Sample queries on the managed table:")

# Basic query
sample_query1 = spark.sql("""
    SELECT OP_CARRIER, COUNT(*) as flight_count
    FROM AIRLINE_DB.flight_data_tbl 
    GROUP BY OP_CARRIER 
    ORDER BY flight_count DESC
""")

print("Flights by carrier:")
sample_query1.show()


# More complex query leveraging bucketing
sample_query2 = spark.sql("""
    SELECT 
        OP_CARRIER,
        ORIGIN,
        COUNT(*) as route_count,
        AVG(DISTANCE) as avg_distance
    FROM AIRLINE_DB.flight_data_tbl 
    WHERE OP_CARRIER IN ('DL', 'AA', 'US')
    GROUP BY OP_CARRIER, ORIGIN
    ORDER BY OP_CARRIER, route_count DESC
""")

print("Route analysis by major carriers (benefits from bucketing):")
sample_query2.show()


# Query using DataFrame API on managed table
managed_table_df = spark.table("AIRLINE_DB.flight_data_tbl")
print(f"Records in managed table: {managed_table_df.count()}")



# --------------------------------------------
# 12. Advanced Catalog Features
# --------------------------------------------
print("\n=== Advanced Catalog Features ===")

# Show catalog capabilities
catalog_features = [
    "✓ Persistent metadata storage across Spark sessions",
    "✓ Schema evolution and compatibility checking", 
    "✓ Partitioning and bucketing metadata management",
    "✓ Query optimization through statistics and indexes",
    "✓ Integration with external catalogs (Hive, Delta, Iceberg)",
    "✓ Table ACLs and security integration",
    "✓ ACID transactions (with compatible formats like Delta)",
    "✓ Time travel queries (format-dependent)"
]

print("Spark Catalog capabilities:")
for feature in catalog_features:
    print(feature)

# Demonstrate additional catalog operations
try:
    # Cache table for faster subsequent queries
    spark.sql("CACHE TABLE AIRLINE_DB.flight_data_tbl")
    print("✓ Table cached for improved query performance")
    
    # Show cached tables
    cached_tables = spark.sql("SHOW TABLES").filter(col("isTemporary") == False)
    print("Cached tables available:")
    cached_tables.show()
    
except Exception as e:
    print(f"Caching operation: {e}")

# --------------------------------------------
# 13. Best Practices and Recommendations
# --------------------------------------------
print("\n=== Best Practices for Managed Tables ===")

best_practices = [
    "✓ Use bucketing for tables frequently joined or grouped by specific columns",
    "✓ Choose bucket counts that align with cluster parallelism (multiples of core count)",
    "✓ Monitor file sizes - avoid creating too many small files with excessive bucketing",
    "✓ Use appropriate storage formats (Parquet for analytics, Delta for ACID requirements)",
    "✓ Implement table partitioning for time-series data to enable partition pruning",
    "✓ Regular ANALYZE TABLE commands to update statistics for query optimization",
    "✓ Consider external tables for data shared across multiple systems",
    "✓ Implement data governance policies through catalog metadata",
    "✓ Use meaningful table and column names with proper documentation"
]

for practice in best_practices:
    print(practice)

# --------------------------------------------
# 14. Cleanup Demonstration
# --------------------------------------------
print("\n=== Resource Management ===")

# Show table management operations
management_commands = [
    "DROP TABLE AIRLINE_DB.flight_data_tbl",           # Remove table and data
    "UNCACHE TABLE AIRLINE_DB.flight_data_tbl",        # Remove from cache
    "DROP DATABASE AIRLINE_DB CASCADE",                 # Remove database and all tables
    "REFRESH TABLE AIRLINE_DB.flight_data_tbl",        # Refresh metadata
    "ANALYZE TABLE AIRLINE_DB.flight_data_tbl COMPUTE STATISTICS"  # Update statistics
]

print("Available table management commands:")
for i, cmd in enumerate(management_commands, 1):
    print(f"  {i}. {cmd}")

# Final verification
final_table_list = spark.catalog.listTables("AIRLINE_DB")
print(f"\nFinal table count in AIRLINE_DB: {len(final_table_list)}")

print("\nStopping Spark session...")
spark.stop()

print("✓ Session ended - managed tables persist in warehouse directory")

# ============================================================================
# SUMMARY: Spark Catalog and Managed Tables demonstrating:
# - Hive metastore integration for persistent table management
# - Strategic bucketing for query performance optimization
# - Database and catalog operations for data organization
# - Managed vs external table considerations
# - Advanced catalog features and metadata management
# - Performance tuning through bucketing and sorting strategies
# - Best practices for enterprise data lake architectures
# ============================================================================