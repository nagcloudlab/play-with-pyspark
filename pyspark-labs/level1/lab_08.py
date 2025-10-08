
# ============================================================================
# PySpark Example 8: Reading from Hive Tables & SQL-Based Data Processing
# ============================================================================
# INTENT: Demonstrate reading from managed tables and SQL-based analytics workflows
# CONCEPTS: Spark SQL, managed table queries, cross-session persistence, analytics patterns

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import spark_partition_id, col, count, avg, max as spark_max, sum as spark_sum, desc


# --------------------------------------------
# 1. Spark Session with Hive Integration
# --------------------------------------------
spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("hive-table-analytics") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "./spark-warehouse") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()
    
    
print("=== Reading from Hive Tables & SQL Analytics ===")

# Verify catalog connectivity
current_database = spark.catalog.currentDatabase()
print(f"Current database: {current_database}")

# List available databases
print("Available databases:")
for db in spark.catalog.listDatabases():
    print(f"  - {db.name}")

# --------------------------------------------
# 2. Table Existence Verification
# --------------------------------------------
print("\n=== Table Verification ===")

# Check if target database and table exist
try:
    # List tables in AIRLINE_DB
    tables = spark.catalog.listTables("AIRLINE_DB")
    table_names = [table.name for table in tables]
    
    print(f"Tables in AIRLINE_DB: {table_names}")
    
    if "flight_data_tbl" in table_names:
        print("✓ flight_data_tbl table found")
        
        # Get table schema
        table_schema = spark.table("AIRLINE_DB.flight_data_tbl").schema
        print("Table schema:")
        for field in table_schema.fields:
            nullable = "nullable" if field.nullable else "not null"
            print(f"  {field.name}: {field.dataType} ({nullable})")
            
    else:
        print("⚠️ flight_data_tbl table not found")
        raise Exception("Target table does not exist")
        
except Exception as e:
    print(f"Table verification failed: {e}")
    print("Creating sample data for demonstration...")
    
    # Create sample data structure
    sample_schema = StructType([
        StructField("FL_DATE", DateType(), True),
        StructField("OP_CARRIER", StringType(), True),
        StructField("OP_CARRIER_FL_NUM", IntegerType(), True),
        StructField("ORIGIN", StringType(), True),
        StructField("DEST", StringType(), True),
        StructField("DISTANCE", IntegerType(), True),
        StructField("CANCELLED", IntegerType(), True)
    ])
    
    # Generate sample flight data
    sample_data = []
    carriers = ["DL", "AA", "US", "UA", "WN"]
    airports = ["ATL", "LAX", "ORD", "DFW", "DEN", "JFK", "SFO", "LAS", "SEA", "MIA"]
    
    for i in range(500):
        carrier = carriers[i % len(carriers)]
        origin = airports[i % len(airports)]
        dest = airports[(i + 3) % len(airports)]  # Different destination
        
        sample_data.append((
            "2000-01-01", carrier, 1000 + i, origin, dest, 
            400 + (i % 1200), 1 if i % 20 == 0 else 0  # 5% cancellation rate
        ))
    
    # Create temporary view for demonstration
    sample_df = spark.createDataFrame(sample_data, sample_schema)
    sample_df.createOrReplaceTempView("flight_data_tbl")
    
    print("✓ Created sample data view for demonstration")    
    


# --------------------------------------------
# 3. Basic Table Reading and Inspection
# --------------------------------------------
print("\n=== Reading from Managed Table ===")

# Read from the managed table using SQL
try:
    DF = spark.sql("SELECT * FROM AIRLINE_DB.flight_data_tbl")
    print("✓ Successfully read from AIRLINE_DB.flight_data_tbl")
except:
    # Fallback to temporary view
    DF = spark.sql("SELECT * FROM flight_data_tbl")
    print("✓ Reading from temporary view")

# Basic table statistics
record_count = DF.count()
column_count = len(DF.columns)

print(f"Table statistics:")
print(f"  Records: {record_count:,}")
print(f"  Columns: {column_count}")
print(f"  Partitions: {DF.rdd.getNumPartitions()}")

# Show data distribution across partitions
print("\nData distribution across partitions:")
DF.groupBy(spark_partition_id().alias("partition_id")) \
  .count() \
  .orderBy("partition_id") \
  .show()

# Sample data preview
print("\nSample data (first 10 rows):")
DF.show(10, truncate=False)    



# --------------------------------------------
# 4. SQL-Based Analytics and Transformations
# --------------------------------------------
print("\n=== SQL-Based Analytics ===")

# Analysis 1: Carrier Performance Analysis
carrier_analysis = spark.sql("""
    SELECT 
        OP_CARRIER,
        COUNT(*) as total_flights,
        SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) as cancelled_flights,
        ROUND(AVG(CASE WHEN CANCELLED = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) as cancellation_rate,
        AVG(DISTANCE) as avg_distance,
        MAX(DISTANCE) as max_distance
    FROM flight_data_tbl
    GROUP BY OP_CARRIER
    ORDER BY total_flights DESC
""")

print("Carrier Performance Analysis:")
carrier_analysis.show(truncate=False)

# Analysis 2: Route Analysis
route_analysis = spark.sql("""
    SELECT 
        ORIGIN,
        DEST,
        COUNT(*) as flight_count,
        COUNT(DISTINCT OP_CARRIER) as carriers_serving,
        AVG(DISTANCE) as avg_distance,
        SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) as cancelled_count
    FROM flight_data_tbl
    GROUP BY ORIGIN, DEST
    HAVING COUNT(*) >= 5
    ORDER BY flight_count DESC
""")

print("Popular Routes Analysis (5+ flights):")
route_analysis.show(15, truncate=False)

# Analysis 3: Airport Hub Analysis
airport_analysis = spark.sql("""
    SELECT 
        airport,
        SUM(departures) as total_departures,
        SUM(arrivals) as total_arrivals,
        SUM(departures + arrivals) as total_operations,
        AVG(avg_distance) as weighted_avg_distance
    FROM (
        SELECT ORIGIN as airport, COUNT(*) as departures, 0 as arrivals, AVG(DISTANCE) as avg_distance
        FROM flight_data_tbl
        GROUP BY ORIGIN
        
        UNION ALL
        
        SELECT DEST as airport, 0 as departures, COUNT(*) as arrivals, AVG(DISTANCE) as avg_distance
        FROM flight_data_tbl
        GROUP BY DEST
    ) airport_ops
    GROUP BY airport
    ORDER BY total_operations DESC
""")

print("Airport Hub Analysis (by total operations):")
airport_analysis.show(truncate=False)



# --------------------------------------------
# 5. Advanced SQL Analytics Patterns
# --------------------------------------------
print("\n=== Advanced Analytics Patterns ===")

# Pattern 1: Window Functions for Ranking
ranking_analysis = spark.sql("""
    SELECT 
        OP_CARRIER,
        ORIGIN,
        DEST,
        COUNT(*) as flight_count,
        RANK() OVER (PARTITION BY OP_CARRIER ORDER BY COUNT(*) DESC) as route_rank_by_carrier,
        DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) as overall_route_rank
    FROM flight_data_tbl
    GROUP BY OP_CARRIER, ORIGIN, DEST
    HAVING COUNT(*) >= 3
    ORDER BY OP_CARRIER, route_rank_by_carrier
""")

print("Route Rankings by Carrier (Window Functions):")
ranking_analysis.show(20, truncate=False)

# Pattern 2: Aggregations with Multiple Grouping Sets
if record_count > 0:
    grouping_analysis = spark.sql("""
        SELECT 
            COALESCE(OP_CARRIER, 'ALL_CARRIERS') as carrier,
            COALESCE(ORIGIN, 'ALL_ORIGINS') as origin,
            COUNT(*) as flight_count,
            AVG(DISTANCE) as avg_distance,
            SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) as cancelled_flights
        FROM flight_data_tbl
        GROUP BY OP_CARRIER, ORIGIN WITH ROLLUP
        ORDER BY 
            CASE WHEN OP_CARRIER IS NULL THEN 1 ELSE 0 END,
            OP_CARRIER,
            CASE WHEN ORIGIN IS NULL THEN 1 ELSE 0 END,
            flight_count DESC
    """)
    
    print("Hierarchical Aggregations (ROLLUP):")
    grouping_analysis.show(30, truncate=False)

# Pattern 3: Conditional Aggregations
conditional_analysis = spark.sql("""
    SELECT 
        OP_CARRIER,
        COUNT(*) as total_flights,
        COUNT(CASE WHEN DISTANCE < 500 THEN 1 END) as short_haul,
        COUNT(CASE WHEN DISTANCE BETWEEN 500 AND 1500 THEN 1 END) as medium_haul,
        COUNT(CASE WHEN DISTANCE > 1500 THEN 1 END) as long_haul,
        ROUND(AVG(CASE WHEN CANCELLED = 0 THEN DISTANCE END), 0) as avg_distance_completed
    FROM flight_data_tbl
    GROUP BY OP_CARRIER
    ORDER BY total_flights DESC
""")

print("Flight Distance Categories by Carrier:")
conditional_analysis.show(truncate=False)



# --------------------------------------------
# 6. DataFrame API Integration with SQL Results
# --------------------------------------------
print("\n=== DataFrame API Integration ===")

# Convert SQL results to DataFrame for further processing
carrier_stats = carrier_analysis.filter(col("total_flights") >= 50)

print("Carriers with 50+ flights (DataFrame API filtering):")
carrier_stats.show()

# Combine SQL and DataFrame operations
hybrid_analysis = DF.filter(col("CANCELLED") == 0) \
    .groupBy("OP_CARRIER") \
    .agg(
        count("*").alias("completed_flights"),
        avg("DISTANCE").alias("avg_distance"),
        spark_max("DISTANCE").alias("max_distance")
    ) \
    .orderBy(desc("completed_flights"))

print("Completed Flights Analysis (DataFrame API):")
hybrid_analysis.show()

# --------------------------------------------
# 7. Performance Analysis and Optimization
# --------------------------------------------
print("\n=== Performance Analysis ===")

# Analyze query execution patterns
print("Performance characteristics:")
performance_notes = [
    f"• Reading from managed table leverages Spark catalog optimizations",
    f"• Table statistics enable cost-based optimization",
    f"• Bucketing (if configured) eliminates shuffles for carrier-based queries",
    f"• Columnar Parquet format enables predicate pushdown",
    f"• Current partitions: {DF.rdd.getNumPartitions()} (affects parallelism)"
]

for note in performance_notes:
    print(note)

# Show execution plan for complex query
print("\nQuery execution plan sample:")
try:
    carrier_analysis.explain(True)
except:
    print("Execution plan display not available in this environment")


# --------------------------------------------
# 8. Data Quality and Validation
# --------------------------------------------
print("\n=== Data Quality Validation ===")

# Data quality checks using SQL
quality_checks = spark.sql("""
    SELECT 
        'Total Records' as metric,
        COUNT(*) as value
    FROM flight_data_tbl
    
    UNION ALL
    
    SELECT 
        'Null OP_CARRIER' as metric,
        COUNT(*) as value
    FROM flight_data_tbl
    WHERE OP_CARRIER IS NULL
    
    UNION ALL
    
    SELECT 
        'Invalid Distances' as metric,
        COUNT(*) as value
    FROM flight_data_tbl
    WHERE DISTANCE <= 0 OR DISTANCE > 10000
    
    UNION ALL
    
    SELECT 
        'Same Origin/Destination' as metric,
        COUNT(*) as value
    FROM flight_data_tbl
    WHERE ORIGIN = DEST
""")

print("Data Quality Report:")
quality_checks.show(truncate=False)

# Check for data completeness
completeness_check = spark.sql("""
    SELECT 
        'FL_DATE' as column_name,
        COUNT(*) as total_records,
        COUNT(FL_DATE) as non_null_records,
        ROUND((COUNT(FL_DATE) * 100.0 / COUNT(*)), 2) as completeness_pct
    FROM flight_data_tbl
    
    UNION ALL
    
    SELECT 
        'OP_CARRIER' as column_name,
        COUNT(*) as total_records,
        COUNT(OP_CARRIER) as non_null_records,
        ROUND((COUNT(OP_CARRIER) * 100.0 / COUNT(*)), 2) as completeness_pct
    FROM flight_data_tbl
    
    UNION ALL
    
    SELECT 
        'DISTANCE' as column_name,
        COUNT(*) as total_records,
        COUNT(DISTANCE) as non_null_records,
        ROUND((COUNT(DISTANCE) * 100.0 / COUNT(*)), 2) as completeness_pct
    FROM flight_data_tbl
""")

print("Data Completeness Report:")
completeness_check.show(truncate=False)

# --------------------------------------------
# 9. Export and Persistence Patterns
# --------------------------------------------
print("\n=== Export and Persistence Patterns ===")

# Create views for common analytics queries
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW carrier_performance AS
    SELECT 
        OP_CARRIER,
        COUNT(*) as total_flights,
        AVG(DISTANCE) as avg_distance,
        SUM(CASE WHEN CANCELLED = 1 THEN 1 ELSE 0 END) as cancelled_flights,
        ROUND(AVG(CASE WHEN CANCELLED = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) as cancellation_rate
    FROM flight_data_tbl
    GROUP BY OP_CARRIER
""")

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW route_summary AS
    SELECT 
        ORIGIN,
        DEST,
        COUNT(*) as flight_count,
        COUNT(DISTINCT OP_CARRIER) as carriers_count,
        AVG(DISTANCE) as avg_distance
    FROM flight_data_tbl
    GROUP BY ORIGIN, DEST
""")

print("Created temporary views:")
print("  - carrier_performance")
print("  - route_summary")

# Demonstrate view usage
print("\nUsing created views:")
spark.sql("SELECT * FROM carrier_performance ORDER BY total_flights DESC").show(5)

# --------------------------------------------
# 10. Best Practices and Recommendations
# --------------------------------------------
print("\n=== Best Practices for Table-Based Analytics ===")

best_practices = [
    "✓ Use SQL for complex analytics that benefit from declarative syntax",
    "✓ Combine DataFrame API and SQL based on operation complexity",
    "✓ Create temporary views for reusable query patterns",
    "✓ Leverage managed tables for persistent, cross-session analytics",
    "✓ Implement data quality checks as part of analytics workflows",
    "✓ Use window functions for ranking and comparative analytics",
    "✓ Monitor query execution plans for performance optimization",
    "✓ Cache frequently accessed tables and intermediate results",
    "✓ Document complex queries with comments for team collaboration"
]

for practice in best_practices:
    print(practice)

print("\n=== Final Output (Original Simple Display) ===")
# Original basic output as requested
DF.show(truncate=False)

print("\nAnalysis complete. Managed table provides persistent, optimized access to flight data.")

# --------------------------------------------
# 11. Cleanup
# --------------------------------------------
spark.stop()

# ============================================================================
# SUMMARY: Hive Table Analytics demonstrating:
# - Reading from managed tables with Spark SQL
# - Advanced SQL analytics patterns and window functions  
# - Data quality validation and completeness checking
# - Performance optimization through catalog integration
# - Hybrid SQL + DataFrame API workflows
# - Temporary view creation for reusable analytics
# - Best practices for table-based data processing
# ============================================================================