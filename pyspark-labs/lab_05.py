# ============================================================================
# PySpark Example 5: JDBC Database Integration - MySQL Connectivity
# ============================================================================
# INTENT: Demonstrate enterprise database integration with JDBC connectivity
# CONCEPTS: External dependencies, JDBC options, database security, connection pooling

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, avg, max as spark_max, min as spark_min

# --------------------------------------------
# 1. Spark Session with External Dependencies
# --------------------------------------------
# Note: MySQL JDBC driver must be downloaded at runtime or added to classpath
spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("mysql-integration") \
    .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.3.0") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Alternative: Pre-downloaded JAR (for production environments)
# .config("spark.jars", "/path/to/mysql-connector-j-8.3.0.jar") \
    
print("=== MySQL JDBC Integration Demo ===")



# --------------------------------------------
# 2. Database Connection Configuration
# --------------------------------------------
# JDBC URL components explained:
# jdbc:mysql://hostname:port/database_name
# - hostname: Database server address (localhost for local development)
# - port: MySQL default port 3306
# - database_name: Target database schema

# Connection properties (centralized for reusability)
db_config = {
    "url": "jdbc:mysql://localhost:3306/todosdb",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "root",
    "password": "root1234"
}
# Additional JDBC options for production environments
jdbc_options = {
    **db_config,
    "fetchsize": "1000",                    # Rows to fetch per round trip
    "batchsize": "1000",                    # Batch size for writes
    "isolationLevel": "READ_COMMITTED",     # Transaction isolation
    "sessionInitStatement": "SET time_zone = '+00:00'"  # UTC timezone
}



# --------------------------------------------
# 3. Data Ingestion from MySQL
# --------------------------------------------
try:
    print("Connecting to MySQL database...")
    
    # Basic table read
    todosDF = spark.read \
        .format("jdbc") \
        .options(**jdbc_options) \
        .option("dbtable", "todos") \
        .load()
    
    print("✓ Successfully connected to MySQL")
    print("\nTodos Table Schema:")
    todosDF.printSchema()
    
    print(f"Total Records: {todosDF.count()}")
    print("\nSample Data:")
    todosDF.show(5, truncate=False)
    
except Exception as e:
    print(f"✗ Database connection failed: {e}")
    print("\nTroubleshooting checklist:")
    print("1. Is MySQL server running on localhost:3306?")
    print("2. Does 'todosdb' database exist?")
    print("3. Are credentials (root/root1234) correct?")
    print("4. Is 'todos' table present in the database?")
    print("5. Does the user have SELECT permissions?")
    

# --------------------------------------------
# 4. Advanced JDBC Reading Patterns
# --------------------------------------------
print("\n=== Advanced JDBC Reading Patterns ===")

# Pattern 1: Custom SQL Query (instead of table name)
try:
    # Use custom query for complex data extraction
    custom_query = """
    (SELECT 
        user_id,
        COUNT(*) as total_todos,
        SUM(CASE WHEN completed = 1 THEN 1 ELSE 0 END) as completed_todos,
        ROUND(AVG(CASE WHEN completed = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) as completion_rate
     FROM todos 
     GROUP BY user_id) as user_stats
    """
    
    user_stats_df = spark.read \
        .format("jdbc") \
        .options(**jdbc_options) \
        .option("dbtable", custom_query) \
        .load()
    
    print("User Statistics from Custom Query:")
    user_stats_df.show()
    
except Exception as e:
    print(f"Custom query failed: {e}")
    print("Computing statistics using DataFrame API instead...")
    

# Pattern 2: Partitioned Reading for Large Tables
print("\n=== Partitioned Reading Strategy ===")
try:
    # For large tables, partition reads for better parallelism
    partitioned_read_df = spark.read \
        .format("jdbc") \
        .options(**jdbc_options) \
        .option("dbtable", "todos") \
        .option("partitionColumn", "user_id") \
        .option("lowerBound", "1") \
        .option("upperBound", "100") \
        .option("numPartitions", "4") \
        .load()
    
    print(f"Partitioned read - Number of partitions: {partitioned_read_df.rdd.getNumPartitions()}")
    
except Exception as e:
    print(f"Partitioned reading not applicable: {e}")
    print("Note: Partitioning requires numeric partition column with known bounds")   
    
    

# --------------------------------------------
# 5. Data Transformation and Analysis
# --------------------------------------------
print("\n=== Data Analysis ===")

# Analyze todo completion patterns
analysis_results = {
    "total_todos": todosDF.count(),
    "completed_todos": todosDF.filter(col("completed") == True).count(),
    "pending_todos": todosDF.filter(col("completed") == False).count(),
    "unique_users": todosDF.select("user_id").distinct().count()
}

print("Todo Analysis Results:")
for metric, value in analysis_results.items():
    print(f"  {metric.replace('_', ' ').title()}: {value}")

# User performance ranking
user_performance = todosDF.groupBy("user_id") \
    .agg(
        count("*").alias("total_tasks"),
        count(when(col("completed") == True, 1)).alias("completed_tasks")
    ) \
    .withColumn("completion_rate", 
               (col("completed_tasks") / col("total_tasks") * 100).cast("decimal(5,2)")) \
    .orderBy(col("completion_rate").desc(), col("total_tasks").desc())

print("\nUser Performance Ranking:")
user_performance.show()    



# --------------------------------------------
# 6. Writing Back to Database (Demonstration)
# --------------------------------------------
print("\n=== Database Write Operations ===")

# Create a summary table for writing back
summary_df = todosDF.groupBy("user_id") \
    .agg(
        count("*").alias("total_todos"),
        count(when(col("completed") == True, 1)).alias("completed_todos"),
        count(when(col("completed") == False, 1)).alias("pending_todos")
    )

print("Summary data to write:")
summary_df.show()

# Write modes for JDBC
write_modes = {
    "append": "Add new records to existing table",
    "overwrite": "Replace entire table contents", 
    "ignore": "Skip write if table already exists",
    "error": "Fail if table already exists (default)"
}

print("Available JDBC write modes:")
for mode, description in write_modes.items():
    print(f"  {mode}: {description}")

# Demonstration of write operation (commented to avoid accidental execution)
print("\nWrite operation example (not executed):")
print("""
summary_df.write \
    .format("jdbc") \
    .options(**jdbc_options) \
    .option("dbtable", "user_todo_summary") \
    .mode("overwrite") \
    .save()
""")

# Alternative: Save as temporary view for SQL access
summary_df.createOrReplaceTempView("user_summary")
print("\nCreated temporary view 'user_summary' for SQL queries")

# Example SQL query on the summary
result = spark.sql("""
    SELECT 
        user_id,
        total_todos,
        completed_todos,
        ROUND(completed_todos * 100.0 / total_todos, 2) as completion_percentage
    FROM user_summary
    WHERE total_todos > 1
    ORDER BY completion_percentage DESC
""")

print("SQL query results:")
result.show()
