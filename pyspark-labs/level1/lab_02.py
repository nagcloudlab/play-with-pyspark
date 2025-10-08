# ============================================================================
# PySpark Example 1: Basic ETL Pipeline with DataFrame Operations
# ============================================================================
# INTENT: Demonstrate fundamental PySpark workflow - read CSV, transform data, write output
# CONCEPTS: SparkSession setup, partitioning, DataFrame API, write modes

from pyspark.sql import SparkSession
from pyspark import SparkConf

# --------------------------------------------
# 1. Spark Session Setup
# --------------------------------------------
conf = SparkConf() \
    .setAppName("app1") \
    .setMaster("local[3]")       # Use 3 CPU cores locally
    
spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .getOrCreate()

# --------------------------------------------
# 2. Data Ingestion
# --------------------------------------------
# Read mental health survey CSV with schema inference
survey_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("./source/survey.csv") 

# Optional: Check data structure
# survey_df.printSchema()
# survey_df.show()

# --------------------------------------------
# 3. Partitioning Management
# --------------------------------------------
# Check initial partitions (affects parallelism)
# print(survey_df.rdd.getNumPartitions())

# Repartition to 2 partitions for better resource utilization
survey_df = survey_df.repartition(2)

# Verify partitioning and data distribution
# print(survey_df.rdd.getNumPartitions())
# print(survey_df.rdd.glom().map(len).collect())  # Records per partition

# --------------------------------------------
# 4. Data Transformation (Lazy Operations)
# --------------------------------------------
# Filter young respondents and count by country
result_df = survey_df \
    .select("Age", "Country") \
    .where("Age < 40") \
    .groupBy("Country") \
    .count()

# Alternative: SQL approach
# survey_df.createOrReplaceTempView("survey")
# result_df = spark.sql("SELECT Country, COUNT(*) as count FROM survey WHERE Age < 40 GROUP BY Country")

# --------------------------------------------
# 5. Data Output (Actions - trigger computation)
# --------------------------------------------
# Option 1: Display results
# result_df.show()

# Option 2: Collect to Python list (avoid for large data)
# python_list = result_df.collect()
# for row in python_list:
#     print(row["Country"], row["count"])

# Option 3: Write to file (recommended for production)
result_df.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save("./sink/survey_count_by_country_df")

# --------------------------------------------
# 6. Cleanup
# --------------------------------------------
# input("Press Enter to continue...")
spark.stop()  # Always release resources

# ============================================================================
# SUMMARY: Complete ETL pipeline demonstrating:
# - Spark configuration and session management
# - CSV data ingestion with schema inference  
# - Partitioning for performance optimization
# - DataFrame transformations (select, filter, groupBy, count)
# - Multiple output options (show, collect, write)
# ============================================================================