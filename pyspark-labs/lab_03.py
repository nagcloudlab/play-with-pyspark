

# ============================================================================
# PySpark Example 3: DataFrame API vs SQL API Comparison
# ============================================================================
# INTENT: Demonstrate two approaches for the same transformation - DataFrame API vs SQL
# CONCEPTS: Temporary views, SQL queries, method chaining, equivalent operations

from pyspark.sql import SparkSession
from pyspark import SparkConf

# --------------------------------------------
# 1. Spark Session Setup
# --------------------------------------------
conf = SparkConf().setAppName("pyspark-app").setMaster("local[3]")

spark = SparkSession \
    .builder \
    .config(conf=conf) \
    .getOrCreate()

# --------------------------------------------
# 2. Data Ingestion - Simplified CSV Reader
# --------------------------------------------
# Alternative syntax for reading CSV with options
survey_df = spark.read.csv(
    "./source/survey.csv", 
    header=True,           # Equivalent to .option("header", "true")
    inferSchema=True       # Equivalent to .option("inferSchema", "true")
)

# --------------------------------------------
# 3. Repartitioning for Performance
# --------------------------------------------
survey_df = survey_df.repartition(2)


# --------------------------------------------
# 4. Data Transformation - Two Equivalent Approaches
# --------------------------------------------

# APPROACH 1: DataFrame API (Functional/Method Chaining)
# Commented out to show SQL approach - uncomment to use
# country_count_df = survey_df \
#     .select("Age", "Country") \
#     .where("Age < 40") \
#     .groupBy("Country") \
#     .count()

# APPROACH 2: SQL API (ANSI SQL)
# Step 1: Create temporary view for SQL queries
# This view exists only for the current SparkSession lifetime
survey_df.createOrReplaceTempView("survey_tbl")


# Step 2: Execute SQL query on the temporary view
country_count_df = spark.sql("""
    SELECT Country, COUNT(*) as count 
    FROM survey_tbl 
    WHERE Age < 40 
    GROUP BY Country
""")


# --------------------------------------------
# 5. Data Output and Results
# --------------------------------------------
# Display results in console
country_count_df.show()

# Alternative: Write to file (commented out)
# country_count_df.write.csv("output/country_count", mode="overwrite", header=True)



# --------------------------------------------
# 6. Session Management
# --------------------------------------------
input("Press Enter to continue...")  # Keep session alive for exploration
spark.stop()  # Clean shutdown



# ============================================================================
# COMPARISON: DataFrame API vs SQL API
# ============================================================================
# 
# DataFrame API:
# + More Pythonic, method chaining
# + Better IDE support (autocomplete, type hints)
# + Compile-time error checking
# - Requires learning DataFrame methods
# 
# SQL API:
# + Familiar to SQL developers
# + Complex queries often more readable
# + Standard ANSI SQL syntax
# - Runtime error detection only
# - Requires temporary view creation
#
# BEST PRACTICE: Choose based on team expertise and query complexity
# Both approaches generate identical execution plans and performance
# ============================================================================