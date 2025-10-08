

# ============================================================================
# PySpark Example 12: DataFrame Functions & Data Transformation Pipeline
# ============================================================================
# INTENT: Demonstrate comprehensive DataFrame transformation techniques
# CONCEPTS: Built-in functions, type casting, conditional logic, date handling, deduplication

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, expr, concat, lit, monotonically_increasing_id, 
    to_date, year, month, dayofmonth, current_date, datediff,
    upper, lower, trim, length, regexp_replace, split,
    count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    row_number, rank, dense_rank, lag, lead
)
from pyspark.sql.types import IntegerType, DateType, StringType
from pyspark.sql.window import Window


# --------------------------------------------
# 1. Spark Session Setup
# --------------------------------------------
spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("dataframe-functions-demo") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=== DataFrame Functions & Data Transformation Pipeline ===")


# --------------------------------------------
# 2. Sample Data Creation and Analysis
# --------------------------------------------
print("\n=== Sample Data Creation ===")

# Your original data with analysis
data_list = [
    ("Ravi", "28", "1", "2002"),
    ("Abdul", "23", "5", "81"),   # 1981
    ("John", "12", "12", "6"),    # 2006
    ("Rosy", "7", "8", "63"),     # 1963
    ("Abdul", "23", "5", "81")    # 1981 (duplicate)
]

# Create DataFrame with explicit partitioning
raw_df = spark.createDataFrame(data_list) \
    .toDF("name", "day", "month", "year") \
    .repartition(3)

print("Original data schema:")
raw_df.printSchema()

print("Original raw data:")
raw_df.show(truncate=False)

print(f"Partitions: {raw_df.rdd.getNumPartitions()}")
print(f"Total records: {raw_df.count()}")

# Analyze data patterns before transformation
print("\nData pattern analysis:")
print("Year values (before standardization):")
raw_df.select("year").distinct().orderBy("year").show()

print("Name patterns:")
raw_df.select("name").distinct().orderBy("name").show()

# Check for duplicates
duplicate_analysis = raw_df.groupBy("name", "day", "month", "year").count().filter(col("count") > 1)
print("Duplicate records:")
duplicate_analysis.show()



# --------------------------------------------
# 3. Step-by-Step Transformation Pipeline
# --------------------------------------------
print("\n=== Step-by-Step Transformation Pipeline ===")

# Step 1: Add unique identifier
print("Step 1: Adding unique identifiers")
step1_df = raw_df.withColumn("id", monotonically_increasing_id())
step1_df.select("id", "name", "day", "month", "year").show()

# Step 2: Type casting with validation
print("Step 2: Type casting and validation")
step2_df = step1_df \
    .withColumn("day", col("day").cast(IntegerType())) \
    .withColumn("month", col("month").cast(IntegerType())) \
    .withColumn("year", col("year").cast(IntegerType()))

print("After type casting:")
step2_df.printSchema()
step2_df.show()



# Step 3: Year standardization logic
print("Step 3: Year standardization with conditional logic")
step3_df = step2_df.withColumn("year_original", col("year")) \
    .withColumn("year",
        when(col("year") < 20, col("year") + 2000)      # 0-19 → 2000-2019
        .when(col("year") < 100, col("year") + 1900)    # 20-99 → 1920-1999
        .otherwise(col("year"))                          # 100+ → unchanged
    )

print("Year standardization results:")
step3_df.select("name", "year_original", "year").show()

# Step 4: Date construction and validation
print("Step 4: Date construction with validation")
step4_df = step3_df \
    .withColumn("date_string", concat(col("day"), lit("/"), col("month"), lit("/"), col("year"))) \
    .withColumn("dob", expr("to_date(concat(day, '/', month, '/', year), 'd/M/y')"))

print("Date construction results:")
step4_df.select("name", "date_string", "dob").show()

# Validate date parsing success
invalid_dates = step4_df.filter(col("dob").isNull()).count()
print(f"Invalid dates after parsing: {invalid_dates}")

# Step 5: Column cleanup
print("Step 5: Column cleanup and organization")
step5_df = step4_df.drop("day", "month", "year_original", "date_string")
step5_df.show()


# Step 6: Deduplication strategy
print("Step 6: Deduplication analysis and removal")
print("Before deduplication:")
step5_df.groupBy("name", "dob").count().show()

step6_df = step5_df.dropDuplicates(["name", "dob"])
print("After deduplication:")
step6_df.show()

# Step 7: Final sorting and presentation
print("Step 7: Final sorting by date of birth")
final_df = step6_df.sort(col("dob").desc())

print("Final result:")
final_df.show(truncate=False)


# --------------------------------------------
# 4. Advanced DataFrame Functions Showcase
# --------------------------------------------
print("\n=== Advanced DataFrame Functions Showcase ===")

# String manipulation functions
print("String manipulation functions:")
string_functions_df = final_df \
    .withColumn("name_upper", upper(col("name"))) \
    .withColumn("name_lower", lower(col("name"))) \
    .withColumn("name_length", length(col("name"))) \
    .withColumn("name_trimmed", trim(col("name"))) \
    .withColumn("name_initials", expr("substring(name, 1, 1)"))

string_functions_df.select("name", "name_upper", "name_lower", "name_length", "name_initials").show()

# Date manipulation functions
print("Date manipulation functions:")
date_functions_df = final_df \
    .withColumn("birth_year", year(col("dob"))) \
    .withColumn("birth_month", month(col("dob"))) \
    .withColumn("birth_day", dayofmonth(col("dob"))) \
    .withColumn("current_date", current_date()) \
    .withColumn("age_days", datediff(current_date(), col("dob"))) \
    .withColumn("age_years", expr("floor(datediff(current_date(), dob) / 365.25)"))

date_functions_df.select("name", "dob", "birth_year", "birth_month", "age_years").show()

# Conditional logic functions
print("Complex conditional logic:")
conditional_df = final_df \
    .withColumn("generation",
        when(year(col("dob")) >= 1997, "Gen Z")
        .when(year(col("dob")) >= 1981, "Millennial") 
        .when(year(col("dob")) >= 1965, "Gen X")
        .when(year(col("dob")) >= 1946, "Baby Boomer")
        .otherwise("Silent Generation")
    ) \
    .withColumn("life_stage",
        when(col("age_years") < 18, "Minor")
        .when(col("age_years") < 30, "Young Adult")
        .when(col("age_years") < 50, "Adult")
        .when(col("age_years") < 65, "Middle-aged")
        .otherwise("Senior")
    )

conditional_df.select("name", "dob", "age_years", "generation", "life_stage").show(truncate=False)

# --------------------------------------------
# 5. Window Functions and Analytics
# --------------------------------------------
print("\n=== Window Functions and Analytics ===")

# Create window specifications
age_window = Window.orderBy(col("age_years").desc())
name_window = Window.partitionBy("generation").orderBy(col("age_years").desc())

# Apply window functions
window_df = conditional_df \
    .withColumn("age_rank", row_number().over(age_window)) \
    .withColumn("age_dense_rank", dense_rank().over(age_window)) \
    .withColumn("generation_rank", row_number().over(name_window)) \
    .withColumn("next_younger", lead("name").over(age_window)) \
    .withColumn("next_older", lag("name").over(age_window))

print("Window function results:")
window_df.select("name", "age_years", "generation", "age_rank", "generation_rank", "next_younger").show(truncate=False)

# --------------------------------------------
# 6. Aggregation Functions
# --------------------------------------------
print("\n=== Aggregation Functions ===")

# Basic aggregations
basic_agg = final_df \
    .withColumn("age_years", expr("floor(datediff(current_date(), dob) / 365.25)")) \
    .agg(
        count("*").alias("total_people"),
        avg("age_years").alias("avg_age"),
        spark_min("age_years").alias("min_age"),
        spark_max("age_years").alias("max_age"),
        spark_min("dob").alias("earliest_birth"),
        spark_max("dob").alias("latest_birth")
    )

print("Basic aggregation results:")
basic_agg.show(truncate=False)

# Grouped aggregations
grouped_agg = conditional_df.groupBy("generation") \
    .agg(
        count("*").alias("count"),
        avg("age_years").alias("avg_age"),
        spark_min("age_years").alias("min_age"),
        spark_max("age_years").alias("max_age")
    ) \
    .orderBy("avg_age")

print("Generation-based aggregation:")
grouped_agg.show(truncate=False)

# --------------------------------------------
# 7. Data Quality and Validation Functions
# --------------------------------------------
print("\n=== Data Quality and Validation ===")

# Data completeness analysis
completeness_df = final_df \
    .withColumn("has_id", when(col("id").isNotNull(), 1).otherwise(0)) \
    .withColumn("has_name", when(col("name").isNotNull() & (col("name") != ""), 1).otherwise(0)) \
    .withColumn("has_dob", when(col("dob").isNotNull(), 1).otherwise(0)) \
    .withColumn("completeness_score", 
        (col("has_id") + col("has_name") + col("has_dob")) * 100 / 3)

print("Data completeness analysis:")
completeness_df.select("name", "has_id", "has_name", "has_dob", "completeness_score").show()

# Data validation rules
validation_df = final_df \
    .withColumn("age_years", expr("floor(datediff(current_date(), dob) / 365.25)")) \
    .withColumn("validation_status",
        when((col("age_years") < 0) | (col("age_years") > 150), "Invalid Age")
        .when(col("name").isNull() | (length(col("name")) < 2), "Invalid Name")
        .when(col("dob").isNull(), "Missing DOB")
        .otherwise("Valid")
    )

print("Data validation results:")
validation_summary = validation_df.groupBy("validation_status").count().orderBy("count", ascending=False)
validation_summary.show()

# --------------------------------------------
# 8. Performance Optimization Techniques
# --------------------------------------------
print("\n=== Performance Optimization Techniques ===")

# Caching for repeated operations
cached_df = final_df.cache()
print(f"DataFrame cached: {cached_df.is_cached}")

# Partition analysis
print(f"Current partitions: {final_df.rdd.getNumPartitions()}")
print("Records per partition:")
partition_distribution = final_df.withColumn("partition_id", expr("spark_partition_id()")) \
    .groupBy("partition_id").count().orderBy("partition_id")
partition_distribution.show()

# Repartitioning strategies
print("Repartitioning strategies:")
strategies = [
    ("Current", final_df.rdd.getNumPartitions()),
    ("Coalesced", final_df.coalesce(1).rdd.getNumPartitions()),
    ("Repartitioned", final_df.repartition(2).rdd.getNumPartitions())
]

for strategy, partitions in strategies:
    print(f"  {strategy}: {partitions} partitions")

# --------------------------------------------
# 9. Function Chaining and Pipeline Optimization
# --------------------------------------------
print("\n=== Optimized Pipeline (All Steps Combined) ===")

# Your original pipeline optimized with additional features
optimized_pipeline = raw_df \
    .withColumn("id", monotonically_increasing_id()) \
    .withColumn("day", col("day").cast(IntegerType())) \
    .withColumn("month", col("month").cast(IntegerType())) \
    .withColumn("year", col("year").cast(IntegerType())) \
    .withColumn("year",
        when(col("year") < 20, col("year") + 2000)
        .when(col("year") < 100, col("year") + 1900)
        .otherwise(col("year"))
    ) \
    .withColumn("dob", expr("to_date(concat(day, '/', month, '/', year), 'd/M/y')")) \
    .drop("day", "month", "year") \
    .dropDuplicates(["name", "dob"]) \
    .withColumn("age_years", expr("floor(datediff(current_date(), dob) / 365.25)")) \
    .withColumn("generation",
        when(year(col("dob")) >= 1997, "Gen Z")
        .when(year(col("dob")) >= 1981, "Millennial")
        .when(year(col("dob")) >= 1965, "Gen X")
        .when(year(col("dob")) >= 1946, "Baby Boomer")
        .otherwise("Silent Generation")
    ) \
    .sort(col("dob").desc())

print("Optimized pipeline results:")
optimized_pipeline.show(truncate=False)

# --------------------------------------------
# 10. Best Practices and Function Categories
# --------------------------------------------
print("\n=== DataFrame Functions Best Practices ===")

function_categories = {
    "Column Operations": ["col()", "when()", "otherwise()", "cast()", "alias()"],
    "String Functions": ["upper()", "lower()", "trim()", "length()", "concat()", "split()"],
    "Date Functions": ["to_date()", "year()", "month()", "current_date()", "datediff()"],
    "Math Functions": ["abs()", "round()", "floor()", "ceil()", "sqrt()", "pow()"],
    "Conditional Logic": ["when()", "otherwise()", "case", "if()", "coalesce()"],
    "Aggregation Functions": ["count()", "sum()", "avg()", "min()", "max()", "stddev()"],
    "Window Functions": ["row_number()", "rank()", "dense_rank()", "lag()", "lead()"],
    "Array Functions": ["array()", "explode()", "array_contains()", "size()"],
    "JSON Functions": ["get_json_object()", "json_tuple()", "from_json()", "to_json()"],
    "Utility Functions": ["monotonically_increasing_id()", "spark_partition_id()", "hash()"]
}

print("Function categories and examples:")
for category, functions in function_categories.items():
    print(f"  {category}: {', '.join(functions[:3])}...")

best_practices = [
    "✓ Use built-in functions instead of UDFs when possible for performance",
    "✓ Chain operations efficiently to minimize DataFrame passes",
    "✓ Use explicit type casting to avoid runtime errors",
    "✓ Handle null values appropriately with when() and otherwise()",
    "✓ Cache DataFrames when used in multiple operations",
    "✓ Use meaningful column aliases for readability",
    "✓ Validate data transformations with sample checks",
    "✓ Consider partitioning impact on performance",
    "✓ Use window functions for complex analytical operations",
    "✓ Test edge cases and boundary conditions"
]

print("\nBest practices:")
for practice in best_practices:
    print(practice)

print("\n=== Final Result (Your Original Code) ===")
# Execute your original transformation for comparison
your_result = raw_df \
    .withColumn("id", monotonically_increasing_id()) \
    .withColumn("day", col("day").cast(IntegerType())) \
    .withColumn("month", col("month").cast(IntegerType())) \
    .withColumn("year", col("year").cast(IntegerType())) \
    .withColumn("year",
        when(col("year") < 20, col("year") + 2000)
        .when(col("year") < 100, col("year") + 1900)
        .otherwise(col("year"))
    ) \
    .withColumn("dob", expr("to_date(concat(day, '/', month, '/', year), 'd/M/y')")) \
    .drop("day", "month", "year") \
    .dropDuplicates(["name", "dob"]) \
    .sort(col("dob").desc())

your_result.show()

# --------------------------------------------
# 11. Cleanup
# --------------------------------------------
spark.stop()

# ============================================================================
# SUMMARY: DataFrame Functions demonstrating:
# - Comprehensive built-in function usage across all categories
# - Type casting and data validation techniques
# - Conditional logic and complex transformations
# - Date manipulation and standardization patterns
# - Window functions for advanced analytics
# - Performance optimization through caching and partitioning
# - Data quality validation and completeness analysis
# - Best practices for function chaining and pipeline design
# ============================================================================
