

# ============================================================================
# PySpark Example 9: DataFrame Row Operations & Data Type Transformations
# ============================================================================
# INTENT: Demonstrate Row object usage, schema management, and data type conversions
# CONCEPTS: Row construction, schema definition, type casting, date parsing patterns

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_date, col, when, isnan, isnull, date_format, year as year_func, month as month_func, dayofmonth as day_func
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, TimestampType


# --------------------------------------------
# 1. Spark Session Setup
# --------------------------------------------
spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("dataframe-row-operations") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=== DataFrame Row Operations & Data Type Transformations ===")

# --------------------------------------------
# 2. Schema Definition and Row Construction
# --------------------------------------------
print("\n=== Schema Definition Strategies ===")

# Method 1: Explicit Schema Definition (Recommended)
my_schema = StructType([
    StructField("ID", StringType(), nullable=False),
    StructField("EventDate", StringType(), nullable=True)
])

print("Defined schema:")
print(my_schema.simpleString())

# Method 2: Alternative Schema Definition Approaches
print("\nAlternative schema definition methods:")

# DDL String Schema
ddl_schema = "ID STRING NOT NULL, EventDate STRING"
print(f"DDL Schema: {ddl_schema}")

# Programmatic with metadata
detailed_schema = StructType([
    StructField("ID", StringType(), False, {"description": "Unique identifier"}),
    StructField("EventDate", StringType(), True, {"description": "Event occurrence date"})
])


# --------------------------------------------
# 3. Row Object Creation and Patterns
# --------------------------------------------
print("\n=== Row Object Creation Patterns ===")

# Pattern 1: Positional Row Creation (your original approach)
my_rows = [
    Row("123", "04/05/2020"), 
    Row("124", "4/5/2020"),
    Row("125", "04/5/2020"), 
    Row("126", "4/05/2020")
]

print("Original rows (positional):")
for i, row in enumerate(my_rows):
    print(f"  Row {i+1}: {row}")

# Pattern 2: Named Row Creation (more explicit)
named_rows = [
    Row(ID="123", EventDate="04/05/2020"),
    Row(ID="124", EventDate="4/5/2020"),
    Row(ID="125", EventDate="04/5/2020"),
    Row(ID="126", EventDate="4/05/2020")
]

print("\nNamed rows (explicit):")
for i, row in enumerate(named_rows):
    print(f"  Row {i+1}: ID={row.ID}, EventDate={row.EventDate}")

# Pattern 3: Row from Dictionary (useful for dynamic data)
dict_data = [
    {"ID": "123", "EventDate": "04/05/2020"},
    {"ID": "124", "EventDate": "4/5/2020"},
    {"ID": "125", "EventDate": "04/5/2020"},
    {"ID": "126", "EventDate": "4/05/2020"}
]

dict_rows = [Row(**data) for data in dict_data]



# --------------------------------------------
# 4. DataFrame Creation with Different Approaches
# --------------------------------------------
print("\n=== DataFrame Creation Methods ===")

# Method 1: RDD-based approach (your original)
my_rdd = spark.sparkContext.parallelize(my_rows, 2)
my_df = spark.createDataFrame(my_rdd, my_schema)

print("Method 1: RDD-based DataFrame creation")
print(f"  Partitions: {my_df.rdd.getNumPartitions()}")
print("  Schema:")
my_df.printSchema()

# Method 2: Direct DataFrame creation (simpler)
direct_df = spark.createDataFrame(named_rows, my_schema)

print("Method 2: Direct DataFrame creation")
print(f"  Partitions: {direct_df.rdd.getNumPartitions()}")

# Method 3: From dictionary data
dict_df = spark.createDataFrame(dict_rows)

print("Method 3: From dictionary (schema inferred)")
print("  Schema:")
dict_df.printSchema()

# Show original data
print("\nOriginal data with inconsistent date formats:")
my_df.show(truncate=False)

# --------------------------------------------


# --------------------------------------------
# 5. Data Quality Analysis Before Transformation
# --------------------------------------------
print("\n=== Data Quality Analysis ===")

# Analyze date format patterns
date_patterns = my_df.select("EventDate").distinct().collect()
print("Unique date patterns found:")
for row in date_patterns:
    date_val = row["EventDate"]
    print(f"  '{date_val}' - Length: {len(date_val)}, Pattern analysis:")
    
    # Analyze pattern characteristics
    parts = date_val.split("/")
    if len(parts) == 3:
        month, day, year = parts
        print(f"    Month: {month} ({'zero-padded' if len(month) == 2 else 'single digit'})")
        print(f"    Day: {day} ({'zero-padded' if len(day) == 2 else 'single digit'})")
        print(f"    Year: {year}")

# Check for potential parsing issues
print("\nPotential parsing challenges:")
challenges = [
    "• Mixed zero-padding: '04/05' vs '4/5'",
    "• Single vs double digit months/days",
    "• Need for flexible date format: 'M/d/y' handles both patterns",
    "• Year format: 'y' vs 'yyyy' considerations"
]

for challenge in challenges:
    print(challenge)



# --------------------------------------------
# 6. Date Transformation with Error Handling
# --------------------------------------------
print("\n=== Date Transformation Process ===")

# Primary transformation using flexible format
print("Applying date transformation with format 'M/d/y':")
new_df = my_df.withColumn("EventDate", to_date(col("EventDate"), "M/d/y"))

print("Transformed schema:")
new_df.printSchema()

print("Transformed data:")
new_df.show(truncate=False)

# Verify transformation success
print("\nTransformation validation:")
null_dates = new_df.filter(col("EventDate").isNull()).count()
total_records = new_df.count()
success_rate = ((total_records - null_dates) / total_records) * 100

print(f"  Total records: {total_records}")
print(f"  Successfully parsed: {total_records - null_dates}")
print(f"  Failed to parse: {null_dates}")
print(f"  Success rate: {success_rate:.1f}%")



# --------------------------------------------
# 7. Advanced Date Transformation Patterns
# --------------------------------------------
print("\n=== Advanced Date Transformation Patterns ===")

# Pattern 1: Multiple format handling
multi_format_df = my_df.withColumn(
    "EventDate_Parsed",
    when(to_date(col("EventDate"), "M/d/yyyy").isNotNull(), 
         to_date(col("EventDate"), "M/d/yyyy"))
    .when(to_date(col("EventDate"), "MM/dd/yyyy").isNotNull(),
          to_date(col("EventDate"), "MM/dd/yyyy"))
    .when(to_date(col("EventDate"), "M/d/y").isNotNull(),
          to_date(col("EventDate"), "M/d/y"))
    .otherwise(None)
)

print("Multi-format handling approach:")
multi_format_df.select("ID", "EventDate", "EventDate_Parsed").show(truncate=False)

# Pattern 2: Date component extraction
enriched_df = new_df.withColumn("Year", year_func(col("EventDate"))) \
                   .withColumn("Month", month_func(col("EventDate"))) \
                   .withColumn("Day", day_func(col("EventDate"))) \
                   .withColumn("FormattedDate", date_format(col("EventDate"), "yyyy-MM-dd"))

print("Date component extraction:")
enriched_df.show(truncate=False)

# Pattern 3: Business date validations
validated_df = enriched_df.withColumn(
    "DateValidation",
    when(col("EventDate").isNull(), "Invalid")
    .when(col("Year") < 2000, "Too Old")
    .when(col("Year") > 2030, "Future Date")
    .otherwise("Valid")
)

print("Business date validation:")
validated_df.select("ID", "EventDate", "DateValidation").show(truncate=False)
