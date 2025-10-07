# ============================================================================
# PySpark Example 11: User Defined Functions (UDFs) - Custom Data Processing
# ============================================================================
# INTENT: Demonstrate UDF creation, registration, and performance considerations
# CONCEPTS: Custom functions, SQL registration, performance trade-offs, vectorization

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, expr, when, regexp_replace, lower, trim
from pyspark.sql.types import StringType, IntegerType, DoubleType, BooleanType
import re

# UDF 1: Gender parsing with regex (your original function)
def parse_gender(gender):
    """
    Parse inconsistent gender representations into standardized values
    
    Args:
        gender: Raw gender string from survey data
    
    Returns:
        Standardized gender: 'Female', 'Male', or 'Unknown'
    """
    if gender is None:
        return "Unknown"
    
    # Convert to string and handle edge cases
    gender_str = str(gender).strip()
    if not gender_str:
        return "Unknown"
    
    # Define patterns for female and male
    female_pattern = r"^f$|f.m|w.m|female|woman"
    male_pattern = r"^m$|ma|m.l|male|man"
    
    # Case-insensitive matching
    gender_lower = gender_str.lower()
    
    if re.search(female_pattern, gender_lower):
        return "Female"
    elif re.search(male_pattern, gender_lower):
        return "Male"
    else:
        return "Unknown"
    
# UDF 2: Age categorization
def categorize_age(age):
    """Categorize age into life stage groups"""
    if age is None:
        return "Unknown"
    
    try:
        age_int = int(age)
        if age_int < 18:
            return "Minor"
        elif age_int < 25:
            return "Young Adult"
        elif age_int < 35:
            return "Adult"
        elif age_int < 50:
            return "Middle-aged"
        elif age_int < 65:
            return "Senior"
        else:
            return "Elderly"
    except (ValueError, TypeError):
        return "Unknown"

# UDF 3: Email domain extraction
def extract_email_domain(email):
    """Extract domain from email address"""
    if not email or email == "None":
        return "No Email"
    
    try:
        email_str = str(email).strip()
        if "@" in email_str:
            return email_str.split("@")[-1].lower()
        else:
            return "Invalid Email"
    except:
        return "Invalid Email"
    
# UDF 4: Data quality scorer
def calculate_data_quality_score(row_dict):
    """
    Calculate data quality score based on completeness
    This demonstrates a more complex UDF that processes multiple columns
    """
    if not isinstance(row_dict, dict):
        return 0.0
    
    total_fields = len(row_dict)
    filled_fields = sum(1 for value in row_dict.values() 
                       if value is not None and str(value).strip() != "")
    
    return round((filled_fields / total_fields) * 100, 2) if total_fields > 0 else 0.0    


# --------------------------------------------
# 2. Spark Session Setup
# --------------------------------------------
spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("udf-demo") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
    
try:
    # Load survey data
    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("source/survey.csv")
    
    print("Successfully loaded survey data")
    
except Exception as e:
    print(f"Could not load survey data: {e}")
    print("Creating sample survey data for demonstration...")    
    
print("Original data schema:")
survey_df.printSchema()

print(f"Total records: {survey_df.count()}")
print("Sample original data:")
survey_df.show(10, truncate=False)

# Show unique gender values before processing
print("Unique gender values in raw data:")
survey_df.select("Gender").distinct().show()


# --------------------------------------------
# 4. UDF Creation and Registration Methods
# --------------------------------------------
print("\n=== UDF Creation and Registration Methods ===")

print("Method 1: Creating UDF with udf() function")
# Create UDF using udf() function
parse_gender_udf = udf(parse_gender, returnType=StringType())

# Check if UDF is available (this won't show it in catalog as it's not registered)
print("UDF created with udf() function - available for DataFrame operations")

print("\nMethod 2: Registering UDF for SQL usage")
# Register UDF for SQL usage
spark.udf.register("parse_gender_udf", parse_gender, StringType())
spark.udf.register("categorize_age_udf", categorize_age, StringType())
spark.udf.register("extract_email_domain_udf", extract_email_domain, StringType())

# List registered functions (filter for our UDFs)
registered_udfs = [r for r in spark.catalog.listFunctions() if "udf" in r.name.lower()]
print("Registered UDFs:")
for udf_info in registered_udfs:
    print(f"  - {udf_info.name}: {udf_info.description}")    




# --------------------------------------------
# 5. UDF Usage Patterns
# --------------------------------------------
print("\n=== UDF Usage Patterns ===")

print("Pattern 1: Using UDF with DataFrame API")
# Apply UDF using DataFrame API
survey_df_method1 = survey_df.withColumn("Gender_Cleaned", parse_gender_udf("Gender"))

print("Results from DataFrame API UDF:")
survey_df_method1.select("Gender", "Gender_Cleaned").distinct().show()

print("\nPattern 2: Using registered UDF with SQL expression")
# Apply registered UDF using SQL expression
survey_df_method2 = survey_df.withColumn("Gender_Cleaned", expr("parse_gender_udf(Gender)"))

print("Results from SQL-registered UDF:")
survey_df_method2.select("Gender", "Gender_Cleaned").distinct().show()

print("\nPattern 3: Using UDF in pure SQL queries")
# Create temporary view for SQL usage
survey_df.createOrReplaceTempView("survey_data")

sql_result = spark.sql("""
    SELECT 
        Gender,
        parse_gender_udf(Gender) as Gender_Cleaned,
        categorize_age_udf(Age) as Age_Group,
        COUNT(*) as count
    FROM survey_data 
    GROUP BY Gender, Gender_Cleaned, Age_Group
    ORDER BY count DESC
""")

print("Results from pure SQL with UDFs:")
sql_result.show(truncate=False)



# --------------------------------------------
# 6. Multiple UDF Applications
# --------------------------------------------
print("\n=== Multiple UDF Applications ===")

# Apply multiple UDFs to create enriched dataset
enriched_df = survey_df \
    .withColumn("Gender_Cleaned", expr("parse_gender_udf(Gender)")) \
    .withColumn("Age_Group", expr("categorize_age_udf(Age)")) \
    .withColumn("Country_Code", 
                when(col("Country") == "USA", "US")
                .when(col("Country") == "UK", "GB")
                .otherwise(col("Country")))

print("Enriched dataset with multiple UDFs:")
enriched_df.select("Age", "Gender", "Country", "Age_Group", "Gender_Cleaned", "Country_Code").show()

# Group analysis using UDF results
demographic_analysis = enriched_df.groupBy("Gender_Cleaned", "Age_Group") \
    .count() \
    .orderBy("Gender_Cleaned", "Age_Group")

print("Demographic analysis using UDF results:")
demographic_analysis.show()

# --------------------------------------------
# 7. Performance Analysis and Optimization
# --------------------------------------------
print("\n=== Performance Analysis ===")

# Compare UDF vs built-in functions performance
print("Performance comparison: UDF vs Built-in Functions")

# UDF approach (your method)
udf_start_time = spark.sparkContext.getLocalProperty("spark.sql.execution.id")
udf_result = survey_df.withColumn("Gender_UDF", parse_gender_udf("Gender"))
udf_count = udf_result.count()  # Trigger computation

# Built-in function approach (when possible)
builtin_result = survey_df.withColumn("Gender_Builtin",
    when(lower(trim(col("Gender"))).isin(["f", "female", "woman"]), "Female")
    .when(lower(trim(col("Gender"))).isin(["m", "male", "man"]), "Male")
    .otherwise("Unknown")
)
builtin_count = builtin_result.count()  # Trigger computation

print("Performance considerations:")
performance_notes = [
    "• UDFs serialize data between JVM and Python (performance overhead)",
    "• Built-in functions execute entirely in JVM (faster)",
    "• UDFs are not optimized by Catalyst optimizer",
    "• Complex logic may justify UDF overhead for maintainability",
    f"• Records processed: {udf_count} (UDF) vs {builtin_count} (built-in)"
]

for note in performance_notes:
    print(note)

# --------------------------------------------
# 8. UDF Best Practices and Common Pitfalls
# --------------------------------------------
print("\n=== UDF Best Practices ===")

best_practices = [
    "✓ Use UDFs only when built-in functions cannot achieve the logic",
    "✓ Handle null values explicitly in UDF functions",
    "✓ Use appropriate return types to avoid casting overhead",
    "✓ Register UDFs with meaningful names for SQL usage",
    "✓ Consider vectorized UDFs (pandas UDFs) for better performance",
    "✓ Test UDFs thoroughly with edge cases and null data",
    "✓ Document UDF logic clearly for team collaboration",
    "✓ Monitor UDF performance impact on large datasets",
    "✓ Cache DataFrames when applying multiple UDFs",
    "✓ Consider built-in functions first before creating UDFs"
]

for practice in best_practices:
    print(practice)

print("\nCommon pitfalls to avoid:")
pitfalls = [
    "✗ Not handling None/null values in UDF logic",
    "✗ Using UDFs when built-in functions would work",
    "✗ Forgetting to specify return types explicitly", 
    "✗ Creating overly complex UDFs that are hard to test",
    "✗ Not considering serialization overhead for simple operations",
    "✗ Using mutable objects in UDFs (can cause issues)",
    "✗ Not registering UDFs when needed for SQL usage"
]

for pitfall in pitfalls:
    print(pitfall)

# --------------------------------------------
# 9. Advanced UDF Patterns
# --------------------------------------------
print("\n=== Advanced UDF Patterns ===")

# Pattern 1: Conditional UDF application
conditional_udf = when(col("Gender").isNotNull(), parse_gender_udf("Gender")) \
                 .otherwise("No Data")

conditional_result = survey_df.withColumn("Gender_Conditional", conditional_udf)
print("Conditional UDF application:")
conditional_result.select("Gender", "Gender_Conditional").distinct().show()

# Pattern 2: UDF with multiple parameters
def format_survey_response(age, gender, country):
    """Format survey response into readable summary"""
    if not all([age, gender, country]):
        return "Incomplete Response"
    
    return f"{gender} aged {age} from {country}"

# Register multi-parameter UDF
spark.udf.register("format_response_udf", format_survey_response, StringType())

# Apply multi-parameter UDF
formatted_df = survey_df.withColumn(
    "Response_Summary",
    expr("format_response_udf(Age, Gender, Country)")
)

print("Multi-parameter UDF results:")
formatted_df.select("Age", "Gender", "Country", "Response_Summary").show(5, truncate=False)

# Pattern 3: UDF chaining
chained_result = survey_df \
    .withColumn("Gender_Step1", parse_gender_udf("Gender")) \
    .withColumn("Age_Step2", expr("categorize_age_udf(Age)")) \
    .withColumn("Final_Profile", 
                expr("format_response_udf(Age, Gender_Step1, Country)"))

print("UDF chaining example:")
chained_result.select("Gender", "Age", "Country", "Final_Profile").show(5, truncate=False)

# --------------------------------------------
# 10. Data Quality with UDFs
# --------------------------------------------
print("\n=== Data Quality Analysis with UDFs ===")

# Create data quality assessment UDF
def assess_data_quality(gender, age, country):
    """Assess data quality based on field completeness and validity"""
    score = 0
    max_score = 3
    
    # Check gender
    if gender and str(gender).strip():
        score += 1
    
    # Check age
    if age is not None and 0 < age < 120:
        score += 1
        
    # Check country
    if country and str(country).strip() and len(str(country).strip()) > 1:
        score += 1
    
    quality_pct = (score / max_score) * 100
    
    if quality_pct >= 100:
        return "Excellent"
    elif quality_pct >= 75:
        return "Good"
    elif quality_pct >= 50:
        return "Fair"
    else:
        return "Poor"

# Register quality assessment UDF
spark.udf.register("assess_quality_udf", assess_data_quality, StringType())

# Apply data quality assessment
quality_df = survey_df.withColumn(
    "Data_Quality",
    expr("assess_quality_udf(Gender, Age, Country)")
)

print("Data quality assessment:")
quality_summary = quality_df.groupBy("Data_Quality").count().orderBy("count", ascending=False)
quality_summary.show()

# --------------------------------------------
# 11. Final Results and Comparison
# --------------------------------------------
print("\n=== Final Results (Your Original Code Enhanced) ===")

# Your original approach with enhancements
print("Original UDF approach results:")
final_result = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
gender_distribution = final_result.groupBy("Gender").count().orderBy("count", ascending=False)
gender_distribution.show()

# Show before/after comparison
print("Before and after comparison:")
before_after = survey_df.select("Gender").union(
    final_result.select("Gender")
).distinct()
before_after.show()

print("UDF processing complete - gender values have been standardized")

# --------------------------------------------
# 12. Cleanup and Resource Management
# --------------------------------------------
print("\n=== Resource Management ===")

# Show registered UDFs for cleanup reference
print("Registered UDFs that persist in this session:")
current_udfs = [r for r in spark.catalog.listFunctions() if "udf" in r.name.lower()]
for udf_info in current_udfs:
    print(f"  - {udf_info.name}")

print("Note: UDFs remain registered until SparkSession ends")

spark.stop()

# ============================================================================
# SUMMARY: User Defined Functions demonstrating:
# - Multiple UDF creation and registration patterns
# - Performance considerations and optimization strategies
# - Complex data processing logic not available in built-in functions
# - SQL integration and catalog management
# - Data quality assessment and validation patterns
# - Best practices for maintainable and efficient UDF usage
# - Advanced patterns including conditional application and chaining
# ============================================================================