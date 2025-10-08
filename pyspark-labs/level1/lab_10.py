# ============================================================================
# PySpark Example 10: Unstructured Data Processing - Log Analysis
# ============================================================================
# INTENT: Demonstrate regex-based parsing of unstructured text data (Apache logs)
# CONCEPTS: Regular expressions, text processing, log analysis, data extraction patterns

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, substring_index, col, count as count_func, sum as spark_sum, 
    avg, max as spark_max, split, when, isnan, isnull, trim, 
    to_timestamp, hour, dayofweek, date_format, desc, asc
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# --------------------------------------------
# 1. Spark Session Setup
# --------------------------------------------
spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("log-analysis-demo") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=== Unstructured Data Processing - Apache Log Analysis ===")

# --------------------------------------------
# 2. Raw Text Data Ingestion
# --------------------------------------------
print("\n=== Raw Log Data Ingestion ===")

try:
    # Read raw log file as text
    file_df = spark.read.text("source/apache_logs.txt")
    print("Successfully loaded Apache log file")
    
except Exception as e:
    print(f"Could not load log file: {e}")
    print("Creating sample Apache log data for demonstration...")
    
print("Raw log data schema:")
file_df.printSchema()    

print(f"Total log entries: {file_df.count()}")
print("\nSample raw log entries:")
file_df.show(3, truncate=False)



# --------------------------------------------
# 3. Apache Log Format Analysis
# --------------------------------------------
print("\n=== Apache Log Format Understanding ===")

print("Apache Common Log Format structure:")
format_explanation = [
    "IP_ADDRESS - - [TIMESTAMP] \"METHOD URL PROTOCOL\" STATUS SIZE \"REFERRER\" \"USER_AGENT\"",
    "",
    "Components breakdown:",
    "• IP Address: Client IP making the request",
    "• Timestamp: When the request occurred [dd/MMM/yyyy:HH:mm:ss timezone]", 
    "• HTTP Method: GET, POST, PUT, DELETE, etc.",
    "• URL: Requested resource path",
    "• Protocol: HTTP version (HTTP/1.0, HTTP/1.1)",
    "• Status Code: HTTP response code (200, 404, 500, etc.)",
    "• Response Size: Bytes sent to client",
    "• Referrer: Source page that linked to this request",
    "• User Agent: Browser/client information"
]

for line in format_explanation:
    print(line)


# --------------------------------------------
# 4. Regular Expression Pattern Development
# --------------------------------------------
print("\n=== Regular Expression Pattern Analysis ===")

# Your original regex pattern
log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

print("Regex pattern breakdown:")
pattern_parts = [
    ("(\S+)", "Group 1: IP Address - non-whitespace characters"),
    ("(\S+)", "Group 2: Identity (usually -)"),  
    ("(\S+)", "Group 3: User (usually -)"),
    ("\[([\w:/]+\s[+\-]\d{4})\]", "Group 4: Timestamp in brackets"),
    ('"(\S+) (\S+) (\S+)"', "Groups 5-7: HTTP Method, URL, Protocol"),
    ("(\d{3})", "Group 8: HTTP Status Code"),
    ("(\S+)", "Group 9: Response Size"),
    ('"(\S+)"', "Group 10: Referrer URL"),
    ('"([^"]*)"', "Group 11: User Agent (anything except quotes)")
]

for i, (pattern, description) in enumerate(pattern_parts, 1):
    print(f"  {pattern} -> {description}")

# Enhanced regex with named groups for clarity
enhanced_log_reg = r'^(?P<ip>\S+) (?P<identity>\S+) (?P<user>\S+) \[(?P<timestamp>[\w:/]+\s[+\-]\d{4})\] "(?P<method>\S+) (?P<url>\S+) (?P<protocol>\S+)" (?P<status>\d{3}) (?P<size>\S+) "(?P<referrer>\S+)" "(?P<user_agent>[^"]*)"'



# --------------------------------------------
# 5. Log Parsing and Structured Data Extraction
# --------------------------------------------
print("\n=== Log Parsing and Data Extraction ===")

# Parse logs using regex extraction (your original approach)
logs_df = file_df.select(
    regexp_extract('value', log_reg, 1).alias('ip'),
    regexp_extract('value', log_reg, 4).alias('date'),
    regexp_extract('value', log_reg, 5).alias('method'),
    regexp_extract('value', log_reg, 6).alias('request'),
    regexp_extract('value', log_reg, 8).alias('status'),
    regexp_extract('value', log_reg, 9).alias('size'), 
    regexp_extract('value', log_reg, 10).alias('referrer'),
    regexp_extract('value', log_reg, 11).alias('user_agent')
)

print("Extracted structured data schema:")
logs_df.printSchema()

print("Sample parsed log entries:")
logs_df.show(5, truncate=False)

# --------------------------------------------
# 6. Data Quality Validation
# --------------------------------------------
print("\n=== Data Quality Validation ===")

# Check parsing success rate
total_records = logs_df.count()
successfully_parsed = logs_df.filter(col("ip") != "").count()
parsing_success_rate = (successfully_parsed / total_records) * 100

print(f"Parsing results:")
print(f"  Total records: {total_records}")
print(f"  Successfully parsed: {successfully_parsed}")
print(f"  Parsing success rate: {parsing_success_rate:.1f}%")

# Validate extracted fields
validation_checks = [
    ("Empty IP addresses", logs_df.filter(col("ip") == "").count()),
    ("Empty timestamps", logs_df.filter(col("date") == "").count()),
    ("Empty HTTP methods", logs_df.filter(col("method") == "").count()),
    ("Empty status codes", logs_df.filter(col("status") == "").count())
]

print("\nData quality checks:")
for check_name, count in validation_checks:
    status = "PASS" if count == 0 else "FAIL"
    print(f"  {check_name}: {count} [{status}]")

# Show unique values for categorical fields
print("\nUnique values analysis:")
print("HTTP Methods:")
logs_df.select("method").distinct().show()

print("Status Codes:")
logs_df.select("status").distinct().show()



# --------------------------------------------
# 7. Advanced Log Analysis Patterns
# --------------------------------------------
print("\n=== Advanced Log Analysis ===")

# Analysis 1: Request analysis by HTTP method
method_analysis = logs_df.groupBy("method") \
    .agg(count_func("*").alias("request_count")) \
    .orderBy(desc("request_count"))

print("Requests by HTTP Method:")
method_analysis.show()

# Analysis 2: Status code distribution
status_analysis = logs_df.groupBy("status") \
    .agg(count_func("*").alias("count")) \
    .withColumn("status_category",
                when(col("status").startswith("2"), "Success")
                .when(col("status").startswith("3"), "Redirect")
                .when(col("status").startswith("4"), "Client Error")
                .when(col("status").startswith("5"), "Server Error")
                .otherwise("Unknown")) \
    .orderBy("status")

print("HTTP Status Code Analysis:")
status_analysis.show()

# Analysis 3: Top IP addresses (potential security analysis)
ip_analysis = logs_df.groupBy("ip") \
    .agg(count_func("*").alias("request_count")) \
    .orderBy(desc("request_count"))

print("Top IP Addresses by Request Volume:")
ip_analysis.show(10)

# Analysis 4: Error analysis
error_analysis = logs_df.filter(col("status").cast("int") >= 400) \
    .groupBy("status", "request") \
    .agg(count_func("*").alias("error_count")) \
    .orderBy(desc("error_count"))

print("Error Analysis (4xx and 5xx):")
error_analysis.show(10, truncate=False)



# --------------------------------------------
# 8. Referrer Analysis (Your Original Focus)
# --------------------------------------------
print("\n=== Referrer Analysis (Original Example Enhanced) ===")

# Your original referrer analysis with enhancements
referrer_analysis = logs_df \
    .filter("trim(referrer) != '-'") \
    .filter("trim(referrer) != ''") \
    .withColumn("referrer_domain", substring_index("referrer", "/", 3)) \
    .groupBy("referrer_domain") \
    .agg(count_func("*").alias("referral_count")) \
    .orderBy(desc("referral_count"))

print("Traffic Sources by Referrer Domain:")
referrer_analysis.show(20, truncate=False)

# Enhanced referrer analysis
enhanced_referrer = logs_df \
    .filter("trim(referrer) != '-'") \
    .withColumn("referrer_domain", substring_index("referrer", "/", 3)) \
    .withColumn("referrer_type",
                when(col("referrer").contains("google"), "Search Engine")
                .when(col("referrer").contains("facebook"), "Social Media")
                .when(col("referrer").contains("twitter"), "Social Media")
                .when(col("referrer") == "-", "Direct")
                .otherwise("Other Website")) \
    .groupBy("referrer_type", "referrer_domain") \
    .agg(count_func("*").alias("count")) \
    .orderBy(desc("count"))

print("Enhanced Referrer Analysis by Type:")
enhanced_referrer.show(truncate=False)



# --------------------------------------------
# 9. User Agent Analysis
# --------------------------------------------
print("\n=== User Agent Analysis ===")

# Extract browser information from user agent
browser_analysis = logs_df \
    .withColumn("browser",
                when(col("user_agent").contains("Chrome"), "Chrome")
                .when(col("user_agent").contains("Firefox"), "Firefox") 
                .when(col("user_agent").contains("Safari"), "Safari")
                .when(col("user_agent").contains("Edge"), "Edge")
                .when(col("user_agent").contains("Mozilla"), "Mozilla")
                .otherwise("Other")) \
    .groupBy("browser") \
    .agg(count_func("*").alias("usage_count")) \
    .orderBy(desc("usage_count"))

print("Browser Usage Statistics:")
browser_analysis.show()

# Operating system detection
os_analysis = logs_df \
    .withColumn("os",
                when(col("user_agent").contains("Windows"), "Windows")
                .when(col("user_agent").contains("Macintosh"), "MacOS")
                .when(col("user_agent").contains("Linux"), "Linux")
                .when(col("user_agent").contains("Android"), "Android")
                .when(col("user_agent").contains("iOS"), "iOS")
                .otherwise("Other")) \
    .groupBy("os") \
    .agg(count_func("*").alias("count")) \
    .orderBy(desc("count"))

print("Operating System Distribution:")
os_analysis.show()



# --------------------------------------------
# 10. Time-based Analysis
# --------------------------------------------
print("\n=== Time-based Analysis ===")

# Parse timestamps for temporal analysis
time_df = logs_df.withColumn(
    "parsed_timestamp",
    to_timestamp(col("date"), "dd/MMM/yyyy:HH:mm:ss Z")
).filter(col("parsed_timestamp").isNotNull())

if time_df.count() > 0:
    # Hourly traffic patterns
    hourly_traffic = time_df \
        .withColumn("hour", hour("parsed_timestamp")) \
        .groupBy("hour") \
        .agg(count_func("*").alias("requests")) \
        .orderBy("hour")
    
    print("Hourly Traffic Patterns:")
    hourly_traffic.show(24)
    
    # Daily patterns
    daily_patterns = time_df \
        .withColumn("day_of_week", dayofweek("parsed_timestamp")) \
        .withColumn("day_name",
                   when(col("day_of_week") == 1, "Sunday")
                   .when(col("day_of_week") == 2, "Monday")
                   .when(col("day_of_week") == 3, "Tuesday")
                   .when(col("day_of_week") == 4, "Wednesday")
                   .when(col("day_of_week") == 5, "Thursday")
                   .when(col("day_of_week") == 6, "Friday")
                   .when(col("day_of_week") == 7, "Saturday")) \
        .groupBy("day_name", "day_of_week") \
        .agg(count_func("*").alias("requests")) \
        .orderBy("day_of_week")
    
    print("Daily Traffic Patterns:")
    daily_patterns.show()

else:
    print("Timestamp parsing failed - timestamps may not match expected format")



# --------------------------------------------
# 11. Performance and Security Analytics
# --------------------------------------------
print("\n=== Performance and Security Analytics ===")

# Response size analysis
size_analysis = logs_df \
    .filter(col("size") != "-") \
    .filter(col("size").cast("int").isNotNull()) \
    .withColumn("size_bytes", col("size").cast("int")) \
    .groupBy("status") \
    .agg(
        count_func("*").alias("request_count"),
        avg("size_bytes").alias("avg_response_size"),
        spark_max("size_bytes").alias("max_response_size"),
        spark_sum("size_bytes").alias("total_bytes")
    ) \
    .orderBy("status")

print("Response Size Analysis by Status Code:")
size_analysis.show()

# Potential security threats
security_analysis = logs_df.filter(
    col("request").contains("admin") |
    col("request").contains("login") |
    col("request").contains("password") |
    col("request").contains("..") |
    col("user_agent").contains("bot") |
    col("user_agent").contains("crawl")
).groupBy("request", "status") \
.agg(count_func("*").alias("count")) \
.orderBy(desc("count"))

print("Potential Security-Related Requests:")
security_analysis.show(10, truncate=False)

# --------------------------------------------
# 12. Best Practices and Performance Optimization
# --------------------------------------------
print("\n=== Best Practices for Log Processing ===")

best_practices = [
    "✓ Use explicit regex patterns with named groups for maintainability",
    "✓ Validate parsing success rates and handle malformed entries",
    "✓ Cache intermediate DataFrames for multiple analysis operations", 
    "✓ Partition log data by date for efficient querying",
    "✓ Use appropriate data types (cast strings to integers where applicable)",
    "✓ Implement data quality checks for critical fields",
    "✓ Consider streaming processing for real-time log analysis",
    "✓ Use columnar formats (Parquet) for processed log storage",
    "✓ Implement retention policies for large log volumes",
    "✓ Monitor processing performance and optimize regex patterns"
]

for practice in best_practices:
    print(practice)

# Performance considerations
print("\nPerformance optimization tips:")
performance_tips = [
    f"• Current partitions: {logs_df.rdd.getNumPartitions()}",
    "• Consider repartitioning by date for time-series analysis",
    "• Cache frequently accessed DataFrames with .cache()",
    "• Use broadcast joins for small lookup tables",
    "• Optimize regex patterns to avoid backtracking",
    "• Consider pre-filtering raw data before expensive operations"
]

for tip in performance_tips:
    print(tip)

# --------------------------------------------
# 13. Export and Persistence Patterns  
# --------------------------------------------
print("\n=== Data Export Patterns ===")

# Create analytical views
logs_df.createOrReplaceTempView("parsed_logs")

# Sample analytical queries using SQL
print("Creating analytical views for downstream consumption:")

analytical_queries = [
    "traffic_by_hour: Hourly request patterns",
    "error_summary: Error rates by endpoint", 
    "security_events: Potential security incidents",
    "performance_metrics: Response time and size analytics",
    "user_behavior: Session and referrer analysis"
]

for query in analytical_queries:
    print(f"  - {query}")

print("\n=== Final Results (Your Original Analysis) ===")
print("Original referrer analysis:")
logs_df \
    .filter("trim(referrer) != '-' ") \
    .withColumn("referrer", substring_index("referrer", "/", 3)) \
    .groupBy("referrer") \
    .count() \
    .show(100, truncate=False)

# --------------------------------------------
# 14. Cleanup
# --------------------------------------------
spark.stop()

# ============================================================================
# SUMMARY: Unstructured Data Processing demonstrating:
# - Regular expression parsing for log file analysis
# - Data quality validation and parsing success monitoring  
# - Comprehensive web analytics from Apache logs
# - Security and performance analysis patterns
# - Time-series analysis and user behavior insights
# - Best practices for large-scale log processing
# - Integration of regex extraction with DataFrame operations
# ============================================================================