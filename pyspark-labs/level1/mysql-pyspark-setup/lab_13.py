# ============================================================================
# PySpark Example 13: Aggregate Functions & Grouped Analytics
# ============================================================================
# INTENT: Demonstrate comprehensive aggregation patterns and business analytics
# CONCEPTS: Grouping, aggregation functions, SQL vs DataFrame API, business metrics

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import (
    col, when, sum as spark_sum, avg, count, min as spark_min, max as spark_max,
    countDistinct, stddev, variance, skewness, kurtosis, percentile_approx,
    first, last, collect_list, collect_set, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# --------------------------------------------
# 1. Spark Session Setup
# --------------------------------------------
spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("aggregate-functions-demo") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("=== Aggregate Functions & Grouped Analytics Demo ===")

# --------------------------------------------
# 2. Data Loading and Preparation
# --------------------------------------------
print("\n=== Invoice Data Loading ===")

try:
    # Load invoice data
    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("source/invoices.csv")
    
    print("Successfully loaded invoice data")
    
except Exception as e:
    print(f"Could not load invoice data: {e}")
    print("Creating sample invoice data for demonstration...")
    
    # Create comprehensive sample invoice data
    sample_schema = StructType([
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True)
    ])
    
    sample_data = [
        ("536365", "85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 6, "2010-12-01 08:26", 2.55, "17850", "United Kingdom"),
        ("536365", "71053", "WHITE METAL LANTERN", 6, "2010-12-01 08:26", 3.39, "17850", "United Kingdom"),
        ("536365", "84406B", "CREAM CUPID HEARTS COAT HANGER", 8, "2010-12-01 08:26", 2.75, "17850", "United Kingdom"),
        ("536366", "22633", "HAND WARMER UNION JACK", 6, "2010-12-01 08:28", 1.85, "17850", "United Kingdom"),
        ("536367", "84879", "ASSORTED COLOUR BIRD ORNAMENT", 32, "2010-12-01 08:34", 1.69, "13047", "United Kingdom"),
        ("536367", "22745", "POPPY'S PLAYHOUSE BEDROOM", 6, "2010-12-01 08:34", 2.10, "13047", "United Kingdom"),
        ("536368", "22748", "POPPY'S PLAYHOUSE KITCHEN", 4, "2010-12-01 08:34", 2.10, "13047", "United Kingdom"),
        ("536369", "21756", "BATH BUILDING BLOCK WORD", 3, "2010-12-01 08:35", 5.95, "13047", "United Kingdom"),
        ("536370", "22633", "HAND WARMER UNION JACK", 12, "2010-12-01 08:45", 1.85, "12583", "France"),
        ("536370", "22632", "HAND WARMER RED POLKA DOT", 6, "2010-12-01 08:45", 1.85, "12583", "France"),
        ("536371", "21754", "HOME BUILDING BLOCK WORD", 4, "2010-12-01 09:00", 5.95, "14606", "Germany"),
        ("536371", "21755", "LOVE BUILDING BLOCK WORD", 3, "2010-12-01 09:00", 5.95, "14606", "Germany"),
        ("536372", "22629", "SPACEBOY LUNCH BOX", 4, "2010-12-01 09:01", 1.95, "17809", "United Kingdom"),
        ("536373", "85014", "RED RETROSPOT CHARLOTTE BAG", 10, "2010-12-01 09:02", 1.69, "12431", "Australia"),
        ("536374", "22090", "PAPERWEIGHT CRYSTAL PYRAMID", 12, "2010-12-01 09:32", 4.25, "16029", "Germany")
    ]
    
    invoice_df = spark.createDataFrame(sample_data, sample_schema)

print("Invoice data schema:")
invoice_df.printSchema()

print(f"Total records: {invoice_df.count()}")
print("Sample data:")
invoice_df.show(5, truncate=False)

# --------------------------------------------
# 3. Basic Aggregation Functions
# --------------------------------------------
print("\n=== Basic Aggregation Functions ===")

# Method 1: Using DataFrame API with functions
print("Method 1: DataFrame API Aggregations")
basic_agg_df = invoice_df.select(
    f.count("*").alias("Total Records"),
    f.sum("Quantity").alias("Total Quantity"),
    f.avg("UnitPrice").alias("Avg Unit Price"),
    f.min("UnitPrice").alias("Min Unit Price"),
    f.max("UnitPrice").alias("Max Unit Price"),
    f.countDistinct("InvoiceNo").alias("Unique Invoices"),
    f.countDistinct("Country").alias("Countries Served"),
    f.countDistinct("StockCode").alias("Unique Products")
)

basic_agg_df.show(truncate=False)

# Method 2: Using selectExpr with SQL expressions
print("Method 2: SQL Expression Aggregations")
sql_expr_agg = invoice_df.selectExpr(
    "count(1) as `Record Count`",
    "sum(Quantity) as `Total Quantity`",
    "avg(UnitPrice) as `Average Unit Price`",
    "round(avg(UnitPrice), 2) as `Rounded Avg Price`",
    "count(distinct InvoiceNo) as `Unique Invoice Count`",
    "count(distinct CustomerID) as `Unique Customer Count`"
)

sql_expr_agg.show(truncate=False)

# Method 3: Statistical aggregations
print("Method 3: Statistical Aggregations")
stats_agg = invoice_df.select(
    f.stddev("UnitPrice").alias("Price Std Dev"),
    f.variance("UnitPrice").alias("Price Variance"),
    f.skewness("Quantity").alias("Quantity Skewness"),
    f.kurtosis("UnitPrice").alias("Price Kurtosis"),
    f.percentile_approx("UnitPrice", 0.5).alias("Median Price"),
    f.percentile_approx("Quantity", 0.25).alias("Q1 Quantity"),
    f.percentile_approx("Quantity", 0.75).alias("Q3 Quantity")
)

stats_agg.show(truncate=False)

# --------------------------------------------
# 4. Grouped Aggregations - Multiple Approaches
# --------------------------------------------
print("\n=== Grouped Aggregations ===")

# Approach 1: SQL-based grouping (commented approach from your code)
print("Approach 1: SQL-based Aggregation")
invoice_df.createOrReplaceTempView("sales")

summary_sql = spark.sql("""
    SELECT 
        Country, 
        InvoiceNo,
        sum(Quantity) as TotalQuantity,
        round(sum(Quantity * UnitPrice), 2) as InvoiceValue,
        count(*) as LineItems,
        avg(UnitPrice) as AvgUnitPrice
    FROM sales
    GROUP BY Country, InvoiceNo
    ORDER BY InvoiceValue DESC
""")

print("SQL-based grouped results:")
summary_sql.show(10, truncate=False)

# Approach 2: DataFrame API grouping (your current approach enhanced)
print("Approach 2: DataFrame API Aggregation")
summary_df = invoice_df \
    .groupBy("Country", "InvoiceNo") \
    .agg(
        f.sum("Quantity").alias("TotalQuantity"),
        f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"),
        f.count("*").alias("LineItems"),
        f.avg("UnitPrice").alias("AvgUnitPrice"),
        f.countDistinct("StockCode").alias("UniqueProducts"),
        f.collect_set("Description").alias("ProductTypes")
    ) \
    .orderBy(f.desc("InvoiceValue"))

print("DataFrame API grouped results:")
summary_df.show(10, truncate=False)

# Approach 3: Multiple grouping levels
print("Approach 3: Hierarchical Aggregations")

# Country-level aggregation
country_summary = invoice_df \
    .groupBy("Country") \
    .agg(
        f.countDistinct("InvoiceNo").alias("InvoiceCount"),
        f.countDistinct("CustomerID").alias("CustomerCount"),
        f.sum("Quantity").alias("TotalQuantity"),
        f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("TotalRevenue"),
        f.avg("UnitPrice").alias("AvgUnitPrice"),
        f.countDistinct("StockCode").alias("UniqueProducts")
    ) \
    .orderBy(f.desc("TotalRevenue"))

print("Country-level analysis:")
country_summary.show(truncate=False)

# Product-level aggregation
product_summary = invoice_df \
    .groupBy("StockCode", "Description") \
    .agg(
        f.sum("Quantity").alias("TotalSold"),
        f.countDistinct("InvoiceNo").alias("TimesOrdered"),
        f.countDistinct("Country").alias("CountriesSold"),
        f.avg("UnitPrice").alias("AvgPrice"),
        f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("TotalRevenue")
    ) \
    .orderBy(f.desc("TotalRevenue"))

print("Top products by revenue:")
product_summary.show(10, truncate=False)

# --------------------------------------------
# 5. Advanced Aggregation Patterns
# --------------------------------------------
print("\n=== Advanced Aggregation Patterns ===")

# Pattern 1: Conditional aggregations
conditional_agg = invoice_df \
    .groupBy("Country") \
    .agg(
        f.sum("Quantity").alias("TotalQuantity"),
        f.sum(f.when(f.col("UnitPrice") > 2.0, f.col("Quantity")).otherwise(0)).alias("HighPriceQuantity"),
        f.count(f.when(f.col("UnitPrice") > 2.0, 1)).alias("HighPriceItems"),
        f.avg(f.when(f.col("UnitPrice") > 2.0, f.col("UnitPrice"))).alias("AvgHighPrice"),
        f.round(f.sum(f.when(f.col("UnitPrice") > 2.0, f.expr("Quantity * UnitPrice")).otherwise(0)), 2).alias("HighPriceRevenue")
    )

print("Conditional aggregations (items > $2.00):")
conditional_agg.show(truncate=False)

# Pattern 2: Multiple grouping sets using ROLLUP
rollup_analysis = spark.sql("""
    SELECT 
        Country,
        CASE 
            WHEN UnitPrice > 3.0 THEN 'Premium'
            WHEN UnitPrice > 1.5 THEN 'Standard' 
            ELSE 'Budget'
        END as PriceCategory,
        count(*) as ItemCount,
        sum(Quantity) as TotalQuantity,
        round(sum(Quantity * UnitPrice), 2) as Revenue
    FROM sales
    GROUP BY Country, PriceCategory WITH ROLLUP
    ORDER BY Country, PriceCategory
""")

print("ROLLUP analysis by country and price category:")
rollup_analysis.show(20, truncate=False)

# Pattern 3: Window functions with aggregations
from pyspark.sql.window import Window

country_window = Window.partitionBy("Country").orderBy(f.desc("InvoiceValue"))

windowed_analysis = summary_df \
    .withColumn("CountryRank", f.row_number().over(country_window)) \
    .withColumn("RunningTotal", f.sum("InvoiceValue").over(
        country_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )) \
    .withColumn("CountryTotal", f.sum("InvoiceValue").over(Window.partitionBy("Country"))) \
    .withColumn("PercentOfCountry", f.round((f.col("InvoiceValue") / f.col("CountryTotal")) * 100, 2))

print("Window functions with aggregations:")
windowed_analysis.select("Country", "InvoiceNo", "InvoiceValue", "CountryRank", "PercentOfCountry").show(15, truncate=False)

# --------------------------------------------
# 6. Business Intelligence Metrics
# --------------------------------------------
print("\n=== Business Intelligence Metrics ===")

# Customer value analysis
customer_analysis = invoice_df \
    .filter(f.col("CustomerID").isNotNull()) \
    .groupBy("CustomerID", "Country") \
    .agg(
        f.countDistinct("InvoiceNo").alias("OrderCount"),
        f.sum("Quantity").alias("TotalItemsBought"),
        f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("CustomerValue"),
        f.avg("UnitPrice").alias("AvgItemPrice"),
        f.countDistinct("StockCode").alias("ProductVariety")
    ) \
    .withColumn("AvgOrderValue", f.round(f.col("CustomerValue") / f.col("OrderCount"), 2)) \
    .orderBy(f.desc("CustomerValue"))

print("Top customers by value:")
customer_analysis.show(10, truncate=False)

# Market penetration analysis
market_analysis = invoice_df \
    .groupBy("Country") \
    .agg(
        f.countDistinct("CustomerID").alias("ActiveCustomers"),
        f.countDistinct("InvoiceNo").alias("TotalOrders"),
        f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("MarketRevenue"),
        f.avg(f.expr("Quantity * UnitPrice")).alias("AvgTransactionValue")
    ) \
    .withColumn("OrdersPerCustomer", f.round(f.col("TotalOrders") / f.col("ActiveCustomers"), 2)) \
    .withColumn("RevenuePerCustomer", f.round(f.col("MarketRevenue") / f.col("ActiveCustomers"), 2)) \
    .orderBy(f.desc("MarketRevenue"))

print("Market penetration analysis:")
market_analysis.show(truncate=False)

# Product performance matrix
product_matrix = invoice_df \
    .groupBy("StockCode") \
    .agg(
        f.first("Description").alias("ProductName"),
        f.sum("Quantity").alias("TotalQuantity"),
        f.countDistinct("InvoiceNo").alias("OrderFrequency"),
        f.countDistinct("CustomerID").alias("CustomerReach"),
        f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("TotalRevenue")
    ) \
    .withColumn("RevenuePerUnit", f.round(f.col("TotalRevenue") / f.col("TotalQuantity"), 2)) \
    .withColumn("AvgQuantityPerOrder", f.round(f.col("TotalQuantity") / f.col("OrderFrequency"), 2)) \
    .orderBy(f.desc("TotalRevenue"))

print("Product performance matrix:")
product_matrix.show(10, truncate=False)

# --------------------------------------------
# 7. Performance Optimization for Aggregations
# --------------------------------------------
print("\n=== Aggregation Performance Optimization ===")

# Optimization 1: Caching for repeated aggregations
cached_invoice_df = invoice_df.cache()
print(f"DataFrame cached for repeated aggregations: {cached_invoice_df.is_cached}")

# Optimization 2: Partitioning analysis
print(f"Current partitions: {invoice_df.rdd.getNumPartitions()}")

# Optimization 3: Broadcast small dimension tables (simulated)
small_countries = country_summary.filter(f.col("TotalRevenue") < 100)
broadcast_countries = f.broadcast(small_countries)

print("Optimization strategies:")
optimization_tips = [
    "• Cache DataFrames when performing multiple aggregations",
    "• Use appropriate partition sizes for groupBy operations",
    "• Broadcast small lookup tables for joins with aggregated data",
    "• Consider bucketing for frequently grouped columns",
    "• Use columnar formats (Parquet) for analytical workloads",
    f"• Current partition count: {invoice_df.rdd.getNumPartitions()} (optimal: 2-4 per CPU core)"
]

for tip in optimization_tips:
    print(tip)

# --------------------------------------------
# 8. Aggregation Function Reference Guide
# --------------------------------------------
print("\n=== Aggregation Function Categories ===")

function_categories = {
    "Basic Aggregations": ["count()", "sum()", "avg()", "min()", "max()"],
    "Statistical Functions": ["stddev()", "variance()", "skewness()", "kurtosis()"],
    "Approximate Functions": ["approx_count_distinct()", "percentile_approx()"],
    "Collection Functions": ["collect_list()", "collect_set()", "first()", "last()"],
    "Conditional Aggregations": ["sum(when())", "count(when())", "avg(when())"],
    "Window Aggregations": ["sum().over()", "avg().over()", "count().over()"],
    "Multi-dimensional": ["grouping()", "grouping_id()", "cube()", "rollup()"]
}

print("Function categories:")
for category, functions in function_categories.items():
    print(f"  {category}: {', '.join(functions)}")

# --------------------------------------------
# 9. Best Practices for Aggregations
# --------------------------------------------
print("\n=== Best Practices for Aggregations ===")

best_practices = [
    "✓ Use appropriate data types to avoid overflow in sum() operations",
    "✓ Handle null values explicitly in aggregation functions",
    "✓ Cache DataFrames when performing multiple aggregation operations",
    "✓ Use countDistinct() sparingly on large datasets (expensive operation)",
    "✓ Consider approx_count_distinct() for large-scale approximate counts",
    "✓ Partition data appropriately for optimal groupBy performance",
    "✓ Use window functions for running totals and rankings",
    "✓ Combine multiple aggregations in single agg() call for efficiency",
    "✓ Use SQL rollup and cube for multi-dimensional analysis",
    "✓ Monitor query execution plans for aggregation optimization"
]

for practice in best_practices:
    print(practice)

print("\n=== Final Results (Your Original Code) ===")
print("Your enhanced groupBy aggregation:")

# Execute your original code with enhancements
your_summary = invoice_df \
    .groupBy("Country", "InvoiceNo") \
    .agg(
        f.sum("Quantity").alias("TotalQuantity"),
        f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"),
        f.expr("round(sum(Quantity * UnitPrice), 2) as InvoiceValueExpr")
    )

your_summary.show()

# Verify both approaches produce same results
print("\nVerification: Both calculation methods produce identical results:")
verification = your_summary.withColumn("ValuesMatch", 
    f.when(f.col("InvoiceValue") == f.col("InvoiceValueExpr"), "✓ Match")
    .otherwise("✗ Mismatch")
)
verification.select("Country", "InvoiceNo", "ValuesMatch").distinct().show()

# --------------------------------------------
# 10. Cleanup
# --------------------------------------------
spark.stop()

# ============================================================================
# SUMMARY: Aggregate Functions demonstrating:
# - Comprehensive aggregation function usage (basic, statistical, conditional)
# - Multiple aggregation approaches (DataFrame API vs SQL)
# - Grouped analytics for business intelligence
# - Advanced patterns (window functions, rollup, conditional aggregations)
# - Performance optimization techniques for large-scale aggregations
# - Business metrics calculation and customer analytics
# - Best practices for production aggregation workflows
# ============================================================================