# ========================================
# Caching Example 1: Why Do We Need Caching?
# ========================================

# Let's see the problem that caching solves with real timing comparisons!

print("🐌 The Problem: Repeated Computations Are SLOW!")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand
import time

# Start Spark session
spark = SparkSession.builder \
    .appName("CachingExample1") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

print("✅ Spark session started!")

# Create a sample dataset that takes some time to process
print("\n📊 Creating a dataset that requires computation...")

# This creates a DataFrame with some expensive transformations
data = spark.range(1, 500000) \
    .withColumn("value", col("id") * 2) \
    .withColumn("category", when(col("id") % 3 == 0, "Premium")
                           .when(col("id") % 3 == 1, "Standard") 
                           .otherwise("Basic")) \
    .withColumn("score", rand() * 100) \
    .withColumn("adjusted_score", 
                when(col("category") == "Premium", col("score") * 1.5)
                .when(col("category") == "Standard", col("score") * 1.2)
                .otherwise(col("score")))

print(f"📈 Dataset created with transformations")
print("   • ID generation")  
print("   • Value calculation")
print("   • Category assignment with conditions")
print("   • Random score generation")
print("   • Adjusted score calculation")

print("\n🔄 Now let's use this data multiple times WITHOUT caching...")

# Scenario: We need to perform multiple operations on the same data
# This simulates real-world analytics where you explore data interactively

operations = [
    "Count total records",
    "Find premium customers", 
    "Calculate average score",
    "Get high-scoring customers"
]

total_time_without_cache = 0

print("\n⏱️ Timing multiple operations WITHOUT caching:")
print("=" * 50)

# Operation 1: Count total records
print("🔄 Operation 1: Count total records")
start_time = time.time()
total_count = data.count()
operation1_time = time.time() - start_time
total_time_without_cache += operation1_time
print(f"   Result: {total_count:,} records")
print(f"   Time: {operation1_time:.2f} seconds")

# Operation 2: Find premium customers
print("\n🔄 Operation 2: Find premium customers")
start_time = time.time()
premium_count = data.filter(col("category") == "Premium").count()
operation2_time = time.time() - start_time
total_time_without_cache += operation2_time
print(f"   Result: {premium_count:,} premium customers")
print(f"   Time: {operation2_time:.2f} seconds")

# Operation 3: Calculate average score
print("\n🔄 Operation 3: Calculate average score")
start_time = time.time()
avg_score = data.agg({"adjusted_score": "avg"}).collect()[0][0]
operation3_time = time.time() - start_time
total_time_without_cache += operation3_time
print(f"   Result: Average score = {avg_score:.2f}")
print(f"   Time: {operation3_time:.2f} seconds")

# Operation 4: Get high-scoring customers
print("\n🔄 Operation 4: Get high-scoring customers")
start_time = time.time()
high_score_count = data.filter(col("adjusted_score") > 80).count()
operation4_time = time.time() - start_time
total_time_without_cache += operation4_time
print(f"   Result: {high_score_count:,} high-scoring customers")
print(f"   Time: {operation4_time:.2f} seconds")

print("\n" + "=" * 50)
print(f"🐌 TOTAL TIME WITHOUT CACHING: {total_time_without_cache:.2f} seconds")

print("\n🧠 What just happened?")
explanation = """
🔄 EVERY operation triggered the ENTIRE computation chain:
   1. Generate 500,000 IDs
   2. Calculate values (id * 2)
   3. Assign categories with conditions
   4. Generate random scores
   5. Calculate adjusted scores
   6. THEN perform the requested operation

📊 This means:
   • The same expensive transformations ran 4 times
   • Total computation = 4x the work needed
   • Each operation had to start from scratch
   • No memory of previous computations
"""
print(explanation)

print(f"\n⚡ THE SOLUTION: CACHING!")
print("   What if we could store the computed data in memory?")
print("   Then each operation would be MUCH faster!")

print(f"\n🎯 In the next example, you'll see:")
next_preview = """
✅ How to cache the DataFrame in memory
✅ Dramatic speed improvements (often 5-10x faster!)
✅ The difference between cache() and actual caching
✅ Why caching is essential for interactive analytics
"""
print(next_preview)

print(f"\n💡 Real-World Scenarios Where This Matters:")
scenarios = """
📊 Interactive Data Analysis:
   • Exploring datasets in Jupyter notebooks
   • Running multiple queries on the same data
   • Building dashboards with repeated calculations

🤖 Machine Learning:
   • Feature engineering pipelines
   • Iterative algorithms (gradient descent)
   • Cross-validation with multiple model training

🔄 ETL Pipelines:
   • Complex transformations used in multiple outputs
   • Data quality checks on processed data
   • Multiple aggregations from the same source
"""
print(scenarios)

print(f"\n📈 Performance Impact Preview:")
impact = """
Without Caching: 🐌🐌🐌🐌 (4x computation)
With Caching:    ⚡        (1x computation + fast access)

Typical speedups:
• 2-5x faster for simple operations
• 5-10x faster for complex transformations  
• 10-100x faster for iterative algorithms
"""
print(impact)

print("\n✅ Example 1 complete! You've seen the problem caching solves.")
print("💡 Ready for Example 2? We'll cache this same data and see the magic!")

# Don't stop Spark yet - we'll use the same session in the next example
print("\n🔧 Keeping Spark session running for Example 2...")


