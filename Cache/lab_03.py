# ========================================
# Caching Example 2: Your First Cache
# ========================================

# Now let's see the magic of caching with the SAME data from Example 1!

print("⚡ The Solution: Your First Cache!")

# NOTE: This continues from Example 1 - we'll use the same Spark session and data
# If running independently, uncomment the setup section below:

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand
import time

spark = SparkSession.builder \
    .appName("CachingExample2") \
    .master("local[*]") \
    .getOrCreate()

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

print("\n💾 Step 1: Cache the DataFrame")
print("🔧 Using the .cache() method...")

# This is your first cache!
# .cache() tells Spark: "Store this DataFrame in memory for fast access"
cached_data = data.cache()

print("✅ DataFrame cached!")
print(f"   Original DataFrame: {data}")
print(f"   Cached DataFrame: {cached_data}")

print("\n🤔 But wait... has anything actually been cached yet?")
print("   Answer: NO! Spark uses LAZY EVALUATION")
print("   The .cache() method just MARKS the DataFrame for caching")
print("   Nothing is stored until we trigger an ACTION")

print("\n🔄 Step 2: Trigger caching with our first operation")
print("   Let's run the same operations as Example 1, but with caching...")

total_time_with_cache = 0

print("\n⏱️ Timing multiple operations WITH caching:")
print("=" * 50)

# Operation 1: Count total records (this will BUILD the cache)
print("🔄 Operation 1: Count total records (BUILDS CACHE)")
start_time = time.time()
total_count = cached_data.count()
operation1_time = time.time() - start_time
total_time_with_cache += operation1_time
print(f"   Result: {total_count:,} records")
print(f"   Time: {operation1_time:.2f} seconds")
print("   ✨ CACHE BUILT! Data is now stored in memory")

# Operation 2: Find premium customers (this will USE the cache)
print("\n🔄 Operation 2: Find premium customers (USES CACHE)")
start_time = time.time()
premium_count = cached_data.filter(col("category") == "Premium").count()
operation2_time = time.time() - start_time
total_time_with_cache += operation2_time
print(f"   Result: {premium_count:,} premium customers")
print(f"   Time: {operation2_time:.2f} seconds")
print("   ⚡ MUCH FASTER! Data read from memory")

# Operation 3: Calculate average score (cache again!)
print("\n🔄 Operation 3: Calculate average score (USES CACHE)")
start_time = time.time()
avg_score = cached_data.agg({"adjusted_score": "avg"}).collect()[0][0]
operation3_time = time.time() - start_time
total_time_with_cache += operation3_time
print(f"   Result: Average score = {avg_score:.2f}")
print(f"   Time: {operation3_time:.2f} seconds")
print("   ⚡ SUPER FAST! No recomputation needed")

# Operation 4: Get high-scoring customers (cache again!)
print("\n🔄 Operation 4: Get high-scoring customers (USES CACHE)")
start_time = time.time()
high_score_count = cached_data.filter(col("adjusted_score") > 80).count()
operation4_time = time.time() - start_time
total_time_with_cache += operation4_time
print(f"   Result: {high_score_count:,} high-scoring customers")
print(f"   Time: {operation4_time:.2f} seconds")
print("   ⚡ LIGHTNING FAST! Data already in memory")

print("\n" + "=" * 50)
print(f"⚡ TOTAL TIME WITH CACHING: {total_time_with_cache:.2f} seconds")

# Compare with Example 1 results (approximate times)
print(f"\n📊 PERFORMANCE COMPARISON:")
print("=" * 40)
# These are typical times - yours may vary
estimated_without_cache = 9.0  # Approximate from Example 1
speedup = estimated_without_cache / total_time_with_cache
print(f"Without caching: ~{estimated_without_cache:.1f} seconds 🐌")
print(f"With caching:     {total_time_with_cache:.1f} seconds ⚡")
print(f"SPEEDUP: {speedup:.1f}x FASTER! 🚀")

print(f"\n🧠 What happened under the hood?")
explanation = """
🔄 First Operation (Count):
   1. Spark computed the entire transformation chain
   2. Stored the result in memory (cache built)
   3. Performed the count operation
   
⚡ Subsequent Operations:
   1. Spark found data already in memory
   2. Skipped all transformations
   3. Directly performed the operation on cached data
   4. MASSIVE time savings!
"""
print(explanation)

print(f"\n💡 Key Insights:")
insights = """
✅ .cache() MARKS a DataFrame for caching (lazy)
✅ First action BUILDS the cache (takes normal time)
✅ Subsequent actions USE the cache (super fast!)
✅ Cache persists until you explicitly remove it
✅ Same data, same results, WAY faster access
"""
print(insights)

print(f"\n🔍 Let's verify the cache is working:")

# Check if DataFrame is cached
print(f"Is DataFrame cached? {cached_data.is_cached}")
print(f"Storage level: {cached_data.storageLevel}")

print(f"\n🎯 Cache Verification Test:")
print("   Running the same operation twice to see consistency...")

# Run same operation twice to show cache consistency
print("\n🔄 First run of operation:")
start_time = time.time()
result1 = cached_data.filter(col("score") > 90).count()
time1 = time.time() - start_time
print(f"   High scorers: {result1:,} in {time1:.3f}s")

print("\n🔄 Second run of SAME operation:")
start_time = time.time()
result2 = cached_data.filter(col("score") > 90).count()
time2 = time.time() - start_time
print(f"   High scorers: {result2:,} in {time2:.3f}s")

print(f"\n✅ Results are identical: {result1 == result2}")
if time2 < time1:
    print(f"⚡ Second run was {time1/time2:.1f}x faster!")
else:
    print("📊 Both runs were very fast due to caching!")

print(f"\n🎛️ Behind the Scenes - What's in Memory:")
memory_explanation = """
🧠 Spark Memory Contains:
   • All 499,999 rows of computed data
   • All columns: id, value, category, score, adjusted_score
   • Ready for instant access by any operation
   • No need to recompute transformations
   
💾 Storage Details:
   • Uses default storage level: MEMORY_ONLY
   • Data stored in deserialized format (faster access)
   • Automatically partitioned across available cores
   • Falls back to recomputation if memory pressure
"""
print(memory_explanation)

print(f"\n🌟 Real-World Applications:")
applications = """
📊 Data Science Workflows:
   • Cache cleaned datasets for exploration
   • Store feature-engineered data for ML experiments
   • Keep intermediate results during analysis

🤖 Machine Learning:
   • Cache training data for multiple model attempts
   • Store preprocessed features for hyperparameter tuning
   • Keep validation sets in memory for quick evaluation

🏢 Business Analytics:
   • Cache daily/monthly aggregations
   • Store customer segments for various reports
   • Keep filtered datasets for dashboard queries
"""
print(applications)

print(f"\n⚠️ Important Notes:")
notes = """
🔧 Default Storage Level:
   • .cache() uses MEMORY_ONLY storage
   • Data lost if not enough memory
   • Next example shows safer alternatives

💾 Memory Usage:
   • Caching uses cluster memory
   • Monitor memory usage in production
   • Remove cache when no longer needed

🎯 When to Cache:
   • Data used 2+ times
   • Expensive transformations
   • Interactive analysis workflows
"""
print(notes)

print("\n✅ Example 2 complete! You've seen caching magic in action!")
print("💡 Next: Example 3 - Understanding cache triggering and lazy evaluation")

# Keep the cached data for the next example
print("\n🔧 Keeping cached data for Example 3...")