# ========================================
# Caching Example 3: Triggering Cache
# ========================================

# Understanding when caching actually happens and how to control it

print("🔄 Understanding Cache Triggering and Lazy Evaluation")

# NOTE: This continues from Example 2 with the same Spark session
# If running independently, uncomment the setup section:

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand
import time

spark = SparkSession.builder \
    .appName("CachingExample3") \
    .master("local[*]") \
    .getOrCreate()

print("\n🧪 Experiment 1: What happens when you just call .cache()?")

# Create a fresh DataFrame for this experiment
fresh_data = spark.range(1, 100000) \
    .withColumn("doubled", col("id") * 2) \
    .withColumn("category", when(col("id") % 2 == 0, "Even").otherwise("Odd"))

print("📊 Created fresh DataFrame")

# Mark for caching
print("\n🔧 Step 1: Calling .cache()...")
cached_fresh = fresh_data.cache()

print("✅ .cache() called")
print(f"   Is cached? {cached_fresh.is_cached}")  # This will be True
print("   But is any data actually in memory yet? Let's check...")

# Check Spark UI or storage info (this is more complex, so we'll use a simple approach)
print("\n🔍 Step 2: Checking if cache was actually built...")

# Run a simple transformation (lazy - shouldn't trigger caching)
print("🔧 Adding a transformation (lazy operation)...")
transformed = cached_fresh.withColumn("tripled", col("id") * 3)
print("✅ Transformation added")
print("   Has caching been triggered? NO - transformations are lazy!")

print("\n⚡ Step 3: Triggering cache with an ACTION...")

# Different actions and their cache-triggering behavior
actions = [
    ("count()", lambda df: df.count()),
    ("first()", lambda df: df.first()),
    ("take(3)", lambda df: df.take(3)),
    ("collect()", lambda df: len(df.collect())),  # Using len() to get count
]

for action_name, action_func in actions:
    print(f"\n🔄 Trying {action_name}:")
    start_time = time.time()
    
    # First time - should build cache
    result = action_func(cached_fresh)
    first_time = time.time() - start_time
    
    # Second time - should use cache  
    start_time = time.time()
    result2 = action_func(cached_fresh)
    second_time = time.time() - start_time
    
    print(f"   First call:  {first_time:.3f}s (builds cache)")
    print(f"   Second call: {second_time:.3f}s (uses cache)")
    
    if second_time < first_time:
        speedup = first_time / second_time
        print(f"   ⚡ Speedup: {speedup:.1f}x faster!")
    else:
        print(f"   📊 Both calls very fast (cache working)")

print(f"\n🧪 Experiment 2: Multiple cache() calls")

# What happens if you call cache() multiple times?
print("\n🔧 Calling .cache() multiple times on same DataFrame...")

multi_cached = fresh_data.cache().cache().cache()
print("✅ Called .cache() three times")
print(f"   Is cached? {multi_cached.is_cached}")
print("   Result: Multiple .cache() calls are safe - no effect after first")

print(f"\n🧪 Experiment 3: Transformations after cache()")

# Key question: When should you call .cache() in your pipeline?
print("\n🔍 Testing: cache() BEFORE vs AFTER transformations...")

# Scenario A: Cache early (less efficient)
print("\n📊 Scenario A: Cache BEFORE expensive transformations")
data_a = spark.range(1, 50000).cache()  # Cache raw data
start_time = time.time()
result_a = data_a.withColumn("expensive", col("id") * col("id")) \
               .filter(col("expensive") > 1000000) \
               .count()
time_a = time.time() - start_time
print(f"   Result: {result_a:,} rows in {time_a:.3f}s")
print("   ❌ Inefficient: Cached raw data, then applied filters")

# Scenario B: Cache after transformations (more efficient)  
print("\n📊 Scenario B: Cache AFTER expensive transformations")
data_b = spark.range(1, 50000) \
            .withColumn("expensive", col("id") * col("id")) \
            .filter(col("expensive") > 1000000) \
            .cache()  # Cache filtered result
start_time = time.time()
result_b = data_b.count()
time_b = time.time() - start_time
print(f"   Result: {result_b:,} rows in {time_b:.3f}s")
print("   ✅ Efficient: Applied filters first, then cached result")

print(f"\n🎯 Cache Placement Best Practice:")
best_practice = """
❌ BAD:  raw_data.cache().filter().groupBy()...
✅ GOOD: raw_data.filter().groupBy()...cache()

Why? Cache the RESULT of expensive operations, not the input!
"""
print(best_practice)

print(f"\n🧪 Experiment 4: Cache vs No Cache in pipelines")

# Compare two identical pipelines - one with cache, one without
print("\n⚡ Pipeline comparison: Multiple uses of processed data")

# Pipeline without caching
print("\n🐌 Pipeline WITHOUT caching:")
base_data = spark.range(1, 80000) \
    .withColumn("value", col("id") * 3) \
    .withColumn("category", when(col("id") % 4 == 0, "A")
                           .when(col("id") % 4 == 1, "B") 
                           .when(col("id") % 4 == 2, "C")
                           .otherwise("D"))

# Multiple operations without caching
operations_without_cache = []

start_time = time.time()
count_A = base_data.filter(col("category") == "A").count()
operations_without_cache.append(time.time() - start_time)

start_time = time.time()
count_B = base_data.filter(col("category") == "B").count()
operations_without_cache.append(time.time() - start_time)

start_time = time.time()
avg_value = base_data.agg({"value": "avg"}).collect()[0][0]
operations_without_cache.append(time.time() - start_time)

total_without_cache = sum(operations_without_cache)
print(f"   Category A: {count_A:,} ({operations_without_cache[0]:.3f}s)")
print(f"   Category B: {count_B:,} ({operations_without_cache[1]:.3f}s)")
print(f"   Avg value: {avg_value:.1f} ({operations_without_cache[2]:.3f}s)")
print(f"   Total time: {total_without_cache:.3f}s")

# Pipeline with caching
print("\n⚡ Pipeline WITH caching:")
cached_base_data = spark.range(1, 80000) \
    .withColumn("value", col("id") * 3) \
    .withColumn("category", when(col("id") % 4 == 0, "A")
                           .when(col("id") % 4 == 1, "B") 
                           .when(col("id") % 4 == 2, "C")
                           .otherwise("D")) \
    .cache()

# Trigger cache
cached_base_data.count()

# Multiple operations with caching
operations_with_cache = []

start_time = time.time()
count_A_cached = cached_base_data.filter(col("category") == "A").count()
operations_with_cache.append(time.time() - start_time)

start_time = time.time()
count_B_cached = cached_base_data.filter(col("category") == "B").count()
operations_with_cache.append(time.time() - start_time)

start_time = time.time()
avg_value_cached = cached_base_data.agg({"value": "avg"}).collect()[0][0]
operations_with_cache.append(time.time() - start_time)

total_with_cache = sum(operations_with_cache)
print(f"   Category A: {count_A_cached:,} ({operations_with_cache[0]:.3f}s)")
print(f"   Category B: {count_B_cached:,} ({operations_with_cache[1]:.3f}s)")
print(f"   Avg value: {avg_value_cached:.1f} ({operations_with_cache[2]:.3f}s)")
print(f"   Total time: {total_with_cache:.3f}s")

if total_with_cache > 0:
    speedup = total_without_cache / total_with_cache
    print(f"   🚀 Speedup: {speedup:.1f}x faster with caching!")

print(f"\n🎯 Cache Triggering Rules:")
rules = """
🔄 LAZY OPERATIONS (don't trigger cache):
   • .select(), .filter(), .withColumn()
   • .groupBy(), .join(), .orderBy()
   • Any transformation that returns a DataFrame

⚡ EAGER OPERATIONS (trigger cache):
   • .count(), .collect(), .take()
   • .first(), .show(), .write()
   • Any action that returns actual data

💡 Best Practice:
   • Call .cache() AFTER expensive transformations
   • Trigger with a lightweight action like .count()
   • Use cached DataFrame for multiple subsequent operations
"""
print(rules)

print(f"\n🧠 Cache Lifecycle:")
lifecycle = """
1️⃣ Mark for caching:     df.cache()
2️⃣ Trigger caching:      df.count() (or any action)
3️⃣ Use cached data:      Multiple operations on df
4️⃣ Remove cache:         df.unpersist() (optional)

🔄 Memory flow:
   Raw Data → Transformations → Cache Storage → Fast Access
"""
print(lifecycle)

print(f"\n⚠️ Common Mistakes:")
mistakes = """
❌ Calling .cache() but never triggering with action
❌ Caching raw data instead of processed results  
❌ Not checking if cache actually improved performance
❌ Forgetting to unpersist when done (memory leaks)
✅ Cache after expensive operations, trigger immediately
"""
print(mistakes)

print(f"\n🔧 Practical Cache Triggering Pattern:")
pattern = """
# Recommended pattern:
processed_data = raw_data \\
    .filter(expensive_condition) \\
    .withColumn("complex_calculation", ...) \\
    .cache()

# Immediately trigger caching:
record_count = processed_data.count()
print(f"Cached {record_count:,} records")

# Now use for multiple operations:
result1 = processed_data.groupBy("category").count()
result2 = processed_data.agg({"value": "avg"})
result3 = processed_data.filter(col("score") > threshold)
"""
print(pattern)

print("\n✅ Example 3 complete! You understand cache triggering and lazy evaluation.")
print("💡 Next: Example 4 - MEMORY_ONLY storage level deep dive")

# Clean up some DataFrames to free memory
fresh_data.unpersist()
base_data.unpersist() if 'base_data' in locals() else None
cached_base_data.unpersist()

print("\n🧹 Cleaned up temporary caches for next example")