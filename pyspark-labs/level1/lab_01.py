
# ==================================================================================
# Lab 1: PySpark Fundamentals & RDDs - Complete Code Guide
# Prerequisites: PySpark environment setup completed
# Difficulty: 🟢 Beginner
# ==================================================================================

"""
Learning Objectives:
- Understand Apache Spark architecture and core components
- Master RDD (Resilient Distributed Dataset) concepts and operations
- Implement basic transformations: map, filter, flatMap, distinct
- Execute basic actions: collect, count, take, reduce
- Understand lazy evaluation and RDD lineage
- Build a complete text processing application
"""


# ==================================================================================
# PART 1: SETUP AND SPARK CONTEXT CREATION
# ==================================================================================

# Import necessary libraries
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
import time
import string
import math

print("🚀 Starting Lab 1: PySpark Fundamentals & RDDs")
print("=" * 60)

# Set up Spark configuration
# - setAppName(): Sets the application name (visible in Spark UI)
# - setMaster(): Sets cluster mode - "local[*]" means local mode using all CPU cores
conf = SparkConf().setAppName("Lab1_Fundamentals").setMaster("local[*]")

# Create Spark Context (low-level API for RDD operations)
# SparkContext is the entry point for all Spark functionality
sc = SparkContext(conf=conf)

print("✅ Spark Context created successfully!")
print(f"🚀 Spark Version: {sc.version}")
print(f"🔧 Master: {sc.master}")
print(f"📱 App Name: {sc.appName}")
print(f"💾 Default Parallelism: {sc.defaultParallelism}")
print(f"📊 Spark UI available at: http://localhost:4040")
print()




# ==================================================================================
# PART 8: HANDS-ON PROJECT - TEXT PROCESSING & WORD COUNT
# ==================================================================================

print("📚 HANDS-ON PROJECT: Text Processing & Word Count")
print("=" * 52)

# Create comprehensive sample text
comprehensive_text = """Apache Spark is a unified analytics engine for large-scale data processing.
Spark provides an interface for programming entire clusters with implicit data parallelism.
RDDs are the fundamental data structure of Apache Spark.
Spark is fast because it uses in-memory computing capabilities.
Data processing with Spark is distributed across multiple nodes in a cluster.
Spark supports multiple programming languages including Python, Java, Scala, and R.
The Spark ecosystem includes Spark SQL, Spark Streaming, MLlib, and GraphX.
Spark can run on Hadoop, Apache Mesos, Kubernetes, standalone, or in the cloud.
Apache Spark was originally developed at UC Berkeley in 2009."""

# Write to file
with open("comprehensive_text.txt", "w") as f:
    f.write(comprehensive_text)

# Step 1: Read the text file
text_rdd = sc.textFile("comprehensive_text.txt")
print(f"📄 Loaded {text_rdd.count()} lines")

# Step 2: Extract words using flatMap
words_rdd = text_rdd.flatMap(lambda line: line.split())
print(f"📝 Total words (raw): {words_rdd.count()}")

# Step 3: Clean and normalize words
def clean_word(word):
    """Clean and normalize a word by removing punctuation and converting to lowercase"""
    # Remove punctuation and convert to lowercase
    cleaned = word.strip(string.punctuation).lower()
    return cleaned if cleaned and cleaned.isalpha() else None

clean_words_rdd = words_rdd.map(clean_word).filter(lambda word: word is not None)
print(f"🧹 Clean words: {clean_words_rdd.count()}")

# Step 4: Count word frequencies using reduceByKey
# First create (word, 1) pairs, then sum the counts
word_counts_rdd = clean_words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Step 5: Sort by frequency (descending)
sorted_counts = word_counts_rdd.sortBy(lambda x: x[1], ascending=False)

print(f"\n📊 Word Frequency Analysis:")
print("-" * 30)
for word, count in sorted_counts.collect():
    print(f"  '{word}': {count}")

# ==================================================================================
# ADVANCED TEXT ANALYSIS
# ==================================================================================

print(f"\n📈 Advanced Text Statistics:")
print("-" * 30)

# Calculate comprehensive statistics
unique_words = clean_words_rdd.distinct().count()
total_words = clean_words_rdd.count()
avg_word_length = clean_words_rdd.map(len).mean()

print(f"  Total words: {total_words}")
print(f"  Unique words: {unique_words}")
print(f"  Vocabulary richness: {unique_words/total_words:.2%}")
print(f"  Average word length: {avg_word_length:.1f} characters")

# Find longest and shortest words
longest_word = clean_words_rdd.reduce(lambda a, b: a if len(a) > len(b) else b)
shortest_word = clean_words_rdd.reduce(lambda a, b: a if len(a) < len(b) else b)

print(f"  Longest word: '{longest_word}' ({len(longest_word)} chars)")
print(f"  Shortest word: '{shortest_word}' ({len(shortest_word)} chars)")

# Word length distribution
word_lengths = clean_words_rdd.map(len)
length_distribution = word_lengths.map(lambda length: (length, 1)).reduceByKey(lambda a, b: a + b)
sorted_lengths = length_distribution.sortByKey()

print(f"\n📊 Word Length Distribution:")
print("-" * 28)
for length, count in sorted_lengths.collect():
    print(f"  {length} chars: {count} words")

# Find words containing specific patterns
spark_words = clean_words_rdd.filter(lambda word: 'spark' in word)
print(f"\n🔥 Words containing 'spark': {spark_words.collect()}")

# Create word pairs (bigrams) for more advanced analysis
lines_words = text_rdd.map(lambda line: line.split())
bigrams = lines_words.flatMap(
    lambda words: [(words[i].lower(), words[i+1].lower()) for i in range(len(words)-1)]
)
print(f"\n👥 Word pairs (bigrams) - first 5: {bigrams.take(5)}")
print()


# ==================================================================================


# Limitations of RDD
print("⚠️ Limitations of RDDs:")
print("- No built-in optimization (unlike DataFrames with Catalyst optimizer)")
print("- More verbose and lower-level API")
print("- Lack of schema enforcement")
print("- Less efficient for complex operations")
print("- Requires more manual tuning for performance")
print()
print("🎉 Lab 1 Completed Successfully!")


# Solutions to RDD limitations
print("💡 Solutions to RDD Limitations:")
print("- Use DataFrames or Datasets for structured data and optimizations")
print("- Leverage Spark SQL for complex queries")
print("- Use built-in functions and libraries for common tasks")
print("- Profile and tune Spark jobs using the Spark UI")
print("- Consider using higher-level APIs for better productivity")
print()


# DataFrame is a higher-level abstraction built on top of RDDs
# It provides a more user-friendly API and optimizations through the Catalyst optimizer
# DataFrames support a wide range of data formats and sources, making them more versatile for big data processing tasks
# They also allow for easier integration with SQL queries and machine learning libraries
# For structured data processing, DataFrames are generally preferred over RDDs due to their performance and ease of use
print("🔍 DataFrames vs RDDs:")


# ==================================================================================

# Stop Spark Context
sc.stop()
print("🛑 Spark Context stopped. Goodbye!")