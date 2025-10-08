

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName("LazyEvaluation-Teaching") \
    .master("local[4]") \
    .config("spark.ui.enabled", "true") \
    .getOrCreate()

# Create datasets for examples
students_data = [
    (1, "Alice", "Computer Science", 85, 3.2, "Senior"),
    (2, "Bob", "Mathematics", 92, 3.8, "Junior"), 
    (3, "Charlie", "Physics", 78, 2.9, "Sophomore"),
    (4, "Diana", "Computer Science", 88, 3.5, "Senior"),
    (5, "Eve", "Mathematics", 95, 3.9, "Graduate"),
    (6, "Frank", "Physics", 82, 3.1, "Junior"),
    (7, "Grace", "Computer Science", 91, 3.7, "Senior"),
    (8, "Henry", "Mathematics", 76, 2.8, "Sophomore")
]

courses_data = [
    (101, "Intro to Programming", "Computer Science", 4),
    (201, "Data Structures", "Computer Science", 4),
    (301, "Calculus I", "Mathematics", 3),
    (401, "Physics Lab", "Physics", 2),
    (501, "Advanced Algorithms", "Computer Science", 4)
]

students = spark.createDataFrame(students_data, 
    ["student_id", "name", "major", "exam_score", "gpa", "year"])

courses = spark.createDataFrame(courses_data,
    ["course_id", "course_name", "department", "credits"])

print("Datasets created:")
students.show()
courses.show()

print("=== LAZY EVALUATION FUNDAMENTALS ===")

def demonstrate_lazy_evaluation_core():
    print("\n📖 Definition:")
    print("   Lazy Evaluation = Operations are recorded but not executed immediately")
    print("   Execution only happens when an action is triggered")
    
    print("\n🔍 Step-by-step demonstration:")
    
    # Step 1: Transformation (lazy)
    print("\n   Step 1: Create transformation")
    filtered_students = students.filter(col("gpa") > 3.0)
    print("   filtered_students = students.filter(gpa > 3.0)")
    print("   Status: Operation RECORDED, not executed")
    print("   Memory usage: Minimal (just metadata)")
    print("   Data processed: 0 rows")
    
    # Step 2: Another transformation (still lazy)
    print("\n   Step 2: Add another transformation")
    cs_students = filtered_students.filter(col("major") == "Computer Science")
    print("   cs_students = filtered_students.filter(major == 'CS')")
    print("   Status: Operation RECORDED, not executed")
    print("   Total operations recorded: 2")
    print("   Data processed: Still 0 rows")
    
    # Step 3: Action (triggers execution)
    print("\n   Step 3: Trigger execution with action")
    print("   cs_students.show()")
    print("   Status: NOW all operations execute together")
    
    start_time = time.time()
    cs_students.show()
    execution_time = time.time() - start_time
    
    print(f"   Execution time: {execution_time:.3f} seconds")
    print("   Result: Both filters applied in single optimized pass")
    
    print("\n💡 Key Points:")
    print("   • Transformations create execution plan without processing data")
    print("   • Multiple transformations are combined for optimization")
    print("   • Actions trigger the actual computation")
    print("   • All recorded operations execute together")

demonstrate_lazy_evaluation_core()


print("\n=== MEMORY AND PERFORMANCE IMPACT ===")

def demonstrate_performance_impact():
    print("\n📊 Memory usage during plan building vs execution:")
    
    import psutil
    import os
    
    def get_memory_mb():
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    
    # Baseline memory
    baseline_memory = get_memory_mb()
    print(f"\n   Baseline memory: {baseline_memory:.1f} MB")
    
    # Build complex transformation chain
    print("\n   Building complex transformation chain...")
    
    complex_chain = students \
        .filter(col("gpa") > 2.5) \
        .withColumn("performance_level", 
                   when(col("exam_score") > 90, "Excellent")
                   .when(col("exam_score") > 80, "Good")
                   .otherwise("Average")) \
        .groupBy("major", "performance_level") \
        .agg(count("*").alias("count"), 
             avg("gpa").alias("avg_gpa")) \
        .filter(col("count") > 1) \
        .orderBy("avg_gpa")
    
    after_planning_memory = get_memory_mb()
    planning_overhead = after_planning_memory - baseline_memory
    
    print(f"   After building plan: {after_planning_memory:.1f} MB")
    print(f"   Planning overhead: {planning_overhead:.1f} MB")
    print("   Operations in plan: 6 transformations")
    
    # Execute the plan
    print("\n   Executing the plan...")
    execution_start_memory = get_memory_mb()
    
    start_time = time.time()
    result = complex_chain.collect()
    execution_time = time.time() - start_time
    
    after_execution_memory = get_memory_mb()
    execution_overhead = after_execution_memory - execution_start_memory
    
    print(f"   After execution: {after_execution_memory:.1f} MB")
    print(f"   Execution overhead: {execution_overhead:.1f} MB")
    print(f"   Execution time: {execution_time:.3f} seconds")
    print(f"   Result rows: {len(result)}")
    
    print(f"\n📈 Analysis:")
    print(f"   Planning uses: {planning_overhead:.1f} MB")
    print(f"   Execution uses: {execution_overhead:.1f} MB")
    
    # Handle case where planning overhead might be 0 or negative
    if planning_overhead > 0:
        ratio = execution_overhead / planning_overhead
        print(f"   Ratio: Execution uses {ratio:.1f}x more memory than planning")
    else:
        print(f"   Planning overhead negligible (< 0.1 MB)")
        print(f"   Execution requires actual memory for data processing")
    
    print("   Conclusion: Plan building is very lightweight compared to execution")

demonstrate_performance_impact()


