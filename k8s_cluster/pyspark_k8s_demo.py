from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max, min as spark_min
import random
import os

def main():
    # Initialize Spark Session with K8s optimizations
    spark = SparkSession.builder \
        .appName("K8s Native PySpark Demo") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    print("🚀 Spark Session Created on Kubernetes!")
    print(f"Spark Version: {spark.version}")
    print(f"Application ID: {spark.sparkContext.applicationId}")
    print(f"Default Parallelism: {spark.sparkContext.defaultParallelism}")
    print(f"Running on: {os.environ.get('HOSTNAME', 'Unknown')}")
    
    # Create sample e-commerce data
    print("\n📊 Generating sample e-commerce data...")
    
    # Generate orders data
    orders_data = []
    products = ['laptop', 'phone', 'tablet', 'headphones', 'keyboard', 'mouse', 'monitor']
    categories = ['electronics', 'accessories', 'computers']
    
    for i in range(5000):
        orders_data.append({
            'order_id': f'ORD-{i:06d}',
            'customer_id': f'CUST-{random.randint(1, 1000):04d}',
            'product': random.choice(products),
            'category': random.choice(categories),
            'quantity': random.randint(1, 5),
            'price': round(random.uniform(50, 2000), 2),
            'discount': round(random.uniform(0, 0.3), 2),
            'region': random.choice(['North', 'South', 'East', 'West', 'Central'])
        })
    
    # Create DataFrame
    orders_df = spark.createDataFrame(orders_data)
    
    # Add calculated columns
    orders_df = orders_df.withColumn(
        'discounted_price', 
        col('price') * (1 - col('discount'))
    ).withColumn(
        'total_amount',
        col('discounted_price') * col('quantity')
    )
    
    print(f"✅ Created orders DataFrame with {orders_df.count()} rows")
    print(f"📊 Partitions: {orders_df.rdd.getNumPartitions()}")
    
    # Cache for multiple operations
    orders_df.cache()
    
    print("\n📋 Schema:")
    orders_df.printSchema()
    
    print("\n🔍 Sample orders:")
    orders_df.show(10, truncate=False)
    
    # Analytics - Sales by Category
    print("\n📈 Sales Analytics:")
    print("=" * 50)
    
    category_sales = orders_df.groupBy("category").agg(
        count("*").alias("total_orders"),
        avg("total_amount").alias("avg_order_value"),
        spark_max("total_amount").alias("max_order"),
        spark_min("total_amount").alias("min_order"),
        avg("quantity").alias("avg_quantity")
    ).orderBy(col("total_orders").desc())
    
    print("Sales by Category:")
    category_sales.show()
    
    # Regional Analysis
    regional_sales = orders_df.groupBy("region").agg(
        count("*").alias("orders"),
        avg("total_amount").alias("avg_amount"),
        (avg("total_amount") * count("*")).alias("total_revenue")
    ).orderBy(col("total_revenue").desc())
    
    print("Regional Sales Performance:")
    regional_sales.show()
    
    # Product Performance
    product_performance = orders_df.groupBy("product").agg(
        count("*").alias("units_sold"),
        avg("price").alias("avg_price"),
        avg("discount").alias("avg_discount"),
        (count("*") * avg("total_amount")).alias("revenue")
    ).orderBy(col("revenue").desc())
    
    print("Top Products by Revenue:")
    product_performance.show()
    
    # Customer Analysis
    customer_stats = orders_df.groupBy("customer_id").agg(
        count("*").alias("order_count"),
        avg("total_amount").alias("avg_spend")
    )
    
    top_customers = customer_stats.filter(col("order_count") >= 3).orderBy(
        col("avg_spend").desc()
    )
    
    print(f"High-value customers (3+ orders): {top_customers.count()}")
    top_customers.show(10)
    
    # Complex aggregation - Monthly trends simulation
    print("\n📊 Advanced Analytics:")
    
    # Simulate discount effectiveness
    discount_analysis = orders_df.selectExpr(
        "case when discount < 0.1 then 'Low' " +
        "when discount < 0.2 then 'Medium' " +
        "else 'High' end as discount_tier"
    ).groupBy("discount_tier").agg(
        count("*").alias("orders"),
        avg("quantity").alias("avg_quantity")
    )
    
    print("Discount Tier Analysis:")
    discount_analysis.show()
    
    # Performance metrics
    print("\n⚡ Performance Metrics:")
    print(f"Total records processed: {orders_df.count():,}")
    print(f"Cache status: {'Cached' if orders_df.is_cached else 'Not cached'}")
    
    # Cleanup
    orders_df.unpersist()
    
    print("\n🎉 K8s PySpark Demo completed successfully!")
    print("Check Spark UI for detailed execution metrics")
    
    spark.stop()

if __name__ == "__main__":
    main()