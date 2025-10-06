
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

def test_pyspark_installation():
    try:
        # Create Spark session
        spark = SparkSession.builder \
            .appName("PySpark Installation Test") \
            .master("local[*]") \
            .getOrCreate()

        # Create a simple DataFrame
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["Name", "Age"]
        df = spark.createDataFrame(data, columns)

        print("✅ PySpark installation successful!")
        print("📊 Sample DataFrame:")
        df.show()

        print(f"🚀 Spark Version: {spark.version}")
        print(f"🐍 Python Version: {spark.sparkContext.pythonVer}")

        # Stop Spark session
        spark.stop()
        return True

    except Exception as e:
        print(f"❌ PySpark installation failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_pyspark_installation()
