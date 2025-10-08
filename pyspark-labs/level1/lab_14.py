

from pyspark.sql import *
from pyspark.sql import functions as f


#--------------------------------------------------------------
# Example 14:  Working with Window Functions
#--------------------------------------------------------------

spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("LogFileDemo") \
    .getOrCreate()


summary_df = spark.read.parquet("source/summary.parquet")

running_total_window=Window.partitionBy("Country") \
    .orderBy(f.col("WeekNumber")) \
    .rowsBetween(-1, Window.currentRow)

summary_df.withColumn("RunningTotal", f.sum("InvoiceValue").over(running_total_window)) \
    .show(10)