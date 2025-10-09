

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# ---------------------------------------------------
# Example 1: Spark Streaming Application:  Word Count
# ---------------------------------------------------

spark = SparkSession \
        .builder \
        .appName("lab_01") \
        .master("local[3]") \
        .getOrCreate()
        
# read from socket        
lines_df=spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()     
    
lines_df.printSchema()    

# transformations
words_df=lines_df.select(explode(split(lines_df.value, " ")).alias("word"))
word_count_df=words_df.groupBy("word").count()

# write to console
query=word_count_df.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("checkpointLocation", "chk-point-dir") \
    .trigger(processingTime="10 seconds") \
    .start()
    
query.awaitTermination()     

