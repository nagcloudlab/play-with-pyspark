

from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf().setAppName("lab_02").setMaster("local[*]")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Load the data ( from any source & any format of data )

survey_df=spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("./data/survey.csv")
    
# survey_df.printSchema()  
# survey_df.show()  

print(survey_df.rdd.getNumPartitions())

# repartition the data
survey_df=survey_df.repartition(2)

print(survey_df.rdd.getNumPartitions())

print(survey_df.rdd.glom().map(len).collect())


# Transformations 
# in 2 ways we can write the transformation
# 1. using SQL
# 2. using DataFrame API

# Select Age , Country where Age < 40 and group by Country 

# 1. using SQL

# survey_df.createOrReplaceTempView("survey_tbl")
# result_df=spark.sql("""
#     select Country, count(*) as total_count 
#     from survey_tbl 
#     where Age < 40 
#     group by Country
# """)

# show the result
# result_df.show()

# 2. using DataFrame API
result_df=survey_df \
    .select("Age","Country") \
    .where("Age < 40") \
    .groupBy("Country") \
    .count() 
# show the result
# result_df.show()
result_df.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save("./data/survey_count_by_country_df")
    
# // for spark UI
input("Press Enter to continue...")   
    
# stop the spark session
spark.stop()

    





