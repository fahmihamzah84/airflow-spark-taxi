import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)

####################################
# Parameters
####################################
postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]

####################################
# Read Postgres
####################################
print("######################################")
print("READING POSTGRES TABLES")
print("######################################")


df_taxi = (
    spark.read
    .format("jdbc")
    .option("url", postgres_db)
    .option("dbtable", "public.taxi")
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .load()
)

####################################
##############Question##############
####################################

#Question 1 
# How many taxi trips were there on February 15?
question1 = (
        df_taxi
        .withColumn("pickup_datetime", to_date(col("pickup_datetime"),"yyyy-MM-dd")) 
        .groupBy("pickup_datetime").count() 
        .where(col("pickup_datetime") == "2021-02-15") 
)
#Question 2
#Find the longest trip for each day ?
df_taxi.createOrReplaceTempView('question')   
question2 = (
    spark.sql('SELECT dayofmonth(pickup_datetime) as day, MAX(MinutesDiff) \
                                        FROM question \
                                        GROUP BY day \
                                        ORDER BY day')
                                        )

#Question 3
# Find Top 5 Most frequent `dispatching_base_num` ?
question3 = (
    spark.sql('SELECT dispatching_base_num, COUNT(*) AS freq\
                                        FROM question \
                                        GROUP BY dispatching_base_num \
                                        ORDER BY freq DESC \
                                        LIMIT 5')
)

# Question 4 
# Find Top 5 Most common location pairs (PUlocationID and DOlocationID)
question4 = ( spark.sql('SELECT LEAST(PUlocationID, DOlocationID) AS first_loc , \
                              GREATEST(PUlocationID, DOlocationID) AS second_loc, \
                              COUNT(*) AS total_route \
                              FROM question \
                              WHERE PUlocationID != DOlocationID \
                              GROUP BY first_loc, second_loc \
                              ORDER BY total_route DESC \
                              LIMIT 5;'))

print("######################################")
print("EXECUTING QUERY AND SAVING RESULTS")
print("######################################")
# Save result to a CSV file

list = [question1, question2, question3, question4]

for items in list:
    items.coalesce(1).write.format("csv").mode("overwrite").save("/home/fahmihamzah84/data_processing/airflow-spark/", header=True)