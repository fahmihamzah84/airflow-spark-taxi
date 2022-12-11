import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import round as rounder
from pyspark.sql.types import DoubleType

# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)

####################################
# Parameters
####################################
taxi_file = "/usr/local/spark/resources/data/fhv_taxi.parquet"
postgres_db = "jdbc:postgresql://postgres/test"
postgres_user = "test"
postgres_pwd = "postgres"

####################################
# Read PARQUET Data
####################################

print("######################################")
print("READING PARQUET FILES")
print("######################################")

df_taxi = (
    spark.read
    .format("parquet") 
    .option("header", "true") 
    .load(taxi_file)
)

# Convert epoch to timestamp and rating to DoubleType
df_taxi_add_interval = (
    df_taxi
    .withColumn('SecondsDiff', unix_timestamp('dropOff_datetime') - unix_timestamp('pickup_datetime'))
    .withColumn('MinutesDiff', rounder(col('SecondsDiff')/60))
)
####################################
# Load data to Postgres
####################################
print("######################################")
print("LOADING POSTGRES TABLES")
print("######################################")

(
     df_taxi_add_interval
     .write
     .format("jdbc")
     .option("url", postgres_db)
     .option("dbtable", "public.taxi")
     .option("user", postgres_user)
     .option("password", postgres_pwd)
     .mode("overwrite")
     .save()
)