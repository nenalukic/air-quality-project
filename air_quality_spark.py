import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import types
from pyspark.sql.functions import to_date

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AirQuaility") \
    .getOrCreate()

# Define schema for pollen data
air_quality_schema = types.StructType([
    types.StructType("date", types.DateType(), True),
    types.StructType("pm10", types.DoubleType(), True),
    types.StructType("pm2_5", types.DoubleType(), True),
    types.StructType("dust", types.DoubleType(), True),
    types.StructType("uv_index", types.DoubleType(), True),
    types.StructType("uv_index_clear_sky", types.DoubleType(), True),
    types.StructType("ammonia", types.DoubleType(), True),
    types.StructType("alder_pollen", types.DoubleType(), True),
    types.StructType("birch_pollen", types.DoubleType(), True),
    types.StructType("grass_pollen", types.DoubleType(), True),
    types.StructType("mugwort_pollen", types.DoubleType(), True),
    types.StructType("olive_pollen", types.DoubleType(), True),
    types.StructType("ragweed_pollen", types.DoubleType(), True),
    types.StructType("european_aqi", types.DoubleType(), True),
    types.StructType("city", types.StringType(), True)
    # Add more fields as needed
])

# Load air quality data from CSV
airquality_df = spark.read \
    .option("header", "true") \
    .schema(air_quality_schema) \
    .csv('air-quality-2024-03-21.csv')

# Convert date string to date type
airquality_df = airquality_df.withColumn("date", to_date("date"))

# Partition pollen data by day
airquality_df.write.partitionBy("date").parquet("airquality_partitioned")

# Stop Spark session
spark.stop()
