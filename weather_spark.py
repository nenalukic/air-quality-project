import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import types
from pyspark.sql.functions import to_date

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Weather") \
    .getOrCreate()

# Define schema for pollen data
weather_schema = types.StructType([
    types.StructType("date", types.DateType(), True),
    types.StructType("temperature_2m", types.DoubleType(), True),
    types.StructType("relative_humidity_2m", types.DoubleType(), True),
    types.StructType("precipitation", types.DoubleType(), True),
    types.StructType("rain", types.DoubleType(), True),
    types.StructType("showers", types.DoubleType(), True),
    types.StructType("surface_pressure", types.DoubleType(), True),
    types.StructType("cloud_cover", types.DoubleType(), True),
    types.StructType("visibility", types.DoubleType(), True),
    types.StructType("wind_speed_10m", types.DoubleType(), True),
    types.StructType("wind_gusts_10m", types.DoubleType(), True),
    types.StructType("city", types.StringType(), True)
    # Add more fields as needed
])

# Load pollen data from CSV
weather_df = spark.read \
    .option("header", "true") \
    .schema(weather_schema) \
    .csv('weather-2024-03-21.csv')

# Convert date string to date type
weather_df = weather_df.withColumn("date", to_date("date"))

# Partition weather data by day
weather_df.write.partitionBy("date").parquet("weather_data_partitioned")

# Stop Spark session
spark.stop()