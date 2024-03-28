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
    types.StructField("date", types.DateType(), True),
    types.StructField("temperature_2m", types.DoubleType(), True),
    types.StructField("relative_humidity_2m", types.DoubleType(), True),
    types.StructField("precipitation", types.DoubleType(), True),
    types.StructField("rain", types.DoubleType(), True),
    types.StructField("showers", types.DoubleType(), True),
    types.StructField("surface_pressure", types.DoubleType(), True),
    types.StructField("cloud_cover", types.DoubleType(), True),
    types.StructField("visibility", types.DoubleType(), True),
    types.StructField("wind_speed_10m", types.DoubleType(), True),
    types.StructField("wind_gusts_10m", types.DoubleType(), True),
    types.StructField("city", types.StringType(), True),
    # Add timestamp column to schema
    types.StructField("timestamp", types.TimestampType(), True)
    # Add more fields as needed
])

# Load pollen data from CSV
weather_df = spark.read \
    .option("header", "true") \
    .schema(weather_schema) \
    .csv('weather-2024-03-21.csv')

# Convert date string to date type
weather_df = weather_df.withColumn("date", to_date("event_date"))

weather_df.show()

# Partition weather data by day
weather_df.write.partitionBy("event_date").parquet("weather_historical_partitioned")

# Stop Spark session
spark.stop()