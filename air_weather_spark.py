import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import types

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AirQuailityWeatherCorrelation") \
    .getOrCreate()

# Load pollen data from CSV
airquality_df = spark.read.csv("air-quality-2024-03-21.csv", header=True, inferSchema=True)

# Load weather data from CSV
weather_df = spark.read.csv("weather-2024-03-21.csv", header=True, inferSchema=True)

# Join pollen and weather DataFrames
joined_df = airquality_df.join(weather_df, on="date", how="inner")

# Perform correlation analysis
correlation = joined_df.stat.corr("pollen_concentration", "weather_parameter")

# Print correlation coefficient
print("Correlation coefficient:", correlation)

# Stop Spark session
spark.stop()
