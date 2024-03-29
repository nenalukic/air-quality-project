import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import types
from pyspark.sql.functions import to_date
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform(data, *args, **kwargs):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("AirQuaility") \
        .getOrCreate()

    # Define schema for pollen data
    air_quality_schema = types.StructType([
        types.StructField("date", types.DateType(), True),
        types.StructField("pm10", types.DoubleType(), True),
        types.StructField("pm2_5", types.DoubleType(), True),
        types.StructField("dust", types.DoubleType(), True),
        types.StructField("uv_index", types.DoubleType(), True),
        types.StructField("uv_index_clear_sky", types.DoubleType(), True),
        types.StructField("ammonia", types.DoubleType(), True),
        types.StructField("alder_pollen", types.DoubleType(), True),
        types.StructField("birch_pollen", types.DoubleType(), True),
        types.StructField("grass_pollen", types.DoubleType(), True),
        types.StructField("mugwort_pollen", types.DoubleType(), True),
        types.StructField("olive_pollen", types.DoubleType(), True),
        types.StructField("ragweed_pollen", types.DoubleType(), True),
        types.StructField("european_aqi", types.DoubleType(), True),
        types.StructField("city", types.StringType(), True)
        # Add more fields as needed
    ])

    # Load air quality data from CSV
    airquality_df = spark.read \
        .option("header", "true") \
        .schema(air_quality_schema) \
        .csv('air-quality-2024-03-21.csv')

    # Convert date string to date type
    airquality_df = airquality_df.withColumn("date", to_date("date"))

    airquality_df.show()

    # Partition pollen data by day
    airquality_df.write.partitionBy("date").parquet("airquality_historical_partitioned")

    # Stop Spark session
    spark.stop()

