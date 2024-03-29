from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import types
from pyspark.sql.functions import to_date, current_timestamp
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform_with_spar(data, *args, **kwargs):
    
    spark = kwargs.get('spark')

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
        types.StructField("city", types.StringType(), True),
        # Add timestamp column to schema
        types.StructField("current_datetime", types.TimestampType(), True)
        # Add more fields as needed
    ])

    # Use the GCS URL directly
    gcs_file_path = 'gs://air-quality-project-417718-historical_data_bucket/air_quality_historical.csv'

    # Load the air quality data from CSV in GCS
    airquality_df = spark.read \
                    .option("header", "true") \
                    .schema(air_quality_schema) \
                    .csv(gcs_file_path)

    # Convert date string to date type
    air_quality_df = air_quality_df.withColumn("date", to_date("date"))

    # Add current timestamp to each row
    air_quality_df = air_quality_df.withColumn("current_datetime", current_timestamp())

    air_quality_df.show()

    air_quality_df.printSchema()

    result_df =air_quality_df.toPandas()

    return result_df
   

    # Stop Spark session
    spark.stop()

