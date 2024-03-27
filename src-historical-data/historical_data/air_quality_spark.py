from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import to_date

def process_air_quality_data(csv_path, output_path):
    """
    Function to process air quality data and partition it by day.

    Parameters:
        csv_path (str): Path to the CSV file containing air quality data.
        output_path (str): Path to save the partitioned data.
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("AirQuality") \
        .getOrCreate()

    # Define schema for air quality data
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
        types.StructField("timestamp", types.TimestampType(), True)
        # Add more fields as needed
    ])

    # Load air quality data from CSV
    air_quality_df = spark.read \
        .option("header", "true") \
        .schema(air_quality_schema) \
        .csv(csv_path)

    # Convert date string to date type
    air_quality_df = air_quality_df.withColumn("date", to_date("event_date"))

    # Show loaded data
    air_quality_df.show()

    air_quality_df.printSchema()

    # Partition air quality data by day
    air_quality_df.write.partitionBy("event_date").parquet(output_path)

    # Stop Spark session
    spark.stop()

# Example usage
process_air_quality_data('air-quality-2024-03-21.csv', 'airquality_historical_partitioned')
