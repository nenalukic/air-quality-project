from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, min, max, col, format_number

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TestColumnAggregations") \
    .getOrCreate()

# Read your partitioned data from Parquet files
data_df = spark.read.parquet("airquality_historical_partitioned")

# Select data for a specific day (replace 'yyyy-MM-dd' with your desired date)
specific_date = "2024-03-23"
filtered_data_df = data_df.filter(col("date") == specific_date)

# Aggregate air quality data by city
air_quality_agg_df = filtered_data_df.groupBy("city") \
    .agg(format_number(avg("pm10"), 2).alias("avg_pm10"))

air_quality_agg_df.show()

spark.stop()