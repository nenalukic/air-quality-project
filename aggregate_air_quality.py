from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, min, max, format_number, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AirColumnAggregations") \
    .getOrCreate()

# Read your partitioned data from Parquet files
data_df = spark.read.parquet("airquality_historical_partitioned")

# Define the columns to aggregate
aggregate_cols = [col for col in data_df.columns if col not in ["date", "city"]]

# Perform aggregations and format number for all data columns
aggregated_df = data_df.groupBy("date", "city").agg(
    *(format_number(avg(col), 2).alias(f"avg_{col}") for col in aggregate_cols),  # Compute average for every column
    *(format_number(min(col), 2).alias(f"min_{col}") for col in aggregate_cols),  # Compute minimum for every column
    *(format_number(max(col), 2).alias(f"max_{col}") for col in aggregate_cols)   # Compute maximum for every column
)

# Write the aggregated DataFrame to a new Parquet file
aggregated_df.write.parquet("aggregated_air_quality_historical_data")

# Stop Spark session
spark.stop()
