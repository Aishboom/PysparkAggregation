from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import (
    from_utc_timestamp,
    from_unixtime,
    avg,
    floor,
    col,
    min,
    max,
    date_trunc
)

# Path to the input dataset
dataset_path = "temp_data.csv"

# Output path for the aggregated data
output_path = "file:/C:/Users/Rittik De/PycharmProjects/Hertz_Spark_Project/temp_output"

# Function to calculate the time bucket based on bucket size in minutes
def time_bucket(timestamp_col: Column, bucket_size_minutes: int):
    minutes = floor(timestamp_col.cast("long") / (bucket_size_minutes * 60)) * bucket_size_minutes
    return from_unixtime(minutes * 60).alias("time_bucket")

# Create a Spark session
spark = SparkSession.builder \
    .appName("Batch Aggregation Coding Challenge") \
    .getOrCreate()

# Read the input dataset as a DataFrame
df = spark.read.format("csv") \
    .option("header", "true") \
    .load(dataset_path)

# Convert the timestamp column to UTC timezone
df = df.withColumn("timestamp", from_utc_timestamp("timestamp", "UTC"))

# 1440 = 24 Hrs * 60 min = 1 Day
# Group the data by metric and time bucket, and calculate average, minimum, and maximum values
df_batch_agg = df.groupBy("metric", time_bucket(col("timestamp"), 1440).alias("time_bucket")) \
    .agg(avg("value"), min("value"), max("value")).orderBy(col("metric"), col("time_bucket"))

# Alternatively, you can use date_trunc to group by hour instead of a fixed time bucket
# df_batch_agg = df.groupBy("metric", date_trunc("hour", "timestamp").alias("time_bucket"))\
#     .agg(avg("value"), min("value"), max("value"))

# Show the aggregated results
df_batch_agg.show(truncate=False)

# Write the aggregated data to the specified output path
df_batch_agg.write.format("csv") \
    .option("header", "true") \
    .save(output_path)
