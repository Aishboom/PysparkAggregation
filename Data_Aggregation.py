from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, avg, min, max

# create a SparkSession
spark = SparkSession.builder.appName("TimeSeriesAggregation").getOrCreate()

# read the input dataset
input_data = spark.read.format("csv").option("header", "true").load("input.csv")

# calculate the time bucket based on the timestamp column
bucketed_data = input_data.withColumn("time_bucket", date_trunc("day", col("Timestamp")))

# aggregate by metric and time bucket, calculate average, minimum and maximum value for each metric and time bucket
aggregated_data = bucketed_data.groupBy(["Metric", "time_bucket"]).agg(avg("Value").alias("avg_value"), min("Value").alias("min_value"), max("Value").alias("max_value"))

# write the output dataset to a file
aggregated_data.write.format("csv").option("header", "true").mode("overwrite").save("output.csv")
