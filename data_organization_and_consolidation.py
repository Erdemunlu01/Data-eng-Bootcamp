"""
Sensor Data Aggregation Script using PySpark

This script reads time-series sensor data from subfolders,
aggregates them to minute-level averages, and combines data from
multiple rooms (folders) into a unified DataFrame.

Author: [Your Name]
Date: [2025-06-08]
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, date_format, from_unixtime, lit
from pyspark.sql.types import StructType, StructField, StringType
import os

# Create Spark session
spark = SparkSession.builder \
    .appName("Sensor Data Processing") \
    .master("local[2]") \
    .getOrCreate()

# Main dataset directory
directory_path = "/opt/examples/datasets/KETI"

# Get list of folders (each represents a room)
folders = [f for f in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, f))]

# Define custom schema for empty DataFrame
custom_schema = StructType([
    StructField("event_ts_min", StringType(), True),
    StructField("_c1", StringType(), True)
])

# List to collect processed DataFrames from each room
df_fs = []

for folder in folders:
    folder_path = os.path.join(directory_path, folder)
    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    result1 = spark.createDataFrame([], schema=custom_schema)

    for csv in csv_files:
        csv_path = os.path.join(folder_path, csv)
        df = spark.read.csv(f"file:///{csv_path}")

        # Convert UNIX timestamp to "yyyy-MM-dd HH:mm"
        df1 = df.withColumn("event_ts_min", from_unixtime("_c0").cast("timestamp"))
        df2 = df1.withColumn("event_ts_min", date_format("event_ts_min", "yyyy-MM-dd HH:mm"))

        # Compute average per minute
        window_spec = Window.partitionBy("event_ts_min")
        sensor_col = csv[0:3]  # Extract sensor name from filename
        df3 = df2.withColumn(sensor_col, avg("_c1").over(window_spec))

        # Select timestamp and calculated average
        result = df3.select("event_ts_min", sensor_col).distinct().orderBy("event_ts_min")

        # Join with previous sensor values
        result1 = result.join(result1, "event_ts_min", "left")

    # Add room identifier
    result1 = result1.withColumn("room", lit(folder))
    df_fs.append(result1)

# Combine all room DataFrames
result_df = df_fs[0]
for df in df_fs[1:]:
    result_df = result_df.union(df)

# Show sample output
result_df.show(truncate=False)

# Save to CSV (optional)
output_path = "file:///opt/examples/datasets/processed_output"
result_df.coalesce(1).write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", True) \
    .save(output_path)