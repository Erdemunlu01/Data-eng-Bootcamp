from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col
from pyspark.sql.types import FloatType

# Create a Spark session
spark = SparkSession.builder \
    .appName("DataFrame to Kafka from CSV") \
    .getOrCreate()

# Define the path to the CSV file
csv_path = "file:///opt/examples/datasets/processed_output/processed_output.csv"

# Read the CSV file
df = spark.read.option("header", "true").csv(csv_path)

# Optionally cast numeric columns to FloatType
df = df.withColumn("tem", col("tem").cast(FloatType())) \
       .withColumn("pir", col("pir").cast(FloatType())) \
       .withColumn("lig", col("lig").cast(FloatType())) \
       .withColumn("hum", col("hum").cast(FloatType())) \
       .withColumn("co2", col("co2").cast(FloatType()))

# Prepare the DataFrame for Kafka:
# - Use 'room' as the key
# - Convert all columns to JSON for the value
df_kafka = df.selectExpr("room as key") \
    .withColumn("value", to_json(struct([col(c) for c in df.columns]))) \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Write the DataFrame to Kafka topic 'test1'
df_kafka.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "test1") \
    .save()

print("Successfully sent data from CSV to Kafka topic 'test1'.")

# Stop the Spark session
spark.stop()