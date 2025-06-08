## Main Code

import findspark
from elasticsearch import Elasticsearch, helpers
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.types import TimestampType

# Define a UDF (User Defined Function) to parse the comma-separated string into a structured row
@F.udf(StructType([
    StructField("event_ts", StringType(), True),  # Timestamp of the event
    StructField("tem", StringType(), True),       # Temperature
    StructField("pir", StringType(), True),       # PIR sensor data (movement)
    StructField("lig", StringType(), True),       # Light level
    StructField("hum", StringType(), True),       # Humidity
    StructField("co2", StringType(), True),       # CO2 level
    StructField("room", StringType(), True)       # Room name or ID
]))
def parse_value(value):
    # Split the incoming Kafka message by comma and return as a tuple
    parts = value.split(",")
    return (parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6])

# Create a Spark session
spark = SparkSession.builder.appName("Read From Kafka").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')  # Set log level to ERROR

# Read streaming data from Kafka topic "test1"
lines = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "kafka:9092")
          .option("subscribe", "test1")
          .load())

# Deserialize Kafka key and value from binary to string
lines2 = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")

# Parse the 'value' column using the UDF and create structured columns
lines3 = lines2.withColumn("parsed_value", parse_value(F.col("value")))

# Flatten the parsed struct column into separate columns
result = lines3.select("parsed_value.*")

# Add a 'movement' column based on PIR sensor: if pir > 0 => "movement"
result = result.withColumn("movement", F.when(result["pir"] > 0, "movement").otherwise("NO movement"))

# Format event timestamp as string
result1 = result.withColumn("event_ts", F.date_format(F.col("event_ts"), "yyyy-MM-dd HH:mm:ss.SSS"))

# Convert the formatted string to Spark's timestamp type
result2 = result1.withColumn("event_ts", F.to_timestamp(F.col("event_ts"), "yyyy-MM-dd HH:mm:ss.SSS"))

# Define Elasticsearch index settings and mappings
online_retail_index_2 = {
    "settings": {
        "index": {
            "analysis": {
                "analyzer": {
                    "custom_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase", "custom_edge_ngram", "asciifolding"
                        ]
                    }
                },
                "filter": {
                    "custom_edge_ngram": {
                        "type": "edge_ngram",
                        "min_gram": 2,
                        "max_gram": 10
                    }
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "event_ts": {"type": "date"},          # Timestamp of the event
            "tem": {"type": "float"},              # Temperature
            "pir": {"type": "float"},              # PIR value
            "lig": {"type": "float"},              # Light level
            "hum": {"type": "float"},              # Humidity
            "co2": {"type": "float"},              # CO2 concentration
            "room": {"type": "keyword", "ignore_above": 256},  # Room identifier
            "movement": {"type": "keyword", "ignore_above": 256} # Movement label
        }
    }
}

# Create Elasticsearch client
es = Elasticsearch("http://es:9200")

# Index to be deleted if it already exists
index_to_delete = "online_retail_spark_5"

# Delete existing index if exists
if es.indices.exists(index=index_to_delete):
    es.indices.delete(index=index_to_delete)

# Create the new index with the defined settings
es.indices.create(index="online_retail_spark_5", body=online_retail_index_2)

# Function to write Spark DataFrame batches to Elasticsearch
def write_to_es(df, batchId):
    df.show()  # Print batch to console for debugging

    # Write DataFrame to Elasticsearch using elasticsearch-hadoop connector
    df.write \
      .format("org.elasticsearch.spark.sql") \
      .mode("append") \
      .option("es.nodes", "es") \
      .option("es.port", "9200") \
      .option("es.nodes.wan.only", "true") \
      .option("es.http.timeout", "5m") \
      .option("es.http.retries", "3") \
      .option("es.net.ssl", "false") \
      .option("es.net.ssl.cert.allow.self.signed", "false") \
      .option("es.nodes.discovery", "false") \
      .option("es.nodes.data.only", "false") \
      .option("es.nodes.client.only", "false") \
      .option("es.nodes.ingest.only", "false") \
      .option("es.nodes.master.only", "false") \
      .option("es.net.ssl.keystore.location", "false") \
      .option("es.net.ssl.keystore.pass", "false") \
      .option("es.net.ssl.truststore.location", "false") \
      .option("es.net.ssl.truststore.pass", "false") \
      .option("es.net.ssl.key.location", "false") \
      .option("es.net.ssl.cert.location", "false") \
      .option("es.http.crawl.concurrent.connections", "100") \
      .option("es.http.crawl.max.connections", "500") \
      .option("es.nodes.resolve.hostname", "true") \
      .option("es.index.read.missing.as.empty", "true") \
      .option("es.write.operation", "index") \
      .option("es.write.metadata", "true") \
      .option("es.batch.size.entries", "10000") \
      .option("es.batch.size.bytes", "10mb") \
      .option("es.batch.write.retry.count", "3") \
      .option("es.batch.write.retry.wait", "10s") \
      .option("es.batch.write.refresh", "false") \
      .option("es.batch.write.retry.handler", "abort") \
      .option("es.batch.write.retry.handler.abort", "io") \
      .option("es.batch.write.retry.handler.abort.io", "false") \
      .option("es.batch.write.retry.handler.abort.iowait", "false") \
      .option("es.batch.write.retry.handler.abort.retrycount", "retry") \
      .option("es.batch.write.retry.handler.abort.retryinterval", "1s") \
      .save("online_retail_spark_5")  # Elasticsearch index name

# Define checkpoint directory for Spark streaming state
checkpoint_dir = "file:///tmp/streaming/read_from_kafka"

# Start Spark Structured Streaming query
streamingQuery = (result2.writeStream
                     .outputMode("append")  # Append mode: only new rows
                     .foreachBatch(write_to_es)  # Write each micro-batch to ES
                     .option("checkpointLocation", checkpoint_dir)  # Checkpoint for fault-tolerance
                     .start())

# Wait for the streaming query to terminate
streamingQuery.awaitTermination()
