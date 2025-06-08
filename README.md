# IoT Kafka Spark Elasticsearch Pipeline

This project demonstrates an end-to-end data pipeline for IoT sensor data using Kafka, Spark, and Elasticsearch. The pipeline includes:

- Kafka for streaming data ingestion
- PySpark for processing and transformation
- Elasticsearch for indexing and search
- Kibana for visualization

---

## 📂 Dataset

The project uses the **KETI IoT Sensor Dataset**, which contains readings from CO₂, temperature, humidity, light, and PIR motion sensors.

A publicly available version is hosted on Kaggle:  
https://www.kaggle.com/datasets/ranakrc/smart-building-system

This dataset was used to generate `processed_output.csv`.

---

## 🔧 Components

- `data_organization_and_consolidation.py`  
  Consolidates and organizes raw sensor data from CSV files.

- `dataframe_to_kafka.py`  
  Sends the processed data (CSV) to a Kafka topic.

- `read_kafka_writeES_withSpark.py`  
  Reads from Kafka, processes the data using PySpark, and writes to Elasticsearch.

- `Docker-Compose.yaml`  
  Sets up the services: Spark, Kafka, Elasticsearch, Kibana, PostgreSQL, and MinIO.

- `kafka_datagen_start.sh`  
  Script for setting up the Kafka producer and data generator.

- `processed_output.csv`  
  Sample processed IoT data to be streamed.

- `requirements.txt`  
  Python dependencies required for the data generator.

---
