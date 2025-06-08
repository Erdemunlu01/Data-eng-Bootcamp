# Access the Kafka container
docker exec -it kafka bash

# Create a Kafka topic named 'test1' with:
# - 3 partitions (for parallelism and scalability)
# - replication factor of 1 (no redundancy, for development/testing environments)
/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test1 --partitions 3 --replication-factor 1

# List all Kafka topics to confirm the topic was created
/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

#!/bin/bash

######################################
# Step 1: Write to Kafka Manually
######################################

# Start Kafka Console Producer (manually send data to Kafka topic)
kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test1

######################################
# Step 2: Send CSV File to Kafka via Script
######################################

# This command sends data from a CSV file to the Kafka topic 'test1' using the dataframe_to_kafka.py script
# Parameters:
# -b : Kafka broker address
# -t : Kafka topic
# --input : Full path to the CSV file
# -r : Number of repetitions (how many times to re-send the file)
python dataframe_to_kafka.py -b kafka:9092 -t test1 --input "/opt/examples/datasets/daneme1/son_data_bina1.csv" -r 1

######################################
# Step 3: Download Data Generator Tool (in Spark container)
######################################

# Clone the data generator tool from GitHub
git clone https://github.com/erkansirin78/data-generator.git

# Navigate to the cloned directory
cd data-generator

######################################
# Step 4: Set Up Virtual Environment and Install Requirements
######################################

# Install virtualenv if not installed
python3 -m pip install virtualenv

# Create a virtual environment named 'datagen'
python3 -m virtualenv datagen

# Activate the virtual environment
source datagen/bin/activate

# Install required Python packages from requirements.txt
pip install -r requirements.txt