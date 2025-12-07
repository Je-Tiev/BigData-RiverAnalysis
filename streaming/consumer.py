import os
import json
from kafka import KafkaConsumer
from pymongo import MongoClient

KAFKA_BOOTSTRAP = "localhost:29092"
KAFKA_TOPIC = "river_sensors"

# MongoDB exposed on host
MONGO_URI = "mongodb://root:password123@localhost:27017/"

# MongoDB
client = MongoClient(MONGO_URI)
db = client["river"]
collection = db["sensor_data"]

print("Connected to MongoDB at", MONGO_URI)

# Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    auto_offset_reset="earliest",
    enable_auto_commit=True, 
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print(f"Listening Kafka topic: {KAFKA_TOPIC}")

for message in consumer:
    data = message.value
    print("Received:", data)

    collection.insert_one(data)
    print("Inserted into MongoDB")
