import pandas as pd
from kafka import KafkaConsumer
from time import sleep
import json
from s3fs import S3FileSystem

# Safe deserializer
def safe_deserializer(x):
    try:
        return json.loads(x.decode('utf-8'))
    except Exception as e:
        print("❌ Failed to decode message:", x)
        print("Error:", e)
        return None

# Kafka consumer configuration
consumer = KafkaConsumer(
    'kafka_ingestion',
    bootstrap_servers=['56.228.33.240:9092'],
    value_deserializer=safe_deserializer,
    auto_offset_reset='earliest',
    group_id='vs-code-consumer-001',
    enable_auto_commit=True
)

# S3 setup
s3 = S3FileSystem()

print("Kafka consumer connected. Reading and writing messages to S3...\n")

# Read messages and save to S3
for count, message in enumerate(consumer):
    if message.value is not None:
        print(f"✅ Received message {count}: {message.value}")
        try:
            s3_path = f"s3://mybucket12sroy/stock_market_{count}.json"
            with s3.open(s3_path, 'w') as file:
                json.dump(message.value, file)
            print(f"Saved to {s3_path}\n")
        except Exception as e:
            print(f" Failed to write to S3: {e}")
    else:
        print(f"Skipped invalid or empty message {count}.")
