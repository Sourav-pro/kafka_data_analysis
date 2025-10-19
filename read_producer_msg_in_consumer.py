import pandas as pd
from kafka import KafkaConsumer
import json

def safe_deserializer(x):
    try:
        return json.loads(x.decode('utf-8'))
    except Exception as e:
        print(" Failed to decode message:", x)
        print("Error:", e)
        return None

consumer = KafkaConsumer(
    'kafka_ingestion',
    bootstrap_servers=['56.228.33.240:9092'],
    value_deserializer=safe_deserializer,
    auto_offset_reset='earliest',
    group_id='vs-code-consumer-001',
    enable_auto_commit=True
)

print("Kafka consumer connected. Waiting for messages...\n")

for c in consumer:
    if c.value is not None:
        print("Received message:", c.value)
    else:
        print("Skipped invalid or empty message.")
