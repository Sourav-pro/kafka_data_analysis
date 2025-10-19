import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps
import json


producer = KafkaProducer(bootstrap_servers=['56.228.33.240:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
future=producer.send('kafka_ingestion', value="{'Hello':'a'}")
try:
    record_metadata = future.get(timeout=10)
    print(f"Message sent to topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")
except Exception as e:
    print("Failed to send message:", e)

df = pd.read_csv("/Users/souravroy/Downloads/indexProcessed.csv") #Used local file instead of API
print(df.head(10).to_dict(orient='records')[0]) #Reading Sample 10 records so that the kafka server doesn't break
