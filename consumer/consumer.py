import json
from kafka import KafkaConsumer
import boto3
import time 

#Minio connection
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9002',
    aws_access_key_id='admin',
    aws_secret_access_key='password123',
     )

bucket_name = "bronze-transactions"

# Initialize the Kafka consumer
consumer = KafkaConsumer(
    "stock-quotes",
    bootstrap_servers=["host.docker.internal:29092"],
    enable_auto_commit=True,
    auto_offset_reset="earliest",
    group_id="bronze-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Consumer Streaming and storing to MinIO.....")

for message in consumer:
    record=message.value
    symbol=record.get("symbol")
    ts=record.get("fetched_at",int(time.time()))
    key=f"{symbol}/{ts}.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json"
    )
    print(f"Saved record for {symbol} at {ts} to MinIO as {key}")