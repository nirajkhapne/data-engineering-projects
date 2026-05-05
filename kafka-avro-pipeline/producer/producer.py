import time
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from producer.db import fetch_data
from producer.checkpoint import get_last_ts, update_last_ts

import os
import json

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent to {msg.topic()} [{msg.partition()}]")

def retry_produce(producer, topic, key, value, retries=3):
    for i in range(retries):
        try:
            producer.produce(topic=topic, key=key, value=value)
            return
        except Exception as e:
            print(f"Retry {i+1} failed: {e}")
            time.sleep(2)
    raise Exception("Max retries exceeded")

def run():

    schema_registry = SchemaRegistryClient({
        "url": os.getenv("SCHEMA_REGISTRY_URL")
    })

    schema_str = open("schemas/product.avsc").read()

    avro_serializer = AvroSerializer(schema_registry, schema_str)

    producer = SerializingProducer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP"),
        "key.serializer": StringSerializer(),
        "value.serializer": avro_serializer
    })

    last_ts = get_last_ts()
    rows = fetch_data(last_ts)

    if not rows:
        print("No new data")
        return

    for row in rows:
        row["last_updated"] = int(row["last_updated"].timestamp() * 1000)

        retry_produce(
            producer,
            "product_updates",
            str(row["ID"]),
            row
        )

    producer.flush()

    max_ts = max(r["last_updated"] for r in rows)
    update_last_ts(max_ts.strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == "__main__":
    run()
