import time
import os
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from producer.db import fetch_data
from producer.checkpoint import get_last_ts, update_last_ts


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

    max_ts = None  # SAFE checkpoint tracking

    for row in rows:

        original_ts = row["last_updated"]  

        # track max timestamp BEFORE mutation
        if max_ts is None or original_ts > max_ts:
            max_ts = original_ts

        # convert only for Kafka
        row["last_updated"] = int(original_ts.timestamp() * 1000)

        retry_produce(
            producer,
            "product_updates",
            str(row["ID"]),
            row
        )

    producer.flush()

    # store checkpoint in MySQL format (NOT epoch)
    update_last_ts(max_ts.strftime("%Y-%m-%d %H:%M:%S"))

    print("Data published successfully")


if __name__ == "__main__":
    run()
