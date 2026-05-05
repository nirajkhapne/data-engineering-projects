import os
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from producer.db import fetch_data
from producer.checkpoint import get_last_ts, update_last_ts

def run():

    schema_registry = SchemaRegistryClient({
        "url": os.getenv("SCHEMA_REGISTRY_URL")
    })

    schema_str = open("schemas/product.avsc").read()

    producer = SerializingProducer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP"),
        "enable.idempotence": True,
        "acks": "all",
        "retries": 5,
        "max.in.flight.requests.per.connection": 5,
        "transactional.id": "product-producer-1",
        "key.serializer": StringSerializer(),
        "value.serializer": AvroSerializer(schema_registry, schema_str)
    })

    producer.init_transactions()

    last_ts = get_last_ts()
    rows = fetch_data(last_ts)

    if not rows:
        print("No new data")
        return

    max_ts = None

    try:
        producer.begin_transaction()

        for row in rows:
            original_ts = row["last_updated"]

            if max_ts is None or original_ts > max_ts:
                max_ts = original_ts

            # convert for Avro
            row["last_updated"] = int(original_ts.timestamp() * 1000)

            producer.produce(
                topic="product_updates",
                key=str(row["ID"]),
                value=row
            )

        producer.commit_transaction()

        # update checkpoint ONLY after successful commit
        update_last_ts(max_ts.strftime("%Y-%m-%d %H:%M:%S"))

        print("Transaction committed")

    except Exception as e:
        print("Transaction failed:", e)
        producer.abort_transaction()


if __name__ == "__main__":
    run()
