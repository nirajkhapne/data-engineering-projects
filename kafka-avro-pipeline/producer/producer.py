import os
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
        "transactional.id": "txn-1",
        "key.serializer": StringSerializer(),
        "value.serializer": AvroSerializer(schema_registry, schema_str)
    })

    producer.init_transactions()

    rows = fetch_data(get_last_ts())

    if not rows:
        return

    max_ts = None

    producer.begin_transaction()

    for row in rows:
        ts = row["last_updated"]

        if max_ts is None or ts > max_ts:
            max_ts = ts

        row["last_updated"] = int(ts.timestamp() * 1000)

        producer.produce("product_updates", key=str(row["ID"]), value=row)

    producer.commit_transaction()

    update_last_ts(max_ts.strftime("%Y-%m-%d %H:%M:%S"))

if __name__ == "__main__":
    run()
