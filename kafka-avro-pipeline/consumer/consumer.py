import os
import sys
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from consumer.transformer import transform
from consumer.writer import write_json
from consumer.dlq_producer import send_to_dlq
from consumer.dedup_store import load_ids, is_processed, mark_processed

def run(instance_id):

    schema_registry = SchemaRegistryClient({
        "url": os.getenv("SCHEMA_REGISTRY_URL")
    })

    schema_str = open("schemas/product.avsc").read()

    consumer = DeserializingConsumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP"),
        "group.id": "product_group",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "key.deserializer": StringDeserializer(),
        "value.deserializer": AvroDeserializer(schema_registry, schema_str)
    })

    consumer.subscribe(["product_updates"])

    processed_ids = load_ids()

    while True:
        msg = consumer.poll(1.0)

        if msg is None or msg.error():
            continue

        record = msg.value()
        record_id = record["ID"]

        # 🔥 DEDUP CHECK
        if is_processed(record_id, processed_ids):
            continue

        try:
            record = transform(record)

            write_json(record, instance_id)

            mark_processed(record_id, processed_ids)

            consumer.commit(message=msg)

        except Exception as e:
            send_to_dlq(record, str(e))


if __name__ == "__main__":
    run(sys.argv[1])
