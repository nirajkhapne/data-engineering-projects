from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from consumer.transformer import transform
from consumer.writer import write_json
from consumer.dlq_producer import send_to_dlq

import os
import sys

def run(instance_id):

    schema_registry = SchemaRegistryClient({
        "url": os.getenv("SCHEMA_REGISTRY_URL")
    })

    schema_str = open("schemas/product.avsc").read()

    avro_deserializer = AvroDeserializer(schema_registry, schema_str)

    consumer = DeserializingConsumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP"),
        "group.id": "product_group",
        "auto.offset.reset": "earliest",
        "key.deserializer": StringDeserializer(),
        "value.deserializer": avro_deserializer
    })

    consumer.subscribe(["product_updates"])

    while True:
        msg = consumer.poll(1.0)

        if msg is None or msg.error():
            continue

        try:
            record = transform(msg.value())

            write_json(record, instance_id)

        except Exception as e:
            send_to_dlq(msg.value(), str(e))


if __name__ == "__main__":
    run(sys.argv[1])
