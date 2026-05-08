import os
from dotenv import load_dotenv

from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from consumer.validator import validate
from consumer.mongo_writer import write_to_mongo
from consumer.dlq_producer import send_to_dlq

load_dotenv()

schema_registry = SchemaRegistryClient({
    "url": os.getenv("SCHEMA_REGISTRY_URL")
})

schema_str = open("schemas/logistics.avsc").read()

consumer = DeserializingConsumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP"),
    "group.id": "logistics_consumer_group",
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
    "key.deserializer": StringDeserializer(),
    "value.deserializer": AvroDeserializer(schema_registry, schema_str)
})

consumer.subscribe([os.getenv("KAFKA_TOPIC")])


while True:

    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print(msg.error())
        continue

    record = msg.value()

    try:

        validate(record)

        write_to_mongo(record)

        consumer.commit(message=msg)

        print("Inserted:", record["BookingID"])

    except Exception as e:

        send_to_dlq(record, str(e))

        print("DLQ:", e)
