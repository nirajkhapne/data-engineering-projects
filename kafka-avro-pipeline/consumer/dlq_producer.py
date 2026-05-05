from confluent_kafka import Producer
import json
import os

producer = Producer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP")
})

def send_to_dlq(record, error):
    payload = {
        "record": record,
        "error": error
    }

    producer.produce(
        "product_dlq",
        value=json.dumps(payload)
    )

    producer.flush()
