import json
import os
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

producer = Producer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP")
})


def send_to_dlq(record, error):

    payload = {
        "record": record,
        "error": error
    }

    producer.produce(
        os.getenv("KAFKA_DLQ_TOPIC"),
        value=json.dumps(payload)
    )

    producer.flush()
