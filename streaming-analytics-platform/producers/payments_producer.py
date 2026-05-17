from kafka import KafkaProducer
import json
import random
import time
import uuid
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(1, 6):

    payment = {
        "payment_id": str(uuid.uuid4()),
        "order_id": f"order_{i}",
        "payment_date": str(datetime.now()),
        "created_at": str(datetime.now()),
        "amount": random.randint(100, 1000)
    }

    producer.send("payments_topic", value=payment)

    print("Published:", payment)

    time.sleep(7)

producer.flush()
