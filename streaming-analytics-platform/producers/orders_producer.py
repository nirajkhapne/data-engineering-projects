from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(1, 6):

    order = {
        "order_id": f"order_{i}",
        "order_date": str(datetime.now()),
        "created_at": str(datetime.now()),
        "customer_id": f"customer_{random.randint(1,10)}",
        "amount": random.randint(100, 1000)
    }

    producer.send("orders_topic", value=order)

    print("Published:", order)

    time.sleep(5)

producer.flush()
