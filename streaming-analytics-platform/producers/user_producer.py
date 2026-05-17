from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open("data/user_data.json") as f:

    for line in f:

        record = json.loads(line)

        producer.send("user_topic", value=record)

        print("Published:", record)

        time.sleep(2)

producer.flush()
