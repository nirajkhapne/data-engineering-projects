import json
from confluent_kafka import Producer
from producer.db_connector import fetch_incremental_data
from producer.checkpoint import get_last_ts, update_last_ts

def run():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    last_ts = get_last_ts()
    rows = fetch_incremental_data(last_ts)

    if not rows:
        print("No new data")
        return

    for row in rows:
        producer.produce(
            "product_updates",
            key=str(row["ID"]),
            value=json.dumps(row)
        )

    producer.flush()

    update_last_ts(max(r["last_updated"] for r in rows))
    print("Produced successfully")

if __name__ == "__main__":
    run()
