import os
from dotenv import load_dotenv
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from producer.csv_loader import load_csv

load_dotenv()


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Produced to {msg.topic()} [{msg.partition()}]")


schema_registry = SchemaRegistryClient({
    "url": os.getenv("SCHEMA_REGISTRY_URL")
})

schema_str = open("schemas/logistics.avsc").read()

producer = SerializingProducer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP"),
    "enable.idempotence": True,
    "acks": "all",
    "key.serializer": StringSerializer(),
    "value.serializer": AvroSerializer(schema_registry, schema_str)
})


def run():

    df = load_csv("data/logistics_data.csv")

    for _, row in df.iterrows():

        payload = {
            "BookingID": str(row["BookingID"]),
            "GpsProvider": row.get("GpsProvider"),
            "vehicle_no": row.get("vehicle_no"),
            "Origin_Location": row.get("Origin_Location"),
            "Destination_Location": row.get("Destination_Location"),
            "Current_Location": row.get("Current_Location"),
            "Material_Shipped": row.get("Material Shipped"),
            "Driver_Name": row.get("Driver_Name"),
            "Driver_MobileNo": str(row.get("Driver_MobileNo"))
        }

        producer.produce(
            topic=os.getenv("KAFKA_TOPIC"),
            key=payload["BookingID"],
            value=payload,
            on_delivery=delivery_report
        )

    producer.flush()


if __name__ == "__main__":
    run()
