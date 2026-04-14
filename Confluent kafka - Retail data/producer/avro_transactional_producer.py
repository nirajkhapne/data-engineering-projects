import os
import logging
import pandas as pd
from dotenv import load_dotenv
from confluent_kafka import SerializingProducer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

load_dotenv()

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka-producer")

# -----------------------------
# Config
# -----------------------------
kafka_config = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET"),

    #   Reliability
    'enable.idempotence': True,
    'acks': 'all',
    'retries': 10,
    'max.in.flight.requests.per.connection': 5,

    #   Transactions
    'transactional.id': 'retail-producer-1',

    #   Performance
    'linger.ms': 50,
    'batch.size': 32768,
    'compression.type': 'snappy'
}

schema_registry_conf = {
    'url': os.getenv("SCHEMA_REGISTRY_URL"),
    'basic.auth.user.info': f"{os.getenv('SR_API_KEY')}:{os.getenv('SR_API_SECRET')}"
}

# -----------------------------
# Schema Registry
# -----------------------------
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
subject_name = 'retail_data_dev-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

key_serializer = StringSerializer('utf_8')
value_serializer = AvroSerializer(schema_registry_client, schema_str)

producer = SerializingProducer({
    **kafka_config,
    'key.serializer': key_serializer,
    'value.serializer': value_serializer
})

# -----------------------------
# Delivery Callback
# -----------------------------
def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Delivered to {msg.topic()} [{msg.partition()}]")

# -----------------------------
# DLQ Producer (separate topic)
# -----------------------------
def send_to_dlq(record, error_msg):
    try:
        producer.produce(
            topic="retail_data_dlq",
            key=str(record.get("CustomerID", "unknown")),
            value={
                "error": str(error_msg),
                "record": record
            }
        )
    except Exception as e:
        logger.error(f"DLQ failed: {e}")

# -----------------------------
# Load Data
# -----------------------------
df = pd.read_csv("retail_data.csv").fillna("null")
records = df.to_dict(orient="records")

topic = "retail_data_dev"

# -----------------------------
# Initialize Transactions
# -----------------------------
producer.init_transactions()

try:
    producer.begin_transaction()

    for row in records:

        try:
            producer.produce(
                topic=topic,
                key=str(row["CustomerID"]),
                value=row,
                on_delivery=delivery_report
            )

            producer.poll(0)

        #   Backpressure handling
        except BufferError:
            logger.warning("Buffer full → applying backpressure")
            producer.flush()

        #   Serialization / schema errors → DLQ
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            send_to_dlq(row, e)

    #   Commit transaction
    producer.commit_transaction()
    logger.info("Transaction committed successfully")

except KafkaException as e:
    logger.error(f"Transaction failed: {e}")
    producer.abort_transaction()

finally:
    producer.flush()
