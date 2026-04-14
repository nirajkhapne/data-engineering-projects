import os
import logging
import time
from dotenv import load_dotenv
from collections import defaultdict
from confluent_kafka import DeserializingConsumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from pymongo import MongoClient, errors, UpdateOne

load_dotenv()

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("retail-mongo-consumer")

# -----------------------------
# Kafka Consumer Config
# -----------------------------
consumer = DeserializingConsumer({
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET"),

    'group.id': 'retail-mongo-group',

    #  CRITICAL
    'enable.auto.commit': False,
    'auto.offset.reset': 'earliest'
})

# -----------------------------
# Schema Registry
# -----------------------------
schema_registry_client = SchemaRegistryClient({
    'url': os.getenv("SCHEMA_REGISTRY_URL"),
    'basic.auth.user.info': f"{os.getenv('SR_API_KEY')}:{os.getenv('SR_API_SECRET')}"
})

schema_str = schema_registry_client.get_latest_version('retail_data_dev-value').schema.schema_str

consumer._key_deserializer = StringDeserializer('utf_8')
consumer._value_deserializer = AvroDeserializer(schema_registry_client, schema_str)

consumer.subscribe(['retail_data_dev'])

# -----------------------------
# MongoDB Setup
# -----------------------------
mongo_client = MongoClient(os.getenv("MONGO_URI"))
db = mongo_client['retail_db']
collection = db['transactions']

#  Idempotency index
collection.create_index(
    [("Invoice", 1), ("StockCode", 1)],
    unique=True
)

# -----------------------------
# DLQ Producer
# -----------------------------
dlq_producer = Producer({
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP"),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv("KAFKA_API_KEY"),
    'sasl.password': os.getenv("KAFKA_API_SECRET"),

    # optional but recommended
    'compression.type': 'snappy',
    'linger.ms': 10
})

def send_to_dlq(msg, error):

    dlq_record = {
        "error": str(error),
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "payload": msg.value()
    }

    dlq_producer.produce(
        topic="retail_data_dlq",
        key=str(msg.key()),
        value=str(dlq_record)
    )
    dlq_producer.flush()

# -----------------------------
# Retry Logic
# -----------------------------
retry_tracker = defaultdict(int)
MAX_RETRIES = 3

# -----------------------------
# Batch Config
# -----------------------------
BATCH_SIZE = 100
batch = []

# -----------------------------
# Lag Monitoring
# -----------------------------
def log_lag(msg):
    logger.info(f"Offset: {msg.offset()} | Partition: {msg.partition()}")

# -----------------------------
# Consume Loop
# -----------------------------
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue

        record = msg.value()

        try:
            #  Basic validation
            if not record or not record.get("Invoice") or not record.get("StockCode"):
                consumer.commit(message=msg)
                continue

            batch.append((msg, record))

            #  Batch trigger
            if len(batch) >= BATCH_SIZE:

                operations = []
                messages = []

                for m, rec in batch:
                    operations.append(
                        UpdateOne(
                            {
                                "Invoice": rec["Invoice"],
                                "StockCode": rec["StockCode"]
                            },
                            {"$set": rec},
                            upsert=True   #  idempotent write
                        )
                    )
                    messages.append(m)

                try:
                    collection.bulk_write(operations, ordered=False)

                except errors.BulkWriteError as bwe:
                    logger.warning("Bulk write partial failure (duplicates handled)")

                #  Commit after successful batch
                for m in messages:
                    consumer.commit(message=m)

                logger.info(f"Processed batch of {len(batch)} records")
                batch.clear()

        except Exception as e:
            key = str(msg.key())
            retry_tracker[key] += 1

            if retry_tracker[key] <= MAX_RETRIES:
                logger.warning(f"Retry {retry_tracker[key]} for key {key}")
                time.sleep(1)
                continue
            else:
                logger.error("Max retries exceeded → DLQ")
                send_to_dlq(msg, e)
                consumer.commit(message=msg)

        log_lag(msg)

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
    mongo_client.close()
