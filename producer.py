import json
import logging
from datetime import datetime, timezone

from configs.settings import settings
from producer.checkpoint import get_checkpoint, update_checkpoint
from producer.db import create_connection, fetch_data

LOGGER = logging.getLogger(__name__)


def load_schema() -> str:
    return settings.schema_path.read_text(encoding="utf-8")


def normalize_row(row: dict) -> dict:
    """Convert database values into the types expected by the Avro schema."""
    timestamp = row["last_updated"]
    if isinstance(timestamp, datetime):
        timestamp_ms = int(timestamp.replace(tzinfo=timezone.utc).timestamp() * 1000)
    else:
        timestamp_ms = int(datetime.fromisoformat(str(timestamp)).replace(tzinfo=timezone.utc).timestamp() * 1000)

    return {
        "ID": int(row["ID"]),
        "name": str(row["name"]),
        "category": str(row["category"]),
        "price": float(row["price"]),
        "last_updated": timestamp_ms,
    }


def build_producer():
    from confluent_kafka import SerializingProducer
    from confluent_kafka.serialization import StringSerializer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer

    registry = SchemaRegistryClient({"url": settings.schema_registry_url})
    schema = load_schema()

    return SerializingProducer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "transactional.id": settings.transactional_id,
            "key.serializer": StringSerializer(),
            "value.serializer": AvroSerializer(registry, schema),
        }
    )


def run() -> int:
    settings.ensure_local_dirs()
    last_updated, last_id = get_checkpoint(settings.checkpoint_path)
    connection = create_connection()
    producer = build_producer()
    rows_sent = 0

    try:
        producer.init_transactions()

        for batch in fetch_data(connection, last_updated, last_id, settings.batch_size):
            producer.begin_transaction()
            try:
                for row in batch:
                    producer.produce(
                        topic=settings.product_topic,
                        key=str(row["ID"]),
                        value=normalize_row(row),
                    )
                    producer.poll(0)

                producer.commit_transaction()
            except Exception:
                producer.abort_transaction()
                raise

            last_row = batch[-1]
            last_updated = last_row["last_updated"]
            last_id = int(last_row["ID"])
            update_checkpoint(settings.checkpoint_path, last_updated, last_id)
            rows_sent += len(batch)

        LOGGER.info("Published %s product records", rows_sent)
        return rows_sent
    finally:
        connection.close()
        producer.flush(10)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
