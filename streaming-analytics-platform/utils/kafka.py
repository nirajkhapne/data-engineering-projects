import json
from pathlib import Path
from typing import Any, Iterable

from kafka import KafkaProducer

from configs.settings import settings
from utils.logging import get_logger

logger = get_logger(__name__)


def create_producer() -> KafkaProducer:
    try:
        return KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=5,
            linger_ms=10,
        )
    except Exception:
        logger.exception("Failed to connect to Kafka at %s", settings.kafka_bootstrap)
        raise


def publish_records(topic: str, records: Iterable[dict[str, Any]]) -> None:
    producer = create_producer()
    try:
        for record in records:
            future = producer.send(topic, value=record)
            future.get(timeout=10)
            logger.info("Published record to %s: %s", topic, record)
        producer.flush()
    except Exception:
        logger.exception("Kafka publish failed for topic %s", topic)
        raise
    finally:
        producer.close()


def load_json_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    try:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc
