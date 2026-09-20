import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    """Runtime configuration shared by producers, streams, and the API."""

    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    mongo_uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db: str = os.getenv("MONGO_DB", "streaming_db")
    mongo_collection: str = os.getenv("MONGO_COLLECTION", "orders_fact")
    checkpoint_dir: Path = Path(os.getenv("CHECKPOINT_DIR", str(PROJECT_ROOT / "checkpoints")))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    user_topic: str = os.getenv("USER_TOPIC", "user_topic")
    transaction_topic: str = os.getenv("TRANSACTION_TOPIC", "transactions_topic")
    order_topic: str = os.getenv("ORDER_TOPIC", "orders_topic")
    payment_topic: str = os.getenv("PAYMENT_TOPIC", "payments_topic")

    spark_kafka_package: str = os.getenv(
        "SPARK_KAFKA_PACKAGE",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    )

    def checkpoint_path(self, name: str) -> str:
        path = self.checkpoint_dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        return str(path)


settings = Settings()
