import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    mysql_host: str = os.getenv("MYSQL_HOST", "localhost")
    mysql_port: int = int(os.getenv("MYSQL_PORT", "3306"))
    mysql_user: str = os.getenv("MYSQL_USER", "pipeline")
    mysql_password: str = os.getenv("MYSQL_PASSWORD", "pipeline")
    mysql_db: str = os.getenv("MYSQL_DB", "retail")
    mysql_table: str = os.getenv("MYSQL_TABLE", "product")

    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    product_topic: str = os.getenv("PRODUCT_TOPIC", "product_updates")
    dlq_topic: str = os.getenv("DLQ_TOPIC", "product_dlq")
    transactional_id: str = os.getenv("TRANSACTIONAL_ID", "product-pipeline-producer")

    schema_registry_url: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    schema_path: Path = PROJECT_ROOT / os.getenv("SCHEMA_PATH", "schemas/product.avsc")

    checkpoint_path: Path = PROJECT_ROOT / os.getenv("CHECKPOINT_PATH", "state/producer_state.json")
    output_path: str = os.getenv("OUTPUT_PATH", str(PROJECT_ROOT / "data/output"))
    spark_checkpoint_path: str = os.getenv(
        "SPARK_CHECKPOINT_PATH", str(PROJECT_ROOT / "state/spark_checkpoint")
    )
    spark_app_name: str = os.getenv("SPARK_APP_NAME", "product-kafka-stream")

    kafka_partitions: int = int(os.getenv("KAFKA_PARTITIONS", "3"))
    kafka_replication_factor: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
    batch_size: int = int(os.getenv("BATCH_SIZE", "500"))
    poll_timeout: float = float(os.getenv("POLL_TIMEOUT", "1.0"))

    def ensure_local_dirs(self) -> None:
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        Path(self.spark_checkpoint_path).mkdir(parents=True, exist_ok=True)


settings = Settings()
