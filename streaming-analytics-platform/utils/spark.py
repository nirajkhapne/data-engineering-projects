from pyspark.sql import SparkSession

from configs.settings import settings
from utils.logging import get_logger

logger = get_logger(__name__)


def create_spark_session(app_name: str) -> SparkSession:
    """Create a Spark session with the Kafka connector used by this project."""
    try:
        return (
            SparkSession.builder
            .appName(app_name)
            .config("spark.jars.packages", settings.spark_kafka_package)
            .getOrCreate()
        )
    except Exception:
        logger.exception("Failed to create Spark session for %s", app_name)
        raise
