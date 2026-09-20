import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json

from configs.settings import settings
from schemas.user_schema import schema


logger = logging.getLogger(__name__)


class StatelessUserStream:
    """Read user events from Kafka, filter them, and expose the result."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_stream(self) -> DataFrame:
        """Create the Kafka streaming DataFrame."""
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
            .option("subscribe", "user_topic")
            .option("failOnDataLoss", "false")
            .load()
        )

    def parse_stream(self, df: DataFrame) -> DataFrame:
        """Convert Kafka JSON payloads into the user schema."""
        return (
            df.selectExpr("CAST(value AS STRING) AS value")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
            .filter(col("data").isNotNull())
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """Keep user events where the user is older than 25."""
        return df.filter(col("age") > 25)

    def write_stream(self, df: DataFrame):
        """Start the console sink for local inspection and testing."""
        return (
            df.writeStream
            .queryName("stateless-user-stream")
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option("checkpointLocation", "checkpoints/stateless")
            .start()
        )

    def process(self):
        """Build the stream in small, testable stages and start the query."""
        raw_df = self.read_stream()
        parsed_df = self.parse_stream(raw_df)
        filtered_df = self.transform(parsed_df)
        return self.write_stream(filtered_df)


def create_spark_session() -> SparkSession:
    """Create the Spark session used by this streaming job."""
    return (
        SparkSession.builder
        .appName("StatelessUserStream")
        .getOrCreate()
    )


def main() -> None:
    """Start the stream and keep the process alive until it is stopped."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    spark = None

    try:
        spark = create_spark_session()
        query = StatelessUserStream(spark).process()
        logger.info("Stateless user stream started.")
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping stateless user stream.")
    except Exception:
        logger.exception("Stateless user stream failed.")
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
