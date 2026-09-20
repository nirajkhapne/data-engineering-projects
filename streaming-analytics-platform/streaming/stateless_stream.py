from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json

from configs.settings import settings
from schemas.user_schema import schema
from utils.logging import get_logger
from utils.spark import create_spark_session

logger = get_logger(__name__)


class StatelessUserStream:
    """Read user events from Kafka and filter them without maintaining state."""

    def __init__(self, spark):
        self.spark = spark

    def read_stream(self) -> DataFrame:
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
            .option("subscribe", settings.user_topic)
            .option("startingOffsets", "earliest")
            .load()
        )

    def parse_stream(self, df: DataFrame) -> DataFrame:
        parsed = df.selectExpr("CAST(value AS STRING) AS value").select(
            from_json(col("value"), schema).alias("data")
        )
        return parsed.filter(col("data").isNotNull()).select("data.*")

    def transform(self, df: DataFrame) -> DataFrame:
        return df.filter(col("age").isNotNull() & (col("age") > 25))

    def write_stream(self, df: DataFrame):
        return (
            df.writeStream
            .queryName("stateless-user-stream")
            .format("console")
            .outputMode("append")
            .option("truncate", False)
            .option("checkpointLocation", settings.checkpoint_path("stateless"))
            .start()
        )

    def process(self):
        query = None
        try:
            stream_df = self.read_stream()
            parsed_df = self.parse_stream(stream_df)
            filtered_df = self.transform(parsed_df)
            query = self.write_stream(filtered_df)
            logger.info("Stateless user stream started")
            return query
        except Exception:
            logger.exception("Failed to start stateless user stream")
            if query is not None:
                query.stop()
            raise


def main() -> None:
    spark = create_spark_session("StatelessUserStream")
    query = None
    try:
        query = StatelessUserStream(spark).process()
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping stateless user stream")
    except Exception:
        logger.exception("Stateless user stream failed")
        raise
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
