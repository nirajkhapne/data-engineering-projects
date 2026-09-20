import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, sum

from configs.settings import settings
from schemas.transaction_schema import schema


logger = logging.getLogger(__name__)


class StatefulGlobalStream:
    """Maintain a running transaction total for each user."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_stream(self) -> DataFrame:
        """Create the Kafka streaming DataFrame for transaction events."""
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
            .option("subscribe", "transactions_topic")
            .option("failOnDataLoss", "true")
            .load()
        )

    def parse_stream(self, df: DataFrame) -> DataFrame:
        """Parse transaction JSON and drop malformed or incomplete records."""
        parsed_df = df.selectExpr("CAST(value AS STRING) AS value").select(
            from_json(col("value"), schema).alias("data")
        )

        return (
            parsed_df
            .filter(col("data").isNotNull())
            .select("data.*")
            .filter(col("user_id").isNotNull() & col("amount").isNotNull())
        )

    def aggregate(self, df: DataFrame) -> DataFrame:
        """Maintain the running transaction amount for each user."""
        return (
            df.groupBy("user_id")
            .agg(sum("amount").alias("total_amount"))
        )

    def write_stream(self, df: DataFrame):
        """Write the current aggregate state to the console."""
        return (
            df.writeStream
            .queryName("stateful-global-transactions")
            .format("console")
            .outputMode("complete")
            .option("truncate", "false")
            .option("checkpointLocation", "checkpoints/global")
            .start()
        )

    def process(self):
        """Build and start the stateful aggregation pipeline."""
        raw_df = self.read_stream()
        parsed_df = self.parse_stream(raw_df)
        aggregated_df = self.aggregate(parsed_df)
        return self.write_stream(aggregated_df)


def create_spark_session() -> SparkSession:
    """Create the Spark session used by this streaming job."""
    return (
        SparkSession.builder
        .appName("StatefulGlobalStream")
        .getOrCreate()
    )


def main() -> None:
    """Start the aggregation and keep it running until it is stopped."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    spark = None

    try:
        spark = create_spark_session()
        query = StatefulGlobalStream(spark).process()
        logger.info("Stateful global transaction stream started.")
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping stateful global transaction stream.")
    except Exception:
        logger.exception("Stateful global transaction stream failed.")
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
