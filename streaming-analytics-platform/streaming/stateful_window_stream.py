from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, sum as spark_sum, window

from configs.settings import settings
from schemas.transaction_schema import schema
from utils.logging import get_logger
from utils.spark import create_spark_session

logger = get_logger(__name__)


class WindowedTransactionAggregation:
    """Aggregate transactions by user and event-time window."""

    WINDOW_DURATION = "3 minutes"
    WATERMARK_DELAY = "5 minutes"

    def __init__(self, spark):
        self.spark = spark

    def read_stream(self) -> DataFrame:
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
            .option("subscribe", settings.transaction_topic)
            .option("startingOffsets", "earliest")
            .load()
        )

    def parse_stream(self, df: DataFrame) -> DataFrame:
        parsed = df.selectExpr("CAST(value AS STRING) AS value").select(
            from_json(col("value"), schema).alias("data")
        )
        return (
            parsed.filter(col("data").isNotNull())
            .select("data.*")
            .filter(
                col("user_id").isNotNull()
                & col("amount").isNotNull()
                & col("timestamp").isNotNull()
            )
        )

    def aggregate(self, df: DataFrame) -> DataFrame:
        return (
            df.withWatermark("timestamp", self.WATERMARK_DELAY)
            .groupBy(window(col("timestamp"), self.WINDOW_DURATION), col("user_id"))
            .agg(spark_sum("amount").alias("total_amount"))
            .select("user_id", "window.start", "window.end", "total_amount")
        )

    def write_stream(self, df: DataFrame):
        return (
            df.writeStream
            .queryName("windowed-transaction-aggregation")
            .format("console")
            .outputMode("update")
            .option("truncate", False)
            .option("checkpointLocation", settings.checkpoint_path("window"))
            .start()
        )

    def process(self):
        query = None
        try:
            query = self.write_stream(self.aggregate(self.parse_stream(self.read_stream())))
            logger.info("Windowed transaction aggregation started")
            return query
        except Exception:
            logger.exception("Failed to start windowed transaction aggregation")
            if query is not None:
                query.stop()
            raise


def main() -> None:
    spark = create_spark_session("WindowedTransactionAggregation")
    query = None
    try:
        query = WindowedTransactionAggregation(spark).process()
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping windowed transaction aggregation")
    except Exception:
        logger.exception("Windowed transaction aggregation failed")
        raise
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
