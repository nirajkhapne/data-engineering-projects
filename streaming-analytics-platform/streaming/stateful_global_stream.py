from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, sum as spark_sum

from configs.settings import settings
from schemas.transaction_schema import schema
from utils.logging import get_logger
from utils.spark import create_spark_session

logger = get_logger(__name__)


class GlobalTransactionAggregation:
    """Maintain a running transaction total per user."""

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
            .filter(col("user_id").isNotNull() & col("amount").isNotNull())
        )

    def aggregate(self, df: DataFrame) -> DataFrame:
        return df.groupBy("user_id").agg(spark_sum("amount").alias("total_amount"))

    def write_stream(self, df: DataFrame):
        return (
            df.writeStream
            .queryName("global-transaction-aggregation")
            .format("console")
            .outputMode("complete")
            .option("truncate", False)
            .option("checkpointLocation", settings.checkpoint_path("global"))
            .start()
        )

    def process(self):
        query = None
        try:
            query = self.write_stream(self.aggregate(self.parse_stream(self.read_stream())))
            logger.info("Global transaction aggregation started")
            return query
        except Exception:
            logger.exception("Failed to start global transaction aggregation")
            if query is not None:
                query.stop()
            raise


def main() -> None:
    spark = create_spark_session("GlobalTransactionAggregation")
    query = None
    try:
        query = GlobalTransactionAggregation(spark).process()
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping global transaction aggregation")
    except Exception:
        logger.exception("Global transaction aggregation failed")
        raise
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
