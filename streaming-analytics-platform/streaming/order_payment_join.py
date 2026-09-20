from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, lit, expr

from configs.settings import settings
from mongodb.writer import upsert_order_batch
from schemas.order_schema import order_schema, payment_schema
from utils.logging import get_logger
from utils.spark import create_spark_session

logger = get_logger(__name__)


class OrderPaymentStreamJoin:
    """Join order and payment streams by order_id within a bounded event-time interval."""

    WATERMARK_DELAY = "10 minutes"
    MAX_EVENT_GAP = "10 minutes"

    def __init__(self, spark):
        self.spark = spark

    def read_stream(self, topic: str) -> DataFrame:
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )

    @staticmethod
    def parse_stream(df: DataFrame, event_schema: str | object, event_type: str) -> DataFrame:
        parsed = df.selectExpr("CAST(value AS STRING) AS value").select(
            from_json(col("value"), event_schema).alias("data")
        )
        return (
            parsed.filter(col("data").isNotNull())
            .select("data.*")
            .filter(col("order_id").isNotNull())
            .withColumn("event_type", lit(event_type))
        )

    def build_join(self, orders: DataFrame, payments: DataFrame) -> DataFrame:
        orders = orders.withWatermark("created_at_ts", self.WATERMARK_DELAY)
        payments = payments.withWatermark("created_at_ts", self.WATERMARK_DELAY)

        return (
            orders.join(
                payments,
                (
                    (orders.order_id == payments.order_id)
                    & (payments.created_at_ts >= orders.created_at_ts)
                    & (
                        payments.created_at_ts
                        <= orders.created_at_ts + expr(f"INTERVAL {self.MAX_EVENT_GAP}")
                    )
                ),
                "inner",
            )
            .select(
                orders.order_id.alias("order_id"),
                orders.order_date.alias("order_date"),
                orders.created_at.alias("created_at"),
                orders.customer_id.alias("customer_id"),
                orders.amount.alias("order_amount"),
                payments.payment_id.alias("payment_id"),
                payments.payment_date.alias("payment_date"),
                payments.amount.alias("payment_amount"),
            )
        )

    @staticmethod
    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        rows = [row.asDict() for row in batch_df.toLocalIterator()]
        written = upsert_order_batch(rows)
        logger.info("Micro-batch %s wrote %s order records to MongoDB", batch_id, written)

    def process(self):
        query = None
        try:
            orders = self.parse_stream(
                self.read_stream(settings.order_topic), order_schema, "order"
            ).withColumn("created_at_ts", col("created_at").cast("timestamp"))
            payments = self.parse_stream(
                self.read_stream(settings.payment_topic), payment_schema, "payment"
            ).withColumn("created_at_ts", col("created_at").cast("timestamp"))

            orders = orders.filter(col("created_at_ts").isNotNull())
            payments = payments.filter(col("created_at_ts").isNotNull())
            joined = self.build_join(orders, payments)

            query = (
                joined.writeStream
                .queryName("order-payment-stream-join")
                .outputMode("append")
                .option("checkpointLocation", settings.checkpoint_path("join"))
                .foreachBatch(self.write_batch)
                .start()
            )
            logger.info("Order-payment stream join started")
            return query
        except Exception:
            logger.exception("Failed to start order-payment stream join")
            if query is not None:
                query.stop()
            raise


def main() -> None:
    spark = create_spark_session("OrderPaymentStreamJoin")
    query = None
    try:
        query = OrderPaymentStreamJoin(spark).process()
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping order-payment stream join")
    except Exception:
        logger.exception("Order-payment stream join failed")
        raise
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
