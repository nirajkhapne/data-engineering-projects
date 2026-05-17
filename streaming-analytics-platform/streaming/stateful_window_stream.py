from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, window

from schemas.transaction_schema import schema
from configs.settings import settings

spark = SparkSession.builder \
    .appName("WindowedAggregation") \
    .getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP) \
    .option("subscribe", "transactions_topic") \
    .load()

parsed = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

watermarked = parsed.withWatermark("timestamp", "5 minutes")

windowed = watermarked.groupBy(
    window(col("timestamp"), "3 minutes"),
    col("user_id")
).agg(sum("amount").alias("total_amount"))

query = windowed.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "checkpoints/window") \
    .start()

query.awaitTermination()
