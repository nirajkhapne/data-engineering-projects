from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum

from schemas.transaction_schema import schema
from configs.settings import settings

spark = SparkSession.builder \
    .appName("StatefulGlobal") \
    .getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP) \
    .option("subscribe", "transactions_topic") \
    .load()

parsed = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

agg_df = parsed.groupBy("user_id") \
    .agg(sum("amount").alias("total_amount"))

query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("checkpointLocation", "checkpoints/global") \
    .start()

query.awaitTermination()
