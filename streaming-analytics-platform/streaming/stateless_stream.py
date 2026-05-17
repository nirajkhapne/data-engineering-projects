from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

from schemas.user_schema import schema
from configs.settings import settings

spark = SparkSession.builder \
    .appName("StatelessStreaming") \
    .getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP) \
    .option("subscribe", "user_topic") \
    .load()

parsed = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

filtered = parsed.filter(col("age") > 25)

query = filtered.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoints/stateless") \
    .start()

query.awaitTermination()
