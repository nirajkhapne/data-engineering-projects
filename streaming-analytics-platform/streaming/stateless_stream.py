from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

from schemas.user_schema import schema

spark = SparkSession.builder \
    .appName("StatelessStream") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_topic") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

filtered = parsed.filter(col("age") > 25)

query = filtered.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoints/stateless") \
    .start()

query.awaitTermination()
