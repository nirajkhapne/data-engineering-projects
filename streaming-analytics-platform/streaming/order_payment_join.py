from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

from schemas.order_schema import order_schema, payment_schema
from configs.settings import settings

spark = SparkSession.builder \
    .appName("OrderPaymentJoin") \
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
    ) \
    .getOrCreate()

orders = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP) \
    .option("subscribe", "orders_topic") \
    .load()

payments = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP) \
    .option("subscribe", "payments_topic") \
    .load()

orders_df = orders.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), order_schema).alias("data")) \
    .select("data.*") \
    .withWatermark("created_at", "10 minutes")

payments_df = payments.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), payment_schema).alias("data")) \
    .select("data.*") \
    .withWatermark("created_at", "10 minutes")

joined = orders_df.join(
    payments_df,
    "order_id"
)

query = joined.writeStream \
    .format("mongodb") \
    .option("spark.mongodb.connection.uri", settings.MONGO_URI) \
    .option("spark.mongodb.database", settings.MONGO_DB) \
    .option("spark.mongodb.collection", settings.MONGO_COLLECTION) \
    .option("checkpointLocation", "checkpoints/join") \
    .outputMode("append") \
    .start()

query.awaitTermination()
