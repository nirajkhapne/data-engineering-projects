from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from pyspark.sql.avro.functions import from_avro

from transform import transform_df

spark = SparkSession.builder \
    .appName("KafkaStream") \
    .getOrCreate()

schema = StructType([
    StructField("ID", IntegerType()),
    StructField("name", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("last_updated", LongType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "product_updates") \
    .load()

parsed = df.select(
        from_avro(col("value"), schema_json).alias("data")
    ).select("data.*")

transformed = transform_df(parsed)

query = transformed.writeStream \
    .format("parquet") \
    .option("path", "s3a://data-lake/products/") \
    .option("checkpointLocation", "s3a://data-lake/checkpoints/") \
    .start()

query.awaitTermination()
