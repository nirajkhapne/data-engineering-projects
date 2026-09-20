import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col

from configs.settings import settings
from spark.transform import transform_df

LOGGER = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName(settings.spark_app_name).getOrCreate()


def load_avro_schema() -> str:
    return settings.schema_path.read_text(encoding="utf-8")


def read_stream(spark: SparkSession):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", settings.product_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_stream(raw_df, schema_json: str):
    return raw_df.select(from_avro(col("value"), schema_json).alias("data")).select("data.*")


def write_stream(transformed_df):
    return (
        transformed_df.writeStream.format("parquet")
        .option("path", settings.output_path)
        .option("checkpointLocation", settings.spark_checkpoint_path)
        .outputMode("append")
        .start()
    )


def run() -> None:
    settings.ensure_local_dirs()
    spark = create_spark_session()
    try:
        raw_df = read_stream(spark)
        parsed_df = parse_stream(raw_df, load_avro_schema())
        transformed_df = transform_df(parsed_df)
        query = write_stream(transformed_df)
        LOGGER.info("Spark streaming query started")
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
