"""Batch and Auto Loader ingestion into Bronze Delta tables."""

from pyspark.sql import functions as F
from .schemas import CUSTOMER_SCHEMA, ORDER_SCHEMA, PAYMENT_SCHEMA, ADDRESS_SCHEMA, REFUND_SCHEMA
from .spark_utils import with_source_metadata, merge_idempotent


def _json_source(spark, path, schema):
    return (spark.read.format("json").schema(schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("recursiveFileLookup", "true").load(path))


def ingest_batch(spark, config):
    """Ingest immutable historical files; repeated runs merge on source-file/hash."""
    source_root = config.source_path("backfill", "gizmobox")
    sources = {
        "customers_raw": _json_source(spark, f"{source_root}/customers", CUSTOMER_SCHEMA).withColumn("_rescued_data", F.lit(None).cast("string")),
        "orders_raw": _json_source(spark, f"{source_root}/orders", ORDER_SCHEMA),
        "payments_raw": spark.read.format("csv").schema(PAYMENT_SCHEMA).option("header", "false").option("mode", "PERMISSIVE").option("recursiveFileLookup", "true").load(f"{source_root}/payments"),
        "addresses_raw": spark.read.format("csv").schema(ADDRESS_SCHEMA).option("header", "true").option("sep", "\t").option("mode", "PERMISSIVE").option("recursiveFileLookup", "true").load(f"{source_root}/addresses"),
        "refunds_raw": spark.read.format("csv").schema(REFUND_SCHEMA).option("header", "true").option("mode", "PERMISSIVE").option("recursiveFileLookup", "true").load(f"{source_root}/refunds"),
    }
    counts = {}
    for table_name, source_df in sources.items():
        enriched = with_source_metadata(source_df)
        merge_idempotent(spark, enriched, config.table("bronze", table_name))
        counts[table_name] = enriched.count()

    # Membership files are binary assets; the filename is the only verified customer key.
    membership_path = f"{source_root}/memberships"
    membership_df = (spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(membership_path)
        .select(F.col("path").alias("_source_file"), F.col("modificationTime").alias("file_modified_at"),
                F.col("length").cast("long").alias("file_size_bytes"),
                F.regexp_extract("path", r"/([0-9]+)\.png$", 1).cast("long").alias("customer_id"),
                F.regexp_extract("path", r"/(\d{4}-\d{2})/[0-9]+\.png$", 1).alias("membership_month"))
        .withColumn("_ingested_at", F.current_timestamp()))
    membership_df = membership_df.withColumn("_record_hash", F.sha2(F.concat_ws("|", "_source_file", "file_size_bytes", "customer_id"), 256))
    merge_idempotent(spark, membership_df, config.table("bronze", "membership_files"))
    counts["membership_files"] = membership_df.count()
    return counts


def ingest_customers_autoloader(spark, config, checkpoint_root):
    """Discover newly arriving customer JSON files without rescanning history."""
    incoming_path = config.source_path("incoming", "customers")
    schema_location = f"{checkpoint_root.rstrip('/')}/schema/customers"
    checkpoint_location = f"{checkpoint_root.rstrip('/')}/checkpoints/customers"
    source_df = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("pathGlobFilter", "*.json")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(CUSTOMER_SCHEMA)
        .load(incoming_path))
    if "_rescued_data" not in source_df.columns:
        source_df = source_df.withColumn("_rescued_data", F.lit(None).cast("string"))
    enriched = (source_df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in source_df.columns])), 256)))
    target = config.table("bronze", "customers_raw")

    def merge_batch(batch_df, batch_id):
        if batch_df.limit(1).count() > 0:
            merge_idempotent(spark, batch_df, target)

    query = (enriched.writeStream.foreachBatch(merge_batch)
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True).start())
    query.awaitTermination()
    return query.lastProgress
