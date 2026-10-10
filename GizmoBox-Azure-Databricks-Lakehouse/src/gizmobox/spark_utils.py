"""Small reusable Spark helpers for repeatable Delta writes."""

from pyspark.sql import functions as F

METADATA_COLUMNS = ["_source_file", "_ingested_at", "_record_hash"]


def with_source_metadata(df):
    """Attach source lineage and a deterministic hash of the record payload."""
    payload_columns = [F.col(c) for c in df.columns if c not in METADATA_COLUMNS]
    return (
        df.withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_hash", F.sha2(F.to_json(F.struct(*payload_columns)), 256))
    )


def ensure_delta_table(spark, table_name, df):
    """Create an empty Delta table from the incoming schema when absent."""
    if not spark.catalog.tableExists(table_name.replace("`", "")):
        (df.limit(0).write.format("delta").mode("overwrite").saveAsTable(table_name))


def merge_idempotent(spark, df, table_name):
    """Merge immutable source records by file path and payload hash."""
    from delta.tables import DeltaTable

    df = df.dropDuplicates(["_source_file", "_record_hash"])
    ensure_delta_table(spark, table_name, df)
    view_name = "incoming_" + table_name.replace("`", "").replace(".", "_")
    df.createOrReplaceTempView(view_name)
    target = DeltaTable.forName(spark, table_name)
    (target.alias("t").merge(
        spark.table(view_name).alias("s"),
        "t._source_file = s._source_file AND t._record_hash = s._record_hash",
    ).whenNotMatchedInsertAll().execute())
    spark.catalog.dropTempView(view_name)


def write_audit(spark, table_name, stage, entity, status, input_count, output_count, rejected_count, message=""):
    """Append one operational audit event; audit history is intentionally append-only."""
    from pyspark.sql import Row
    row = Row(
        stage=stage, entity=entity, status=status,
        input_count=int(input_count), output_count=int(output_count),
        rejected_count=int(rejected_count), message=str(message)[:2000],
    )
    (spark.createDataFrame([row])
        .withColumn("run_id", F.expr("uuid()"))
        .withColumn("recorded_at", F.current_timestamp())
        .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name))


def merge_on_keys(spark, df, table_name, keys):
    """Upsert mutable business entities using their declared natural key."""
    from delta.tables import DeltaTable
    ensure_delta_table(spark, table_name, df)
    view_name = "upsert_" + table_name.replace("`", "").replace(".", "_")
    df.createOrReplaceTempView(view_name)
    condition = " AND ".join(f"t.`{key}` <=> s.`{key}`" for key in keys)
    (DeltaTable.forName(spark, table_name).alias("t").merge(
        spark.table(view_name).alias("s"), condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    spark.catalog.dropTempView(view_name)
