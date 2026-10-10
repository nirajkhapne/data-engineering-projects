# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
from gizmobox.bronze import ingest_batch
from gizmobox.spark_utils import write_audit

counts = ingest_batch(spark, config)
for entity, count in counts.items():
    write_audit(spark, config.table("ops", "pipeline_audit"), "bronze_batch", entity, "SUCCESS", count, count, 0)
print(f"Historical batch ingestion completed: {counts}")
