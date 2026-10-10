# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
from gizmobox.bronze import ingest_customers_autoloader
from gizmobox.spark_utils import write_audit

progress = ingest_customers_autoloader(spark, config, config.checkpoint_root)
progress = progress or {}
input_count = int(progress.get("numInputRows", 0))
write_audit(spark, config.table("ops", "pipeline_audit"), "bronze_autoloader", "customers", "SUCCESS", input_count, input_count, 0, str(progress))
print(f"Auto Loader completed. Last progress: {progress}")
