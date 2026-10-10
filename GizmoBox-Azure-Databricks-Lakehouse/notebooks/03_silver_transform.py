# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
from gizmobox.silver import transform_all
from gizmobox.spark_utils import write_audit

counts = transform_all(spark, config)
for entity, count in counts.items():
    write_audit(spark, config.table("ops", "pipeline_audit"), "silver", entity, "SUCCESS", count, count, 0)
print(f"Silver transformations completed: {counts}")
