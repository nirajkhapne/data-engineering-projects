# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
from gizmobox.gold import build_gold
from gizmobox.spark_utils import write_audit

counts = build_gold(spark, config)
for entity, count in counts.items():
    write_audit(spark, config.table("ops", "pipeline_audit"), "gold", entity, "SUCCESS", count, count, 0)
print(f"Gold models completed: {counts}")
