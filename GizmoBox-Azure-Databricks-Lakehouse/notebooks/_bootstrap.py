# Databricks notebook source
# Shared notebook bootstrap. Keep workspace parameters out of source control.
import os
import re
import sys

# Resolve the deployed bundle root from this notebook's workspace path.
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_notebook_path = "/Workspace" + notebook_path if not notebook_path.startswith("/Workspace") else notebook_path
bundle_root = workspace_notebook_path.rsplit("/notebooks/", 1)[0]
sys.path.insert(0, f"{bundle_root}/src")

from gizmobox.config import PipelineConfig


def _widget(name, default, label=None):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default, label or name.replace("_", " ").title())
        return dbutils.widgets.get(name)

catalog = _widget("catalog", "gizmobox")
landing_root = _widget("landing_root", "")
checkpoint_root = _widget("checkpoint_root", "")

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("catalog must be a simple Unity Catalog identifier")
if not landing_root.startswith("abfss://"):
    raise ValueError("landing_root must be an ADLS Gen2 abfss:// path")
if not checkpoint_root.startswith("abfss://"):
    raise ValueError("checkpoint_root must be an ADLS Gen2 abfss:// path")

config = PipelineConfig(catalog=catalog, landing_root=landing_root, checkpoint_root=checkpoint_root)
for schema_name in (config.bronze_schema, config.silver_schema, config.gold_schema, config.ops_schema):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema_name}`")

print(f"GizmoBox pipeline configured for catalog={catalog}")
