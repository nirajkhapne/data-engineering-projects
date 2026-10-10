# Databricks notebook source
# MAGIC %run ./_bootstrap

# COMMAND ----------
from gizmobox.quality import run_quality_checks

checks, failed = run_quality_checks(spark, config)
for check in checks:
    print(f"{check['status']}: {check['check_name']} | {check['observed_value']} | {check['details']}")
if failed:
    raise RuntimeError(f"{len(failed)} data-quality check(s) failed. Review {config.table('ops', 'quality_results')}.")
print(f"All {len(checks)} data-quality checks passed.")
