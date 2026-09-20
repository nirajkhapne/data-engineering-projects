"""Static validation for the Hive telecom project.

This does not require Hive/HDFS/YARN. It checks project structure, SQL references,
required setup files, and a few unsafe patterns that previously caused rerun bugs.
"""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql"
REQUIRED = [
    SQL / "01_setup/01_create_database.sql",
    SQL / "02_ingestion/01_create_raw_external_table.sql",
    SQL / "03_transformation/01_create_staging_table.sql",
    SQL / "03_transformation/02_create_curated_partitioned_table.sql",
    SQL / "03_transformation/03_incremental_delta_load.sql",
    SQL / "04_dimensional_model/01_dimensional_model.sql",
    SQL / "05_analytics/01_churn_analysis.sql",
    SQL / "06_format_benchmarks/01_storage_format_benchmark.sql",
    SQL / "07_data_quality/01_data_quality_framework.sql",
]

missing = [str(p.relative_to(ROOT)) for p in REQUIRED if not p.exists()]
assert not missing, f"Missing required files: {missing}"

for path in SQL.rglob("*.sql"):
    text = path.read_text(errors="replace")
    assert "TODO" not in text, f"Unresolved TODO in {path}"

# Incremental script must rebuild intermediate CTAS tables on rerun.
delta = (SQL / "03_transformation/03_incremental_delta_load.sql").read_text()
for table in ("telecom_merged_temp", "telecom_latest"):
    assert f"DROP TABLE IF EXISTS {table}" in delta, f"Rerun safety missing for {table}"

# Dimension inserts must not append duplicate surrogate-key rows.
dim = (SQL / "04_dimensional_model/01_dimensional_model.sql").read_text()
assert "INSERT OVERWRITE TABLE dim_contract" in dim
assert "INSERT OVERWRITE TABLE dim_payment" in dim
assert "INSERT OVERWRITE TABLE dim_service_type" in dim

# Analytics should use the dimensional model, not staging as the reporting source.
analytics = (SQL / "05_analytics/01_churn_analysis.sql").read_text()
assert "FROM fact_customer_activity" in analytics

print("Hive telecom project static validation passed.")
