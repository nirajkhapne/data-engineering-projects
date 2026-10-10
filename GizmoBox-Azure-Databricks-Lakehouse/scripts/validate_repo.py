#!/usr/bin/env python3
"""Static checks and source-fixture audit; does not require Spark or Azure."""

import ast
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
errors = []
python_files = sorted([*ROOT.glob("src/**/*.py"), *ROOT.glob("notebooks/*.py"), *ROOT.glob("tests/*.py"), *ROOT.glob("scripts/*.py")])
for path in python_files:
    try:
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        errors.append(f"Python syntax: {path.relative_to(ROOT)}:{exc.lineno}: {exc.msg}")

required = [
    "README.md", "databricks.yml", "resources/gizmobox_pipeline.yml",
    "notebooks/_bootstrap.py", "notebooks/01_bronze_batch.py",
    "notebooks/02_bronze_autoloader_customers.py", "notebooks/03_silver_transform.py",
    "notebooks/04_gold_models.py", "notebooks/05_data_quality.py",
    "src/gizmobox/bronze.py", "src/gizmobox/silver.py", "src/gizmobox/gold.py",
    "src/gizmobox/quality.py", "sql/azure_sql_refunds.sql",
    "docs/architecture.md", "docs/data_dictionary.md", "docs/runbook.md",
    "tests/test_contracts.py", "tests/test_sample_data.py",
]
for relative in required:
    if not (ROOT / relative).is_file():
        errors.append(f"Missing required file: {relative}")

if (ROOT / ".env").exists():
    errors.append("A real .env file must not be committed.")

orders_dir = ROOT / "data/sample/gizmobox/orders"
order_lines = 0
bad_json_lines = 0
for path in sorted(orders_dir.glob("*.json")):
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        order_lines += 1
        try:
            json.loads(line)
        except json.JSONDecodeError:
            bad_json_lines += 1
if order_lines == 0:
    errors.append("No sample order records found.")
if bad_json_lines != 1:
    errors.append(f"Expected one intentionally malformed sample order line; found {bad_json_lines}.")

incoming = ROOT / "data/sample/incoming/customers/customer_delta_2024_12.json"
try:
    json.loads(incoming.read_text(encoding="utf-8"))
except Exception as exc:
    errors.append(f"Incoming Auto Loader sample is not valid JSON: {exc}")

try:
    import yaml
    for path in [ROOT / "databricks.yml", ROOT / "resources/gizmobox_pipeline.yml"]:
        yaml.safe_load(path.read_text(encoding="utf-8"))
except ImportError:
    print("NOTE: PyYAML not installed; YAML parse skipped.")
except Exception as exc:
    errors.append(f"YAML parse failed: {exc}")
else:
    resource = yaml.safe_load((ROOT / "resources/gizmobox_pipeline.yml").read_text(encoding="utf-8"))
    jobs = resource.get("resources", {}).get("jobs", {})
    if "gizmobox_lakehouse_pipeline" not in jobs:
        errors.append("Lakeflow job resource is missing.")
    else:
        job = jobs["gizmobox_lakehouse_pipeline"]
        for task in job.get("tasks", []):
            notebook_path = task.get("notebook_task", {}).get("notebook_path")
            if notebook_path and not (ROOT / "resources" / notebook_path).resolve().is_file():
                errors.append(f"Job task notebook does not exist: {notebook_path}")

for path in ROOT.rglob("*"):
    if path.is_file() and (path.name == ".DS_Store" or path.suffix == ".dbc" or path.suffix.lower() == ".pdf"):
        errors.append(f"Unexpected course/OS artifact in repository: {path.relative_to(ROOT)}")

if errors:
    print("Repository validation FAILED:")
    for error in errors:
        print(f" - {error}")
    sys.exit(1)

print(f"Repository validation passed: {len(python_files)} Python files parsed; required files present; sample JSON contracts checked.")
print(f"Sample orders: {order_lines} lines, {bad_json_lines} intentionally malformed line for quarantine testing.")
print("Note: this is static/local validation, not an Azure Databricks runtime test.")
