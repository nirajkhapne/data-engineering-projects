# Operational runbook

## Before first run

- Confirm the target catalog exists and the run identity can create schemas/tables.
- Confirm ADLS Gen2 access via Unity Catalog and validate read/write access to landing and checkpoint paths.
- Upload historical fixtures and the incoming customer file to the paths documented in the README.
- Confirm checkpoint root is durable and not cleared by any cleanup/backfill process.
- Validate the bundle and check that the selected Azure VM type exists in the region.

## Normal run

Run `GizmoBox Lakehouse Pipeline` from Workflows. Inspect task status in dependency order:

1. `bronze_historical_batch`
2. `bronze_autoloader_customers`
3. `silver_transformations`
4. `gold_models`
5. `data_quality_gate`

Review `ops.pipeline_audit` and `ops.quality_results` after each run.

## Failure handling

- **ADLS permission denied:** verify storage credential/external location grants and the identity used by compute. Do not add account keys to code.
- **Auto Loader schema/checkpoint error:** preserve the checkpoint; inspect schema evolution and rescued data before changing it. Resetting a checkpoint may replay files.
- **Malformed JSON:** inspect the corresponding Silver rejected table and Bronze `_corrupt_record`. Fix the upstream file or document an explicit exception; don't silently filter it out.
- **Duplicate key / Delta merge failure:** inspect source duplicates and confirm the configured natural key matches the actual source grain.
- **Quality gate fails:** inspect `ops.quality_results`; blocking failures stop the job. Warnings need investigation and an explicit decision before production use.
- **Rerun after partial failure:** keep checkpoints and Bronze/Silver/Gold tables intact; rerun the job. Business tables are upserted on their keys, while audit tables append a new event.

## Backfill changes

Historical files are assumed immutable. If replacing a historical file, don't simply overwrite it and expect old records to disappear. Use an explicit file-version strategy or controlled table replacement process, then reconcile downstream tables.

## Evidence to capture for GitHub

- Bundle validation output (redact workspace IDs and storage account names).
- A successful Lakeflow Job run with task dependencies.
- Bronze counts and a malformed-record quarantine query.
- Evidence that a second run does not duplicate business rows.
- Evidence that a newly added customer JSON file reaches Gold.
- Quality results and monthly order summary.
- Cluster/runtime configuration with any tenant-specific details redacted.
