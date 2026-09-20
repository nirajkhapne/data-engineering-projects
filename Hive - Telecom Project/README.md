# Telecom Customer Churn Data Warehouse

An end-to-end **Apache Hive data engineering project** demonstrating a production-patterned batch pipeline on **HDFS + Tez/YARN**. The project covers raw ingestion, typed transformation, incremental snapshot merging, partitioning, bucketing, dimensional modeling, storage-format benchmarking, and repeatable data-quality auditing using the IBM Telco Customer Churn dataset.

> **Environment:** Hive 3.x, HDFS, Tez, YARN. The repository contains SQL and validation code; source CSVs are expected to be mounted into the documented HDFS paths.

## Architecture

```text
HDFS raw CSV
   │
   ▼
telecom_raw (EXTERNAL / TEXTFILE)
   │
   ▼
telecom_staging (typed ORC + SNAPPY)
   │
   ├────────────── initial snapshot ──────────────┐
   │                                              │
   ▼                                              ▼
telecom_curated                         telecom_delta_raw
(partitioned by Contract,               (EXTERNAL delta feed)
 bucketed by customerID)                          │
                                                 ▼
                                      telecom_delta_staging (ORC)
                                                 │
                         base + delta UNION ALL + ROW_NUMBER()
                                                 │
                                                 ▼
                                      telecom_latest (latest row/customer)
                                                 │
                                                 ▼
                                      telecom_curated refresh
                                                 │
                    ┌────────────────────────────┼─────────────────────────┐
                    ▼                            ▼                         ▼
             dim_contract                dim_payment              dim_service_type
                    \ \__________________________│________________________/
                                               ▼
                                  fact_customer_activity
                                      (star-schema fact)
                                               │
                              ┌────────────────┴───────────────┐
                              ▼                                ▼
                       Churn analytics                    DQ audit metrics
```

## Engineering Patterns Demonstrated

### 1. Layered ingestion and transformation
- Raw CSV is exposed through an **EXTERNAL TEXTFILE table**, keeping source ownership in HDFS.
- `TotalCharges` remains `STRING` at the raw boundary and is explicitly promoted to `FLOAT` in staging.
- Staging uses **ORC + SNAPPY** and `INSERT OVERWRITE` for deterministic reruns.

### 2. Incremental snapshot processing
- Delta records arrive through a separate external table with a source-provided `load_date`.
- Base + delta are combined with `UNION ALL`.
- `ROW_NUMBER() OVER (PARTITION BY customerID ORDER BY load_date DESC, source_priority DESC)` keeps the latest customer record.
- Same-day conflicts explicitly prefer the delta source.
- Intermediate merge tables are rebuilt on every run, avoiding stale `CREATE TABLE IF NOT EXISTS ... AS SELECT` state.

This is a practical **non-ACID snapshot merge** pattern. For very large transactional workloads, Hive ACID `MERGE` or partition-level incremental strategies would be evaluated instead.

### 3. Partitioning and bucketing
- `telecom_curated` is partitioned by low-cardinality `Contract`.
- `customerID` is used as the bucketing key to support customer-level joins.
- The project explicitly enables dynamic partitioning and bucket-related settings where required.

### 4. Dimensional modeling
The curated snapshot feeds a star schema:
- `dim_contract`
- `dim_payment`
- `dim_service_type`
- `fact_customer_activity` — grain: one row per customer in the latest snapshot

Dimension tables are rebuilt idempotently so repeated project runs do not append duplicate surrogate-key rows. A partitioned/bucketed fact variant is also included for scale-oriented design.

### 5. Data quality and reconciliation
The DQ framework records per-run:
- row count
- null customer IDs
- null `TotalCharges`
- duplicate customer IDs
- invalid tenure values
- invalid contract values
- execution timestamp

A parameterized run-over-run query flags **>10% row-count regression** without relying on hardcoded calendar dates.

### 6. Storage-format benchmarking
The benchmark creates isolated TEXTFILE, ORC+SNAPPY and PARQUET copies of the same workload, scales the 7,044-row source to **901,632 rows**, collects Hive statistics, compares physical metadata and runs the same analytical query against all formats.

The original benchmark run observed:

| Format | Size | Query time |
|---|---:|---:|
| TEXTFILE | 124.40 MB | 25.5 s |
| ORC + SNAPPY | 4.70 MB | 18.9 s |
| PARQUET | 9.43 MB | 11.7 s |

These timings are **environment-specific observations**, not universal guarantees. The benchmark script should be rerun on the target cluster when comparing formats.

## Repository Structure

```text
Hive - Telecom Project/
├── README.md
├── .gitignore
├── scripts/
│   └── validate_project.py
└── sql/
    ├── 01_setup/
    │   ├── 01_create_database.sql
    │   └── README.md
    ├── 02_ingestion/
    │   └── 01_create_raw_external_table.sql
    ├── 03_transformation/
    │   ├── 01_create_staging_table.sql
    │   ├── 02_create_curated_partitioned_table.sql
    │   └── 03_incremental_delta_load.sql
    ├── 04_dimensional_model/
    │   └── 01_dimensional_model.sql
    ├── 05_analytics/
    │   └── 01_churn_analysis.sql
    ├── 06_format_benchmarks/
    │   └── 01_storage_format_benchmark.sql
    └── 07_data_quality/
        └── 01_data_quality_framework.sql
```

## HDFS Input Contracts

Initial snapshot:
```text
/data/raw/telecom/
```

Incremental feed:
```text
/data/raw/telecom_delta/
```

The delta CSV must contain the same customer attributes as the base feed plus a source-provided `load_date` column in an ISO-sortable format such as `YYYY-MM-DD`.

## Run Order

```sql
SOURCE sql/01_setup/01_create_database.sql;
SOURCE sql/02_ingestion/01_create_raw_external_table.sql;
SOURCE sql/03_transformation/01_create_staging_table.sql;
SOURCE sql/03_transformation/02_create_curated_partitioned_table.sql;
SOURCE sql/04_dimensional_model/01_dimensional_model.sql;
SOURCE sql/05_analytics/01_churn_analysis.sql;
SOURCE sql/07_data_quality/01_data_quality_framework.sql;
```

For an incremental refresh, after the initial load and after placing the delta feed in HDFS:

```sql
SOURCE sql/03_transformation/03_incremental_delta_load.sql;
SOURCE sql/04_dimensional_model/01_dimensional_model.sql;
SOURCE sql/05_analytics/01_churn_analysis.sql;
SOURCE sql/07_data_quality/01_data_quality_framework.sql;
```

The incremental script defaults `hivevar:initial_load_date` to `2026-03-01`; change it to the actual initial snapshot date before running the merge.

For the isolated storage benchmark:

```sql
SOURCE sql/06_format_benchmarks/01_storage_format_benchmark.sql;
```

## Validation

Run the static validator from the project root:

```bash
python scripts/validate_project.py
```

The validator checks required files, rerun safety of incremental intermediates, idempotent dimension refreshes, and that analytics consume the dimensional model.

## Important Scope Note

This project is designed as a **production-patterned GitHub portfolio project**, not a claim of production deployment. Actual production readiness would additionally require an orchestrator, secrets/configuration management, automated alert delivery, cluster-level resource tuning, integration tests on the target Hive distribution, and operational SLAs.

## Stack

**Apache Hive 3.x | HDFS | Apache Tez | YARN | SQL | ORC | Parquet | Star Schema | Batch ETL | Data Quality**
