# Telecom Customer Churn — End-to-End Hive Pipeline

A production-patterned data engineering pipeline on Apache Hive (Tez/YARN) demonstrating ingestion, transformation, dimensional modeling, storage format optimization, and automated data quality — using the IBM Telco Customer Churn dataset (7,044 customers).

---

## Table of Contents
- [Architecture](#architecture)
- [Pipeline Stages](#pipeline-stages)
- [Storage Format Benchmark](#storage-format-benchmark)
- [Dimensional Model](#dimensional-model)
- [Data Quality Framework](#data-quality-framework)
- [Key Hive Gotchas Documented](#key-hive-gotchas-documented)
- [How to Run](#how-to-run)
- [Stack](#stack)

---

## Architecture

```
HDFS /data/raw/telecom/          HDFS /data/raw/telecom_delta/
        │                                    │
        ▼                                    ▼
 telecom_raw (EXTERNAL)          telecom_delta_raw (EXTERNAL)
 TEXTFILE, CSV                   TEXTFILE, CSV + load_date
        │                                    │
        ▼                                    ▼
 telecom_staging (MANAGED)       telecom_delta_staging (ORC)
 ORC + SNAPPY, typed cast              │
        │                              │
        └──────────┬───────────────────┘
                   ▼
          telecom_merged_temp
          ROW_NUMBER() dedup
                   │
                   ▼
          telecom_latest (rn=1)
                   │
                   ▼
     ┌─────────────────────────────┐
     │    telecom_curated          │
     │    ORC, PARTITIONED BY      │
     │    Contract, BUCKETED BY    │
     │    customerID (4 buckets)   │
     └─────────────────────────────┘
                   │
        ┌──────────┼──────────┐
        ▼          ▼          ▼
  dim_contract  dim_payment  dim_service_type
        │          │          │
        └──────────┼──────────┘
                   ▼
        fact_customer_activity
        (Star Schema Fact Table)
                   │
                   ▼
          Churn Analysis Queries
```

---

## Pipeline Stages

### Stage 1 — Raw Ingestion (`02_ingestion/`)
- External table on `/data/raw/telecom/` — Hive does not own the files.
- `TotalCharges` intentionally ingested as `STRING`: upstream data contains empty strings (`''`) for new customers with zero tenure. Silent `CAST` would produce `NULL` inconsistently across Hive versions; explicit `CASE` logic in staging handles this deterministically.
- Pre-promotion DQ checks: duplicate `customerID` scan, null/empty `TotalCharges` count, row count baseline.

### Stage 2 — Staging (`03_transformation/01`)
- Managed ORC + SNAPPY table.
- `INSERT OVERWRITE` pattern ensures idempotency — re-running the job never duplicates data.
- `TotalCharges` type-promoted from `STRING` → `FLOAT` with explicit `CASE WHEN TotalCharges = '' THEN NULL` guard.

**Why ORC + SNAPPY at staging?**
- Columnar storage eliminates full-row scans for downstream transformations that touch only a subset of columns.
- SNAPPY codec: 3–5x faster decompression than ZLIB/GZIP at ~20% larger file size — correct tradeoff for a frequently-read staging table.
- Observed: staging reads return in <1 second (`COUNT(*) = 7044`); TEXTFILE equivalent took 10+ seconds on same Tez cluster.

### Stage 3 — Curated Layer (`03_transformation/02`)
- Partitioned by `Contract` (3 distinct values — Month-to-month, One year, Two year).
- Bucketed by `customerID` into 4 buckets.

**Partition key selection rationale:**

| Column Considered | Cardinality | Decision | Reason |
|---|---|---|---|
| `Contract` | 3 | ✅ Partition key | Low cardinality, dominant query filter |
| `Churn` | 2 | ❌ | It's a measure, not an access dimension |
| `customerID` | 7,044 | ❌ | High cardinality → small-file explosion |
| `PaymentMethod` | 4 | ❌ | Secondary filter; not used for pruning |

**Bucketing:** `customerID` INTO 4 BUCKETS enables bucket map join when this table is joined with another customerID-bucketed table, eliminating the sort-merge shuffle phase.

### Stage 4 — Incremental Delta Load (`03_transformation/03`)
Implements a `UNION ALL + ROW_NUMBER()` merge pattern — Hive's practical equivalent of `MERGE INTO` for non-ACID tables.

**Why not use ACID MERGE?**

| Approach | Pros | Cons |
|---|---|---|
| ACID MERGE (Hive 3+) | True upsert, efficient updates | Requires transactional ORC + bucketing + concurrency config; operational overhead |
| ROW_NUMBER() dedup | Works on any Hive version, debuggable intermediate table | Rewrites full table on every run |

For a ~7K–1M row dataset on a batch pipeline, the ROW_NUMBER() pattern is the pragmatic choice. ACID MERGE becomes worthwhile above ~50M rows where full rewrites are cost-prohibitive.

---

## Storage Format Benchmark

Dataset inflated to **901,632 rows** via repeated self-INSERT to simulate a realistic analytical workload. Same query run against three formats: `SELECT AVG(MonthlyCharges) FROM <table> WHERE Churn = 'Yes'`.

### Storage Size (901,632 rows, 21 columns)

| Format | Total Size | Files | vs TEXTFILE |
|---|---|---|---|
| TEXTFILE (CSV) | 124.40 MB | 9 | baseline |
| ORC + SNAPPY | **4.70 MB** | 4 | **−96.2%** |
| PARQUET | 9.43 MB | 4 | −92.4% |

ORC is ~2x smaller than Parquet on this dataset because ORC's type-specific encoders (RLE for integers, dictionary encoding for low-cardinality strings) and stripe-level lightweight indexes are more efficient for this schema's mix of INT, FLOAT, and low-cardinality STRING columns.

### Query Execution Time

| Format | Execution Time | Mappers | Δ vs TEXTFILE |
|---|---|---|---|
| TEXTFILE | 25.5 s | 4 | baseline |
| ORC + SNAPPY | 18.9 s | 2 | −26% |
| PARQUET | **11.7 s** | **1** | **−54%** |

**Why Parquet is faster on this specific query:**
The query touches only 2 of 21 columns (`MonthlyCharges`, `Churn`). Parquet's columnar layout means Hive reads ~9.5% of the data. ORC also skips columns, but Parquet's row group–level predicate pushdown on `Churn` further reduces the scan to a single mapper.

### Format Selection Guide

| Use Case | Recommended Format |
|---|---|
| Production Hive on YARN, mixed read/write | ORC + SNAPPY |
| Spark / Presto / AWS Athena analytics | Parquet |
| External system integration, human-readable | TEXTFILE (raw layer only) |
| ACID transactions (updates/deletes) | ORC (transactional) |

---

## Dimensional Model

Star schema on top of the staging layer.

```
                    dim_contract (3 rows)
                    ┌──────────────────┐
                    │ contract_key PK  │
                    │ contract_name    │
                    └────────┬─────────┘
                             │
dim_service_type             │        dim_payment (4 rows)
┌──────────────────┐         │        ┌──────────────────┐
│ service_key PK   │         │        │ payment_key PK   │
│ internet_service │         │        │ payment_method   │
└────────┬─────────┘         │        └─────────┬────────┘
         │                   │                  │
         └─────────┬─────────┘──────────────────┘
                   ▼
       fact_customer_activity
       ┌────────────────────────┐
       │ customerID             │
       │ contract_key  FK       │
       │ payment_key   FK       │
       │ service_key   FK       │
       │ tenure                 │
       │ MonthlyCharges         │
       │ TotalCharges           │
       │ Churn                  │
       └────────────────────────┘
```

**Surrogate keys** generated via `ROW_NUMBER() OVER (ORDER BY <natural_key>)` — deterministic for static dimension tables. For slowly-changing dimensions, this would need a dedicated key sequence table.

**Known model limitation:** No date dimension. The current fact table is a point-in-time snapshot. To support time-series analysis (churn rate by cohort month), the grain would need to change to `(customerID, month)` with a `dim_date` table.

---

## Data Quality Framework

Append-only `data_quality_metrics` table tracks per-run statistics for each table in the pipeline.

**Metrics captured per run:**
- `row_count` — total rows (reconciliation anchor)
- `null_totalcharges` — null count in the promoted FLOAT column
- `duplicate_customerids` — `COUNT(*) - COUNT(DISTINCT customerID)` (0 = clean)
- `created_ts` — timestamp of DQ run

**Anomaly detection:** A self-join on `data_quality_metrics` flags any table where today's row count dropped >10% vs the previous run. Zero result set = healthy pipeline.

**Hive-specific DQ constraints documented:**
- Scalar subqueries in `SELECT` list are unsupported → use single-pass `CASE WHEN` aggregation
- CTE (`WITH ... AS`) prefixed before `INSERT INTO` throws `ParseException` in Hive CLI → restructure as inline subquery or separate CTEs
- Reserved word `current` as table alias causes `NoViableAltException` → use `c`/`p` instead

---

## Key Hive Gotchas Documented

These are real errors encountered during this project, documented for reproducibility:

| Error | Root Cause | Fix |
|---|---|---|
| `CalciteSubquerySemanticException [Error 10249]` | Scalar subquery in SELECT list | Replace with CASE WHEN aggregation in single pass |
| `ParseException: cannot recognize input near 'WITH'` | CTE before INSERT not supported in Hive CLI | Use subquery inline or separate CREATE TABLE AS |
| `NoViableAltException` in SELECT clause | Reserved word `current` used as alias | Rename alias to `c` or `curr` |
| `Cannot insert: column count mismatch` | Table had 5 columns, query returned 6 | Run `DESCRIBE` before INSERT; used `ALTER TABLE ADD COLUMNS` |
| Dynamic partition INSERT column order | Partition column must be LAST in SELECT | Reorder SELECT list; Hive maps by position not name |

---

## How to Run

```bash
# 1. Start Hive CLI
hive

# 2. Run in sequence
SOURCE sql/01_setup/01_create_database.sql;
SOURCE sql/02_ingestion/01_create_raw_external_table.sql;
SOURCE sql/03_transformation/01_create_staging_table.sql;
SOURCE sql/03_transformation/02_create_curated_partitioned_table.sql;
SOURCE sql/04_dimensional_model/01_dimensional_model.sql;
SOURCE sql/05_analytics/01_churn_analysis.sql;
SOURCE sql/06_format_benchmarks/01_storage_format_benchmark.sql;
SOURCE sql/07_data_quality/01_data_quality_framework.sql;
```

For the incremental load, place delta CSV at `/data/raw/telecom_delta/` before running:
```bash
SOURCE sql/03_transformation/03_incremental_delta_load.sql;
```

---

## Stack

| Component | Version |
|---|---|
| Apache Hive | 3.1.3 |
| Execution Engine | Apache Tez |
| Cluster Manager | YARN (Google Dataproc) |
| Storage | HDFS |
| ORC Compression | SNAPPY |
| Dataset | IBM Telco Churn (~7K rows, expanded to ~900K for benchmarks) |

---

## Project Structure

```
.
├── sql/
│   ├── 01_setup/
│   │   └── 01_create_database.sql
│   ├── 02_ingestion/
│   │   └── 01_create_raw_external_table.sql
│   ├── 03_transformation/
│   │   ├── 01_create_staging_table.sql
│   │   ├── 02_create_curated_partitioned_table.sql
│   │   └── 03_incremental_delta_load.sql
│   ├── 04_dimensional_model/
│   │   └── 01_dimensional_model.sql
│   ├── 05_analytics/
│   │   └── 01_churn_analysis.sql
│   ├── 06_format_benchmarks/
│   │   └── 01_storage_format_benchmark.sql
│   └── 07_data_quality/
│       └── 01_data_quality_framework.sql
└── README.md
```
