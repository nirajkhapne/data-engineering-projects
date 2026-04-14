-- =============================================================================
-- FILE: 01_storage_format_benchmark.sql
-- PURPOSE: Empirically compare TEXTFILE vs ORC (SNAPPY) vs PARQUET for:
--          (a) physical storage size, (b) query execution time.
--
-- METHODOLOGY:
--   - Same dataset (telecom, ~7K rows) replicated to ~900K rows via
--     repeated self-INSERT to simulate realistic analytical workload size.
--   - Identical analytical query (AVG + WHERE filter) run against all three
--     formats to produce a fair execution time comparison.
--   - DESCRIBE FORMATTED used to extract physical storage sizes from Hive
--     metastore stats (numFiles, totalSize, rawDataSize).
--
-- WHY THIS MATTERS:
--   Storage format is one of the highest-leverage decisions in a Hive pipeline.
--   It affects: scan I/O, compression ratio, mapper count, and query latency.
--   Getting this wrong at table creation time is expensive to fix later.
-- =============================================================================

USE hive_db;

SET hive.exec.reducers.max = 4;


-- =============================================================================
-- STEP 1: Create base TEXTFILE table (uncompressed baseline)
-- =============================================================================
CREATE TABLE IF NOT EXISTS telecom_text (
    customerID      STRING,
    gender          STRING,
    SeniorCitizen   INT,
    Partner         STRING,
    Dependents      STRING,
    tenure          INT,
    PhoneService    STRING,
    MultipleLines   STRING,
    InternetService STRING,
    OnlineSecurity  STRING,
    OnlineBackup    STRING,
    DeviceProtection STRING,
    TechSupport     STRING,
    StreamingTV     STRING,
    StreamingMovies STRING,
    Contract        STRING,
    PaperlessBilling STRING,
    PaymentMethod   STRING,
    MonthlyCharges  FLOAT,
    TotalCharges    FLOAT,
    Churn           STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE telecom_text
SELECT * FROM telecom_staging;


-- =============================================================================
-- STEP 2: Inflate dataset to ~900K rows for meaningful benchmarking
-- Self-join INSERT doubles the row count on each iteration.
-- 7,044 → 14,088 → 28,176 → ... → 901,632 (after 7 doublings)
-- This is a controlled way to scale a small dataset for format benchmarking.
-- =============================================================================

-- Run these 7 times (or until COUNT(*) ≈ 900K):
-- INSERT INTO telecom_text SELECT * FROM telecom_text;

-- Verify inflation:
SELECT COUNT(*) AS inflated_row_count FROM telecom_text;
-- Expected: 901,632


-- =============================================================================
-- STEP 3: Create ORC (SNAPPY) and Parquet tables from the inflated dataset
-- =============================================================================
DROP TABLE IF EXISTS telecom_orc;
CREATE TABLE telecom_orc
STORED AS ORC
TBLPROPERTIES ("orc.compress" = "SNAPPY")
AS SELECT * FROM telecom_text;

DROP TABLE IF EXISTS telecom_parquet;
CREATE TABLE telecom_parquet
STORED AS PARQUET
AS SELECT * FROM telecom_text;


-- =============================================================================
-- STEP 4: Inspect physical storage via metastore stats
-- Look at: totalSize (actual bytes on disk), numFiles (parallelism indicator)
-- =============================================================================
DESCRIBE FORMATTED telecom_text;
DESCRIBE FORMATTED telecom_orc;
DESCRIBE FORMATTED telecom_parquet;

-- RESULTS OBSERVED (901,632 rows):
-- ┌──────────────────┬────────────┬──────────┬───────────────────────────┐
-- │ Format           │ Total Size │ # Files  │ Notes                     │
-- ├──────────────────┼────────────┼──────────┼───────────────────────────┤
-- │ TEXTFILE         │ 124.40 MB  │ 9        │ Uncompressed row-store    │
-- │ ORC + SNAPPY     │   4.70 MB  │ 4        │ 96.2% compression vs TEXT │
-- │ PARQUET          │   9.43 MB  │ 4        │ 92.4% compression vs TEXT │
-- └──────────────────┴────────────┴──────────┴───────────────────────────┘
-- ORC is ~2x smaller than Parquet here — primarily because ORC's built-in
-- lightweight indexes (min/max per stripe) and type-specific encoders
-- (RLE for INT/FLOAT) are more efficient than Parquet on this schema mix.


-- =============================================================================
-- STEP 5: Execution time benchmark — same analytical query, 3 formats
-- Query: Average MonthlyCharges for churned customers
-- Chosen because it requires: full scan + column projection + predicate filter
-- =============================================================================

-- TEXTFILE (baseline): ~25.5 seconds | 4 mappers
SELECT AVG(MonthlyCharges) FROM telecom_text    WHERE Churn = 'Yes';

-- ORC + SNAPPY:        ~18.9 seconds | 2 mappers (predicate pushdown)
SELECT AVG(MonthlyCharges) FROM telecom_orc     WHERE Churn = 'Yes';

-- PARQUET:             ~11.7 seconds | 1 mapper  (columnar + predicate pushdown)
SELECT AVG(MonthlyCharges) FROM telecom_parquet WHERE Churn = 'Yes';

-- All three return: 74.44 (result must be identical — data integrity check)

-- RESULTS SUMMARY:
-- ┌──────────────────┬──────────────────┬───────────┬──────────────────────────────────────────┐
-- │ Format           │ Query Time       │ # Mappers │ Why                                      │
-- ├──────────────────┼──────────────────┼───────────┼──────────────────────────────────────────┤
-- │ TEXTFILE         │ 25.5 s (baseline)│ 4         │ Reads all bytes, all columns, no pruning │
-- │ ORC + SNAPPY     │ 18.9 s (−26%)    │ 2         │ Stripe-level min/max pruning on Churn    │
-- │ PARQUET          │ 11.7 s (−54%)    │ 1         │ Column projection + row group filtering  │
-- └──────────────────┴──────────────────┴───────────┴──────────────────────────────────────────┘
--
-- KEY INSIGHT: Parquet wins on this query because it only reads 2 columns
-- (MonthlyCharges, Churn) out of 21 — columnar skip is maximum here.
-- ORC wins on storage size. The right format depends on your workload:
--   * Write-heavy / mixed workload → ORC
--   * Read-heavy / columnar analytics → Parquet
--   * Production Hive on YARN → ORC (tighter Hive integration, ACID support)
--   * Spark / Presto / Athena → Parquet (wider ecosystem support)
