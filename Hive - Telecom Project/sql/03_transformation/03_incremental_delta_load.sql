-- =============================================================================
-- FILE: 03_incremental_delta_load.sql
-- PURPOSE: Simulate a daily incremental (delta) load using a UNION ALL +
--          ROW_NUMBER() deduplication pattern — Hive's equivalent of MERGE
--          (ACID MERGE is available in Hive 3+ with transactional tables, but
--          requires ORC + bucketing + hive.support.concurrency=true, which
--          adds overhead not justified for a batch pipeline of this scale).
--
-- PATTERN: "Snapshot Merge via ROW_NUMBER"
--   1. Create external raw table pointing to delta HDFS location (new/updated rows)
--   2. Cast and promote delta to ORC staging
--   3. UNION ALL base + delta, assign ROW_NUMBER partitioned by customerID
--      ordered by load_date DESC → latest record per customer gets rn=1
--   4. Filter rn=1 into telecom_latest
--   5. Overwrite telecom_curated from telecom_latest
--
-- TRADE-OFF vs ACID MERGE:
--   + No transactional table config required
--   + Works on Hive 2.x and 3.x
--   + Easier to debug (intermediate table is inspectable)
--   - Rewrites the entire table on every run (full overwrite)
--   - For very large tables (100M+ rows), prefer ACID MERGE or a
--     partition-level incremental overwrite strategy
-- =============================================================================

USE hive_db;

SET hive.exec.dynamic.partition       = true;
SET hive.exec.dynamic.partition.mode  = nonstrict;
SET hive.auto.convert.join            = true;
SET hive.optimize.bucketmapjoin       = true;


-- -----------------------------------------------------------------------------
-- STEP 1: External raw table for delta feed
-- load_date is ingested from the source file — do NOT derive it in Hive.
-- Deriving load_date as CURRENT_DATE() in Hive would cause reprocessing
-- issues if the job is re-run on a different date.
-- -----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS telecom_delta_raw (
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
    TotalCharges    STRING,
    Churn           STRING,
    load_date       STRING     -- Source-system provided date; drives deduplication order
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/telecom_delta/';


-- -----------------------------------------------------------------------------
-- STEP 2: Cast and compress delta into ORC staging
-- CTAS is used here instead of CREATE + INSERT for brevity on a one-time
-- staging table. For production, prefer explicit CREATE TABLE to lock schema.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS telecom_delta_staging
STORED AS ORC
AS
SELECT
    customerID,
    gender,
    SeniorCitizen,
    Partner,
    Dependents,
    tenure,
    PhoneService,
    MultipleLines,
    InternetService,
    OnlineSecurity,
    OnlineBackup,
    DeviceProtection,
    TechSupport,
    StreamingTV,
    StreamingMovies,
    Contract,
    PaperlessBilling,
    PaymentMethod,
    MonthlyCharges,
    CASE
        WHEN TotalCharges = '' THEN NULL
        ELSE CAST(TotalCharges AS FLOAT)
    END AS TotalCharges,
    Churn,
    load_date
FROM telecom_delta_raw;


-- -----------------------------------------------------------------------------
-- STEP 3: Merge base + delta, deduplicate — keep latest record per customer
-- The hardcoded '2026-03-01' load_date for the base table marks when the
-- initial full load was ingested. In production, store this in a config table.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS telecom_merged_temp
STORED AS ORC
AS
SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY customerID
        ORDER BY load_date DESC
    ) AS rn
FROM (
    SELECT
        customerID, gender, SeniorCitizen, Partner, Dependents, tenure,
        PhoneService, MultipleLines, InternetService, OnlineSecurity,
        OnlineBackup, DeviceProtection, TechSupport, StreamingTV,
        StreamingMovies, Contract, PaperlessBilling, PaymentMethod,
        MonthlyCharges, TotalCharges, Churn,
        '2026-03-01' AS load_date   -- Initial full-load date stamp
    FROM telecom_staging

    UNION ALL

    SELECT
        customerID, gender, SeniorCitizen, Partner, Dependents, tenure,
        PhoneService, MultipleLines, InternetService, OnlineSecurity,
        OnlineBackup, DeviceProtection, TechSupport, StreamingTV,
        StreamingMovies, Contract, PaperlessBilling, PaymentMethod,
        MonthlyCharges, TotalCharges, Churn,
        load_date
    FROM telecom_delta_staging
) t;


-- -----------------------------------------------------------------------------
-- STEP 4: Extract latest snapshot (rn=1 per customer)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS telecom_latest
STORED AS ORC
AS
SELECT *
FROM telecom_merged_temp
WHERE rn = 1;


-- -----------------------------------------------------------------------------
-- STEP 5: Refresh curated layer with merged latest data
-- -----------------------------------------------------------------------------
INSERT OVERWRITE TABLE telecom_curated
PARTITION (Contract)
SELECT
    customerID, gender, SeniorCitizen, Partner, Dependents, tenure,
    PhoneService, MultipleLines, InternetService, OnlineSecurity,
    OnlineBackup, DeviceProtection, TechSupport, StreamingTV,
    StreamingMovies, PaperlessBilling, PaymentMethod,
    MonthlyCharges, TotalCharges, Churn,
    Contract
FROM telecom_latest;


-- =============================================================================
-- VERIFICATION
-- =============================================================================
SELECT DISTINCT Contract FROM telecom_delta_staging;
-- Validate delta only contains known contract types (no schema drift)
