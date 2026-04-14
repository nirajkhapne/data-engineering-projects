-- =============================================================================
-- FILE: 01_create_raw_external_table.sql
-- PURPOSE: Land raw CSV data as an EXTERNAL table pointing to HDFS location.
--          External = Hive doesn't own the data; DROP TABLE won't delete files.
--          This is the correct pattern for raw ingestion layers.
-- ENGINE: Apache Hive 3.1.3 on Tez / YARN
-- =============================================================================

USE hive_db;

-- -----------------------------------------------------------------------------
-- RAW TABLE (External)
-- NOTE: TotalCharges ingested as STRING intentionally — raw data contains
--       empty strings that cannot be cast at ingestion time. Type correction
--       is deferred to the staging layer via explicit CASE logic.
-- -----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS telecom_raw (
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
    TotalCharges    STRING,   -- Kept as STRING: upstream has '' (empty string) for new customers
    Churn           STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/telecom/';


-- =============================================================================
-- DATA QUALITY CHECKS — Run before promoting to staging
-- =============================================================================

-- 1. Duplicate customerIDs
--    Expected: 0 rows returned (customerID must be a natural key)
SELECT customerID, COUNT(*) AS occurrence_count
FROM telecom_raw
GROUP BY customerID
HAVING COUNT(*) > 1;

-- 2. Null / empty TotalCharges
--    Acceptable if it corresponds to new customers (tenure = 0)
--    Non-zero tenure + NULL TotalCharges = data pipeline issue upstream
SELECT COUNT(*) AS null_or_empty_totalcharges
FROM telecom_raw
WHERE TotalCharges IS NULL OR TotalCharges = '';

-- 3. Row count baseline — anchor for downstream reconciliation
SELECT COUNT(*) AS raw_row_count
FROM telecom_raw;
-- Expected: 7044
