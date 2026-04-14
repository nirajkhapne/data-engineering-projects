-- =============================================================================
-- FILE: 02_create_curated_partitioned_table.sql
-- PURPOSE: Build the curated layer — partitioned + bucketed ORC table.
--          This is the production-read layer for analytics and BI tools.
--
-- PARTITION STRATEGY:
--   * Partitioned by Contract (3 distinct values: Month-to-month, One year,
--     Two year). Low-cardinality column = good partition key.
--   * High-cardinality columns (e.g., customerID) should NEVER be partition
--     keys — they would create thousands of micro-partitions, destroying
--     metastore performance and HDFS small-file count.
--
-- BUCKETING STRATEGY:
--   * Clustered by customerID INTO 4 BUCKETS.
--   * Enables bucket map join when joining with other bucketed tables on
--     customerID — eliminates the sort-merge shuffle entirely.
--   * 4 buckets = conservative choice for ~7K rows; scale to 16–32 for
--     tens of millions of rows.
--
-- WHY NOT PARTITION BY CHURN?
--   Churn has only 2 values (Yes/No). Although low-cardinality, it's a
--   *measure*, not a natural access dimension. Analysts rarely say
--   "give me all churned customers" as the primary filter; they join
--   contract type and service type first. Partitioning by Contract aligns
--   with the dominant query pattern observed in the analytics layer.
-- =============================================================================

USE hive_db;

SET hive.exec.dynamic.partition       = true;
SET hive.exec.dynamic.partition.mode  = nonstrict;
SET hive.auto.convert.join            = true;
SET hive.optimize.bucketmapjoin       = true;
SET hive.auto.convert.sortmerge.join  = true;


CREATE TABLE IF NOT EXISTS telecom_curated (
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
    PaperlessBilling STRING,
    PaymentMethod   STRING,
    MonthlyCharges  FLOAT,
    TotalCharges    FLOAT,
    Churn           STRING
)
PARTITIONED BY (Contract STRING)     -- Contract excluded from column list; appears as partition col
CLUSTERED BY (customerID) INTO 4 BUCKETS
STORED AS ORC;


-- NOTE: In dynamic partition INSERT, the partition column MUST be the LAST
--       column in the SELECT — Hive maps position, not name.
INSERT OVERWRITE TABLE telecom_curated
PARTITION (Contract)
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
    PaperlessBilling,
    PaymentMethod,
    MonthlyCharges,
    TotalCharges,
    Churn,
    Contract       -- Partition column LAST
FROM telecom_staging;


-- =============================================================================
-- PARTITION VERIFICATION
-- =============================================================================
-- Month-to-month is the majority class — validate partition count
SELECT COUNT(*) AS mtm_count
FROM telecom_curated
WHERE Contract = 'Month-to-month';
-- Observed: 3875 out of 7044 (~55%) — expected distribution
