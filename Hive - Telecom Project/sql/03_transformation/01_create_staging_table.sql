-- =============================================================================
-- FILE: 01_create_staging_table.sql
-- PURPOSE: Promote raw data to a typed, compressed ORC staging table.
--          This is the cleansed, single-source-of-truth layer for all
--          downstream transformations.
--
-- KEY DECISIONS:
--   * ORC + SNAPPY: columnar format reduces I/O for analytical queries;
--     SNAPPY provides a fast compression codec (vs ZLIB which is slower
--     but produces smaller files — SNAPPY is the right tradeoff here
--     because this layer is read frequently).
--   * TotalCharges cast here with explicit CASE: CAST alone would silently
--     return NULL for empty strings in some Hive versions — the CASE guard
--     makes intent explicit and behavior version-independent.
--   * INSERT OVERWRITE: idempotent pattern — re-running the job won't
--     duplicate data, unlike INSERT INTO.
-- =============================================================================

USE hive_db;

CREATE TABLE IF NOT EXISTS telecom_staging (
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
    TotalCharges    FLOAT,   -- Promoted from STRING; nulls retained for tenure=0 records
    Churn           STRING
)
STORED AS ORC
TBLPROPERTIES ("orc.compress" = "SNAPPY");


INSERT OVERWRITE TABLE telecom_staging
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
    Churn
FROM telecom_raw;


-- =============================================================================
-- RECONCILIATION: Staging row count must match raw
-- =============================================================================
SELECT COUNT(*) AS staging_row_count FROM telecom_staging;
-- Expected: 7044 (must match telecom_raw)
