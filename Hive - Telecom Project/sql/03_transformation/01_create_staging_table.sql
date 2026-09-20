-- =============================================================================
-- STAGING
-- Typed, compressed and idempotently rebuilt from the raw external table.
-- =============================================================================

USE hive_db;

DROP TABLE IF EXISTS telecom_staging;

CREATE TABLE telecom_staging (
    customerID          STRING,
    gender              STRING,
    SeniorCitizen       INT,
    Partner             STRING,
    Dependents          STRING,
    tenure              INT,
    PhoneService        STRING,
    MultipleLines       STRING,
    InternetService     STRING,
    OnlineSecurity      STRING,
    OnlineBackup        STRING,
    DeviceProtection    STRING,
    TechSupport         STRING,
    StreamingTV         STRING,
    StreamingMovies     STRING,
    Contract            STRING,
    PaperlessBilling    STRING,
    PaymentMethod       STRING,
    MonthlyCharges      FLOAT,
    TotalCharges        FLOAT,
    Churn               STRING
)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE telecom_staging
SELECT
    customerID, gender, SeniorCitizen, Partner, Dependents, tenure,
    PhoneService, MultipleLines, InternetService, OnlineSecurity,
    OnlineBackup, DeviceProtection, TechSupport, StreamingTV,
    StreamingMovies, Contract, PaperlessBilling, PaymentMethod,
    MonthlyCharges,
    CASE
        WHEN TotalCharges IS NULL OR TRIM(TotalCharges) = '' THEN NULL
        ELSE CAST(TotalCharges AS FLOAT)
    END AS TotalCharges,
    Churn
FROM telecom_raw;

-- Fail-fast checks are represented as result sets so they can be wired to an
-- orchestration layer without hiding bad records.
SELECT COUNT(*) AS staging_row_count FROM telecom_staging;
SELECT COUNT(*) AS staging_distinct_customers FROM telecom_staging WHERE customerID IS NOT NULL;
