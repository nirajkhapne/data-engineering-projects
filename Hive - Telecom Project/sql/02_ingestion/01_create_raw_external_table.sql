-- =============================================================================
-- RAW INGESTION
-- Purpose: expose source CSV files through an EXTERNAL Hive table.
-- Raw data remains outside Hive's ownership and is never transformed in place.
-- =============================================================================

USE hive_db;

CREATE EXTERNAL TABLE IF NOT EXISTS telecom_raw (
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
    TotalCharges        STRING,
    Churn               STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/telecom/';

-- Source-level reconciliation and contract checks.
SELECT COUNT(*) AS raw_row_count FROM telecom_raw;

SELECT COUNT(*) AS null_or_empty_customer_ids
FROM telecom_raw
WHERE customerID IS NULL OR TRIM(customerID) = '';

SELECT COUNT(*) AS duplicate_customer_ids
FROM (
    SELECT customerID
    FROM telecom_raw
    GROUP BY customerID
    HAVING COUNT(*) > 1
) d;

SELECT COUNT(*) AS invalid_totalcharges_for_nonzero_tenure
FROM telecom_raw
WHERE tenure > 0
  AND (TotalCharges IS NULL OR TRIM(TotalCharges) = '');
