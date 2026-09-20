-- =============================================================================
-- INCREMENTAL SNAPSHOT MERGE
-- Pattern: UNION ALL + ROW_NUMBER() for non-ACID Hive tables.
-- The delta source supplies the authoritative load_date.
-- Same-day conflicts prefer the delta row over the base snapshot.
-- =============================================================================

USE hive_db;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.enforce.bucketing = true;
SET hivevar:initial_load_date = 2026-03-01;

CREATE EXTERNAL TABLE IF NOT EXISTS telecom_delta_raw (
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
    Churn               STRING,
    load_date           STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/telecom_delta/';

DROP TABLE IF EXISTS telecom_delta_staging;
CREATE TABLE telecom_delta_staging (
    customerID          STRING, gender STRING, SeniorCitizen INT, Partner STRING,
    Dependents STRING, tenure INT, PhoneService STRING, MultipleLines STRING,
    InternetService STRING, OnlineSecurity STRING, OnlineBackup STRING,
    DeviceProtection STRING, TechSupport STRING, StreamingTV STRING,
    StreamingMovies STRING, Contract STRING, PaperlessBilling STRING,
    PaymentMethod STRING, MonthlyCharges FLOAT, TotalCharges FLOAT,
    Churn STRING, load_date STRING
) STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY');

INSERT OVERWRITE TABLE telecom_delta_staging
SELECT
    customerID, gender, SeniorCitizen, Partner, Dependents, tenure,
    PhoneService, MultipleLines, InternetService, OnlineSecurity,
    OnlineBackup, DeviceProtection, TechSupport, StreamingTV,
    StreamingMovies, Contract, PaperlessBilling, PaymentMethod,
    MonthlyCharges,
    CASE WHEN TotalCharges IS NULL OR TRIM(TotalCharges) = '' THEN NULL
         ELSE CAST(TotalCharges AS FLOAT) END,
    Churn, load_date
FROM telecom_delta_raw;

-- Rebuild intermediate snapshots on every run: avoids stale CTAS results.
DROP TABLE IF EXISTS telecom_merged_temp;
CREATE TABLE telecom_merged_temp STORED AS ORC AS
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY customerID
           ORDER BY load_date DESC, source_priority DESC
       ) AS rn
FROM (
    SELECT customerID, gender, SeniorCitizen, Partner, Dependents, tenure,
           PhoneService, MultipleLines, InternetService, OnlineSecurity,
           OnlineBackup, DeviceProtection, TechSupport, StreamingTV,
           StreamingMovies, Contract, PaperlessBilling, PaymentMethod,
           MonthlyCharges, TotalCharges, Churn,
           '${hivevar:initial_load_date}' AS load_date,
           0 AS source_priority
    FROM telecom_staging

    UNION ALL

    SELECT customerID, gender, SeniorCitizen, Partner, Dependents, tenure,
           PhoneService, MultipleLines, InternetService, OnlineSecurity,
           OnlineBackup, DeviceProtection, TechSupport, StreamingTV,
           StreamingMovies, Contract, PaperlessBilling, PaymentMethod,
           MonthlyCharges, TotalCharges, Churn, load_date,
           1 AS source_priority
    FROM telecom_delta_staging
) u;

DROP TABLE IF EXISTS telecom_latest;
CREATE TABLE telecom_latest STORED AS ORC AS
SELECT
    customerID, gender, SeniorCitizen, Partner, Dependents, tenure,
    PhoneService, MultipleLines, InternetService, OnlineSecurity,
    OnlineBackup, DeviceProtection, TechSupport, StreamingTV,
    StreamingMovies, Contract, PaperlessBilling, PaymentMethod,
    MonthlyCharges, TotalCharges, Churn, load_date
FROM telecom_merged_temp
WHERE rn = 1;

-- Refresh the production snapshot from the latest customer record.
DROP TABLE IF EXISTS telecom_curated;
CREATE TABLE telecom_curated (
    customerID STRING, gender STRING, SeniorCitizen INT, Partner STRING,
    Dependents STRING, tenure INT, PhoneService STRING, MultipleLines STRING,
    InternetService STRING, OnlineSecurity STRING, OnlineBackup STRING,
    DeviceProtection STRING, TechSupport STRING, StreamingTV STRING,
    StreamingMovies STRING, PaperlessBilling STRING, PaymentMethod STRING,
    MonthlyCharges FLOAT, TotalCharges FLOAT, Churn STRING
)
PARTITIONED BY (Contract STRING)
CLUSTERED BY (customerID) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

INSERT OVERWRITE TABLE telecom_curated
PARTITION (Contract)
SELECT customerID, gender, SeniorCitizen, Partner, Dependents, tenure,
       PhoneService, MultipleLines, InternetService, OnlineSecurity,
       OnlineBackup, DeviceProtection, TechSupport, StreamingTV,
       StreamingMovies, PaperlessBilling, PaymentMethod, MonthlyCharges,
       TotalCharges, Churn, Contract
FROM telecom_latest;

SELECT COUNT(*) AS latest_customer_count FROM telecom_latest;
SELECT COUNT(*) AS delta_customer_count FROM telecom_delta_staging;
