-- =============================================================================
-- STORAGE FORMAT BENCHMARK
-- Purpose: compare TEXTFILE vs ORC+SNAPPY vs PARQUET using the same workload.
-- The benchmark is intentionally isolated from the production curated tables.
-- Re-running this script starts from the original staging snapshot.
-- =============================================================================

USE hive_db;
SET hive.exec.reducers.max = 4;
SET hive.enforce.bucketing = true;

DROP TABLE IF EXISTS telecom_text;
CREATE TABLE telecom_text (
    customerID STRING, gender STRING, SeniorCitizen INT, Partner STRING,
    Dependents STRING, tenure INT, PhoneService STRING, MultipleLines STRING,
    InternetService STRING, OnlineSecurity STRING, OnlineBackup STRING,
    DeviceProtection STRING, TechSupport STRING, StreamingTV STRING,
    StreamingMovies STRING, Contract STRING, PaperlessBilling STRING,
    PaymentMethod STRING, MonthlyCharges FLOAT, TotalCharges FLOAT, Churn STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

INSERT OVERWRITE TABLE telecom_text SELECT * FROM telecom_staging;

-- Controlled scale-up: 7 doublings -> 7,044 x 128 = 901,632 rows.
INSERT INTO telecom_text SELECT * FROM telecom_text;
INSERT INTO telecom_text SELECT * FROM telecom_text;
INSERT INTO telecom_text SELECT * FROM telecom_text;
INSERT INTO telecom_text SELECT * FROM telecom_text;
INSERT INTO telecom_text SELECT * FROM telecom_text;
INSERT INTO telecom_text SELECT * FROM telecom_text;
INSERT INTO telecom_text SELECT * FROM telecom_text;

SELECT COUNT(*) AS benchmark_row_count FROM telecom_text;

DROP TABLE IF EXISTS telecom_orc;
CREATE TABLE telecom_orc STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY') AS
SELECT * FROM telecom_text;

DROP TABLE IF EXISTS telecom_parquet;
CREATE TABLE telecom_parquet STORED AS PARQUET AS
SELECT * FROM telecom_text;

ANALYZE TABLE telecom_text COMPUTE STATISTICS;
ANALYZE TABLE telecom_orc COMPUTE STATISTICS;
ANALYZE TABLE telecom_parquet COMPUTE STATISTICS;

-- Inspect totalSize/numFiles after statistics are collected.
DESCRIBE FORMATTED telecom_text;
DESCRIBE FORMATTED telecom_orc;
DESCRIBE FORMATTED telecom_parquet;

-- Run the identical analytical workload. Record wall-clock time from the Hive
-- CLI output; do not treat timings as portable because cluster resources vary.
SELECT AVG(MonthlyCharges) AS avg_monthly_charges
FROM telecom_text
WHERE Churn = 'Yes';

SELECT AVG(MonthlyCharges) AS avg_monthly_charges
FROM telecom_orc
WHERE Churn = 'Yes';

SELECT AVG(MonthlyCharges) AS avg_monthly_charges
FROM telecom_parquet
WHERE Churn = 'Yes';

-- Integrity check: all three formats must return the same aggregate.
SELECT 'TEXTFILE' AS format_name, ROUND(AVG(MonthlyCharges), 4) AS avg_monthly_charges
FROM telecom_text WHERE Churn = 'Yes'
UNION ALL
SELECT 'ORC_SNAPPY', ROUND(AVG(MonthlyCharges), 4)
FROM telecom_orc WHERE Churn = 'Yes'
UNION ALL
SELECT 'PARQUET', ROUND(AVG(MonthlyCharges), 4)
FROM telecom_parquet WHERE Churn = 'Yes';
