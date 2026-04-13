-- Check for duplicate customerIDs
SELECT customerID, COUNT(*)
FROM telecom_raw
GROUP BY customerID
HAVING COUNT(*) > 1;

-- Check for null or empty TotalCharges
SELECT COUNT(*)
FROM telecom_raw
WHERE TotalCharges IS NULL OR TotalCharges = '';

-- Check for total number of rows
SELECT COUNT(*) FROM telecom_raw;



-- Data Quality Metrics Table

CREATE TABLE IF NOT EXISTS data_quality_metrics (
run_date STRING,
table_name STRING,
row_count BIGINT,
null_totalcharges BIGINT,
duplicate_customerids BIGINT,
created_ts TIMESTAMP
)
STORED AS ORC;

SET hivevar:run_date=2026-03-03;

-- Insert Metrics (Single Scan Optimized)
INSERT INTO TABLE data_quality_metrics
SELECT
'${hivevar:run_date}' AS run_date,
'telecom_staging' AS table_name,
COUNT(*) AS row_count,
SUM(CASE WHEN TotalCharges IS NULL THEN 1 ELSE 0 END) AS null_totalcharges,
COUNT(*) - COUNT(DISTINCT customerID) AS duplicate_customerids,
CURRENT_TIMESTAMP()
FROM telecom_staging;

INSERT INTO TABLE data_quality_metrics
SELECT
'${hivevar:run_date}',
'telecom_curated',
COUNT(*),
SUM(CASE WHEN TotalCharges IS NULL THEN 1 ELSE 0 END),
COUNT(*) - COUNT(DISTINCT customerID),
CURRENT_TIMESTAMP()
FROM telecom_curated;
