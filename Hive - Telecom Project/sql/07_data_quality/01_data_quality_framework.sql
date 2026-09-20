-- =============================================================================
-- DATA QUALITY / AUDIT
-- Append-only per-run metrics with parameterized dates and run-over-run checks.
-- =============================================================================

USE hive_db;

SET hivevar:run_date = 2026-03-03;
SET hivevar:previous_run_date = 2026-03-02;

CREATE TABLE IF NOT EXISTS data_quality_metrics (
    run_date STRING,
    table_name STRING,
    row_count BIGINT,
    null_customerid BIGINT,
    null_totalcharges BIGINT,
    duplicate_customerids BIGINT,
    invalid_tenure BIGINT,
    invalid_contract BIGINT,
    created_ts TIMESTAMP
) STORED AS ORC;

-- Single-pass checks per table keep the DQ collection simple and reproducible.
INSERT INTO TABLE data_quality_metrics
SELECT '${hivevar:run_date}', 'telecom_staging',
       COUNT(*),
       SUM(CASE WHEN customerID IS NULL OR TRIM(customerID) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN TotalCharges IS NULL THEN 1 ELSE 0 END),
       COUNT(*) - COUNT(DISTINCT customerID),
       SUM(CASE WHEN tenure IS NULL OR tenure < 0 THEN 1 ELSE 0 END),
       SUM(CASE WHEN Contract IS NULL OR Contract NOT IN ('Month-to-month','One year','Two year') THEN 1 ELSE 0 END),
       CURRENT_TIMESTAMP()
FROM telecom_staging;

INSERT INTO TABLE data_quality_metrics
SELECT '${hivevar:run_date}', 'telecom_curated',
       COUNT(*),
       SUM(CASE WHEN customerID IS NULL OR TRIM(customerID) = '' THEN 1 ELSE 0 END),
       SUM(CASE WHEN TotalCharges IS NULL THEN 1 ELSE 0 END),
       COUNT(*) - COUNT(DISTINCT customerID),
       SUM(CASE WHEN tenure IS NULL OR tenure < 0 THEN 1 ELSE 0 END),
       SUM(CASE WHEN Contract IS NULL OR Contract NOT IN ('Month-to-month','One year','Two year') THEN 1 ELSE 0 END),
       CURRENT_TIMESTAMP()
FROM telecom_curated;

-- Alert query: returns only tables with >10% row-count regression.
SELECT c.table_name,
       c.row_count AS current_count,
       p.row_count AS previous_count,
       ROUND(
           CAST(c.row_count - p.row_count AS DOUBLE) / NULLIF(p.row_count, 0),
           4
       ) AS pct_change
FROM data_quality_metrics c
JOIN data_quality_metrics p
  ON c.table_name = p.table_name
WHERE c.run_date = '${hivevar:run_date}'
  AND p.run_date = '${hivevar:previous_run_date}'
  AND c.row_count < p.row_count * 0.90;

SELECT *
FROM data_quality_metrics
WHERE run_date = '${hivevar:run_date}'
ORDER BY table_name;
