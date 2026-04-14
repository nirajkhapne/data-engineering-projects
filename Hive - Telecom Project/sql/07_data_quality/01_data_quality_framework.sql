-- =============================================================================
-- FILE: 01_data_quality_framework.sql
-- PURPOSE: Automated data quality tracking table + daily DQ metric collection.
--          Enables run-over-run row count comparison and anomaly detection.
--
-- DESIGN NOTES:
--   1. data_quality_metrics is an append-only audit log — never UPDATE/DELETE.
--   2. The created_ts column was added via ALTER TABLE after initial creation,
--      documenting that schema evolution happened. Note it here so reviewers
--      understand the ALTER TABLE in the session log.
--   3. Hive 3.1's CTE-in-INSERT (WITH ... AS INSERT) is NOT supported in the
--      CLI interactive mode — the parser throws NoViableAltException. Use
--      inline aggregation with CASE WHEN instead (documented below).
--   4. Using reserved words as aliases (e.g., alias = 'current') causes
--      NoViableAltException in the SELECT clause. Always use non-reserved
--      aliases (c, p, prev, etc.).
--
-- LESSONS FROM FAILED QUERIES (documented for transparency):
--   FAILED: Scalar subqueries in SELECT list
--     → Hive does not support subquery expressions outside WHERE/HAVING
--   FAILED: WITH (CTE) before INSERT INTO
--     → Hive CLI does not support CTE-prefixed INSERT
--   FAILED: Alias 'current' in FROM clause
--     → 'current' is a reserved word in Hive's parser
--   WORKING: Single-pass CASE WHEN aggregation (documented below)
-- =============================================================================

USE hive_db;


-- =============================================================================
-- SETUP: Data quality metrics table
-- =============================================================================
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    run_date              STRING,
    table_name            STRING,
    row_count             BIGINT,
    null_totalcharges     BIGINT,
    duplicate_customerids BIGINT,
    created_ts            TIMESTAMP
)
STORED AS ORC;

-- NOTE: If the table was created without created_ts, run:
-- ALTER TABLE data_quality_metrics ADD COLUMNS (created_ts TIMESTAMP);


-- =============================================================================
-- DAILY DQ INSERT — Parameterized by run_date
-- Single-pass aggregation: avoids multiple table scans (more efficient than
-- separate COUNT(*) queries joined via CROSS JOIN).
-- =============================================================================
SET hivevar:run_date = 2026-03-03;


-- DQ metrics for telecom_staging
INSERT INTO TABLE data_quality_metrics
SELECT
    '${hivevar:run_date}'                                         AS run_date,
    'telecom_staging'                                             AS table_name,
    COUNT(*)                                                      AS row_count,
    SUM(CASE WHEN TotalCharges IS NULL THEN 1 ELSE 0 END)         AS null_totalcharges,
    COUNT(*) - COUNT(DISTINCT customerID)                         AS duplicate_customerids,
    -- Duplicate detection logic:
    -- COUNT(*) - COUNT(DISTINCT customerID) = 0 → no duplicates
    -- If positive, there are (N - distinct) extra rows to investigate
    CURRENT_TIMESTAMP()                                           AS created_ts
FROM telecom_staging;


-- DQ metrics for telecom_curated
INSERT INTO TABLE data_quality_metrics
SELECT
    '${hivevar:run_date}',
    'telecom_curated',
    COUNT(*),
    SUM(CASE WHEN TotalCharges IS NULL THEN 1 ELSE 0 END),
    COUNT(*) - COUNT(DISTINCT customerID),
    CURRENT_TIMESTAMP()
FROM telecom_curated;


-- =============================================================================
-- ANOMALY DETECTION: Row count regression alert
-- Flags tables where today's row count dropped by more than 10% vs yesterday.
-- In production, wire this output to an alerting system (PagerDuty, Slack).
-- =============================================================================
SELECT *
FROM (
    SELECT
        (c.row_count - p.row_count) / p.row_count AS pct_change,
        c.table_name,
        c.run_date                                AS current_date,
        p.run_date                                AS prev_date,
        c.row_count                               AS current_count,
        p.row_count                               AS prev_count
    FROM data_quality_metrics c
    JOIN data_quality_metrics p
      ON c.table_name = p.table_name
    WHERE c.run_date = '${hivevar:run_date}'
      AND p.run_date = '2026-03-02'
) t
WHERE pct_change < -0.10;
-- Returns rows only if a table lost >10% of its rows since last run.
-- Silent = healthy. Any result = pipeline incident.


-- =============================================================================
-- AUDIT VIEW: Full DQ history for a table
-- =============================================================================
SELECT
    run_date,
    table_name,
    row_count,
    null_totalcharges,
    duplicate_customerids,
    created_ts
FROM data_quality_metrics
WHERE table_name = 'telecom_staging'
ORDER BY run_date DESC;
