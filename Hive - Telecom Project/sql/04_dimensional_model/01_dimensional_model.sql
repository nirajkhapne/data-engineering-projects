-- =============================================================================
-- FILE: 01_dimensional_model.sql
-- PURPOSE: Build a star schema dimensional model on top of the staging layer.
--          Enables BI tooling (Power BI, Superset, Metabase) to query via
--          foreign-key joins rather than scanning the full flat table.
--
-- SCHEMA:
--   fact_customer_activity  (grain: one row per customer)
--       ├── dim_contract     (3 rows: Month-to-month, One year, Two year)
--       ├── dim_payment      (4 rows: Electronic check, Mailed check, etc.)
--       └── dim_service_type (3 rows: DSL, Fiber optic, No)
--
-- SURROGATE KEY GENERATION:
--   ROW_NUMBER() OVER (ORDER BY <natural_key>) is used to generate integer
--   surrogate keys. This is deterministic as long as the source data doesn't
--   change — which is safe for these low-cardinality, static dimension tables.
--   For slowly-changing dimensions, use a dedicated key table with MAX(key)+1.
--
-- LIMITATION OF THIS MODEL:
--   The current fact table has no date dimension — it's a snapshot, not a
--   time-series fact table. To support trend analysis (churn rate by month),
--   you'd need a date dimension and a fact grain of (customerID, month).
-- =============================================================================

USE hive_db;

SET hive.auto.convert.join           = true;
SET hive.optimize.bucketmapjoin      = true;
SET hive.auto.convert.sortmerge.join = true;


-- =============================================================================
-- DIMENSIONS
-- =============================================================================

-- dim_contract
CREATE TABLE IF NOT EXISTS dim_contract (
    contract_key  INT,
    contract_name STRING
)
STORED AS ORC;

INSERT INTO dim_contract
SELECT
    ROW_NUMBER() OVER (ORDER BY Contract) AS contract_key,
    Contract                              AS contract_name
FROM (
    SELECT DISTINCT Contract
    FROM telecom_staging
) t;
-- Result: 3 rows (Month-to-month=1, One year=2, Two year=3)


-- dim_payment
CREATE TABLE IF NOT EXISTS dim_payment (
    payment_key    INT,
    payment_method STRING
)
STORED AS ORC;

INSERT INTO dim_payment
SELECT
    ROW_NUMBER() OVER (ORDER BY PaymentMethod) AS payment_key,
    PaymentMethod                              AS payment_method
FROM (
    SELECT DISTINCT PaymentMethod
    FROM telecom_staging
) t;
-- Result: 4 rows


-- dim_service_type
CREATE TABLE IF NOT EXISTS dim_service_type (
    service_key      INT,
    internet_service STRING
)
STORED AS ORC;

INSERT INTO dim_service_type
SELECT
    ROW_NUMBER() OVER (ORDER BY InternetService) AS service_key,
    InternetService                              AS internet_service
FROM (
    SELECT DISTINCT InternetService
    FROM telecom_staging
) t;
-- Result: 3 rows (DSL, Fiber optic, No)


-- =============================================================================
-- FACT TABLE
-- grain: one row per customer (snapshot)
-- =============================================================================
CREATE TABLE IF NOT EXISTS fact_customer_activity (
    customerID    STRING,
    contract_key  INT,
    payment_key   INT,
    service_key   INT,
    tenure        INT,
    MonthlyCharges FLOAT,
    TotalCharges  FLOAT,
    Churn         STRING
)
STORED AS ORC;

INSERT OVERWRITE TABLE fact_customer_activity
SELECT
    t.customerID,
    c.contract_key,
    p.payment_key,
    s.service_key,
    t.tenure,
    t.MonthlyCharges,
    t.TotalCharges,
    t.Churn
FROM telecom_staging t
JOIN dim_contract     c ON t.Contract        = c.contract_name
JOIN dim_payment      p ON t.PaymentMethod   = p.payment_method
JOIN dim_service_type s ON t.InternetService = s.internet_service;
-- NOTE: These are small-table joins. Hive's auto broadcast join (MapJoin)
--       will kick in automatically since dim tables are well under the
--       hive.auto.convert.join.noconditionaltask.size threshold (~25MB).


-- =============================================================================
-- PARTITIONED FACT (for scale)
-- When fact row count grows beyond 10M+, partition by contract_key to
-- allow partition pruning on the most common filter dimension.
-- =============================================================================
CREATE TABLE IF NOT EXISTS fact_customer_activity_partitioned (
    customerID    STRING,
    payment_key   INT,
    service_key   INT,
    tenure        INT,
    MonthlyCharges FLOAT,
    TotalCharges  FLOAT,
    Churn         STRING
)
PARTITIONED BY (contract_key INT)
CLUSTERED BY (customerID) INTO 4 BUCKETS
STORED AS ORC;
-- Populated separately as part of incremental refresh cycle
