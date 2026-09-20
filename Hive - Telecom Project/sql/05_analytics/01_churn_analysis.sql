-- =============================================================================
-- ANALYTICS
-- Business-facing queries consume the dimensional model rather than raw/staging
-- tables. Ratios use DOUBLE arithmetic to avoid integer division.
-- =============================================================================

USE hive_db;
SET hive.auto.convert.join = true;

-- 1. Churn rate by contract x service x payment segment.
SELECT
    dc.contract_name,
    ds.internet_service,
    dp.payment_method,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN f.Churn = 'Yes' THEN 1 ELSE 0 END) AS churned_customers,
    ROUND(
        CAST(SUM(CASE WHEN f.Churn = 'Yes' THEN 1 ELSE 0 END) AS DOUBLE)
        / NULLIF(COUNT(*), 0), 4
    ) AS churn_rate
FROM fact_customer_activity f
JOIN dim_contract dc ON f.contract_key = dc.contract_key
JOIN dim_payment dp ON f.payment_key = dp.payment_key
JOIN dim_service_type ds ON f.service_key = ds.service_key
GROUP BY dc.contract_name, ds.internet_service, dp.payment_method
ORDER BY churn_rate DESC
LIMIT 10;

-- 2. Customer economics by churn outcome.
SELECT
    Churn,
    COUNT(*) AS customer_count,
    ROUND(AVG(MonthlyCharges), 2) AS avg_monthly_charges,
    ROUND(AVG(TotalCharges), 2) AS avg_total_charges,
    ROUND(AVG(tenure), 1) AS avg_tenure_months
FROM fact_customer_activity
GROUP BY Churn;

-- 3. Churn rate by tenure bucket.
SELECT
    CASE
        WHEN tenure BETWEEN 0 AND 12 THEN '0-12 months'
        WHEN tenure BETWEEN 13 AND 24 THEN '13-24 months'
        WHEN tenure BETWEEN 25 AND 36 THEN '25-36 months'
        WHEN tenure BETWEEN 37 AND 48 THEN '37-48 months'
        WHEN tenure BETWEEN 49 AND 60 THEN '49-60 months'
        ELSE '60+ months'
    END AS tenure_bucket,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) AS churned_customers,
    ROUND(
        CAST(SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) AS DOUBLE)
        / NULLIF(COUNT(*), 0), 4
    ) AS churn_rate
FROM fact_customer_activity
GROUP BY
    CASE
        WHEN tenure BETWEEN 0 AND 12 THEN '0-12 months'
        WHEN tenure BETWEEN 13 AND 24 THEN '13-24 months'
        WHEN tenure BETWEEN 25 AND 36 THEN '25-36 months'
        WHEN tenure BETWEEN 37 AND 48 THEN '37-48 months'
        WHEN tenure BETWEEN 49 AND 60 THEN '49-60 months'
        ELSE '60+ months'
    END
ORDER BY churn_rate DESC;

-- 4. Curated snapshot reconciliation.
SELECT COUNT(*) AS fact_rows FROM fact_customer_activity;
SELECT COUNT(*) AS curated_rows FROM telecom_curated;
