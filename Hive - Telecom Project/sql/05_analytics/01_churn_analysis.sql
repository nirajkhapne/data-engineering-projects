-- =============================================================================
-- FILE: 01_churn_analysis.sql
-- PURPOSE: Business-level analytical queries on the star schema.
--          These are the "value delivery" queries — they answer the actual
--          business question: which customer segments have the highest churn?
-- =============================================================================

USE hive_db;

SET hive.auto.convert.join = true;


-- =============================================================================
-- QUERY 1: Churn Rate by Contract Type × Internet Service × Payment Method
-- Business Question: Which combination of attributes drives the highest churn?
--
-- RESULT INTERPRETATION (observed output):
--   Month-to-month + Fiber optic + Electronic check → 60.4% churn rate
--   This is the highest-risk segment. These customers:
--     - Have no lock-in (month-to-month)
--     - Pay the most (fiber optic is the premium tier)
--     - Use the least "sticky" payment method (manual check vs auto-pay)
--   Recommendation: Target this cohort with auto-pay incentives and
--   contract upgrade offers before tenure hits the 12-month mark.
-- =============================================================================
SELECT
    dc.contract_name,
    ds.internet_service,
    dp.payment_method,
    COUNT(*)                                                           AS total_customers,
    SUM(CASE WHEN f.Churn = 'Yes' THEN 1 ELSE 0 END)                  AS churned_customers,
    ROUND(
        SUM(CASE WHEN f.Churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*),
        4
    )                                                                  AS churn_rate
FROM fact_customer_activity f
JOIN dim_contract     dc ON f.contract_key = dc.contract_key
JOIN dim_payment      dp ON f.payment_key  = dp.payment_key
JOIN dim_service_type ds ON f.service_key  = ds.service_key
GROUP BY
    dc.contract_name,
    ds.internet_service,
    dp.payment_method
ORDER BY churn_rate DESC
LIMIT 10;


-- =============================================================================
-- QUERY 2: Average Monthly Charges for churned vs retained customers
-- Business Question: Are churned customers paying more than retained ones?
--
-- High MonthlyCharges + high churn = pricing pressure problem.
-- Low MonthlyCharges + high churn = value perception / service quality problem.
-- =============================================================================
SELECT
    Churn,
    COUNT(*)                        AS customer_count,
    ROUND(AVG(MonthlyCharges), 2)   AS avg_monthly_charges,
    ROUND(AVG(TotalCharges), 2)     AS avg_total_charges,
    ROUND(AVG(tenure), 1)           AS avg_tenure_months
FROM telecom_staging
GROUP BY Churn;


-- =============================================================================
-- QUERY 3: Churn rate by tenure bucket
-- Business Question: At which stage of the customer lifecycle does churn peak?
--
-- Insight: Early-tenure churn (0–12 months) is acquisition-quality problem.
-- Late-tenure churn (36+ months) is competitive/pricing pressure problem.
-- =============================================================================
SELECT
    CASE
        WHEN tenure BETWEEN 0  AND 12  THEN '0–12 months'
        WHEN tenure BETWEEN 13 AND 24  THEN '13–24 months'
        WHEN tenure BETWEEN 25 AND 36  THEN '25–36 months'
        WHEN tenure BETWEEN 37 AND 48  THEN '37–48 months'
        WHEN tenure BETWEEN 49 AND 60  THEN '49–60 months'
        ELSE '60+ months'
    END                                                            AS tenure_bucket,
    COUNT(*)                                                       AS total_customers,
    SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END)                AS churned,
    ROUND(
        SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*),
        4
    )                                                              AS churn_rate
FROM telecom_staging
GROUP BY
    CASE
        WHEN tenure BETWEEN 0  AND 12  THEN '0–12 months'
        WHEN tenure BETWEEN 13 AND 24  THEN '13–24 months'
        WHEN tenure BETWEEN 25 AND 36  THEN '25–36 months'
        WHEN tenure BETWEEN 37 AND 48  THEN '37–48 months'
        WHEN tenure BETWEEN 49 AND 60  THEN '49–60 months'
        ELSE '60+ months'
    END
ORDER BY churn_rate DESC;
