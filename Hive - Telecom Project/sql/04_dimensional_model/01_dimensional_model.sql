-- =============================================================================
-- STAR SCHEMA
-- Grain: one row per customer in the latest curated snapshot.
-- Dimensions are rebuilt idempotently; facts are refreshed from curated data.
-- =============================================================================

USE hive_db;
SET hive.auto.convert.join = true;
SET hive.optimize.bucketmapjoin = true;

DROP TABLE IF EXISTS dim_contract;
CREATE TABLE dim_contract (contract_key INT, contract_name STRING) STORED AS ORC;
INSERT OVERWRITE TABLE dim_contract
SELECT ROW_NUMBER() OVER (ORDER BY Contract), Contract
FROM (SELECT DISTINCT Contract FROM telecom_curated WHERE Contract IS NOT NULL) x;

DROP TABLE IF EXISTS dim_payment;
CREATE TABLE dim_payment (payment_key INT, payment_method STRING) STORED AS ORC;
INSERT OVERWRITE TABLE dim_payment
SELECT ROW_NUMBER() OVER (ORDER BY PaymentMethod), PaymentMethod
FROM (SELECT DISTINCT PaymentMethod FROM telecom_curated WHERE PaymentMethod IS NOT NULL) x;

DROP TABLE IF EXISTS dim_service_type;
CREATE TABLE dim_service_type (service_key INT, internet_service STRING) STORED AS ORC;
INSERT OVERWRITE TABLE dim_service_type
SELECT ROW_NUMBER() OVER (ORDER BY InternetService), InternetService
FROM (SELECT DISTINCT InternetService FROM telecom_curated WHERE InternetService IS NOT NULL) x;

DROP TABLE IF EXISTS fact_customer_activity;
CREATE TABLE fact_customer_activity (
    customerID STRING, contract_key INT, payment_key INT, service_key INT,
    tenure INT, MonthlyCharges FLOAT, TotalCharges FLOAT, Churn STRING
) STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY');

INSERT OVERWRITE TABLE fact_customer_activity
SELECT t.customerID, c.contract_key, p.payment_key, s.service_key,
       t.tenure, t.MonthlyCharges, t.TotalCharges, t.Churn
FROM telecom_curated t
JOIN dim_contract c ON t.Contract = c.contract_name
JOIN dim_payment p ON t.PaymentMethod = p.payment_method
JOIN dim_service_type s ON t.InternetService = s.internet_service;

-- Scaled variant: partition by contract key and bucket by customer.
DROP TABLE IF EXISTS fact_customer_activity_partitioned;
CREATE TABLE fact_customer_activity_partitioned (
    customerID STRING, payment_key INT, service_key INT, tenure INT,
    MonthlyCharges FLOAT, TotalCharges FLOAT, Churn STRING
) PARTITIONED BY (contract_key INT)
CLUSTERED BY (customerID) INTO 4 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='SNAPPY');

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
INSERT OVERWRITE TABLE fact_customer_activity_partitioned
PARTITION (contract_key)
SELECT customerID, payment_key, service_key, tenure, MonthlyCharges,
       TotalCharges, Churn, contract_key
FROM fact_customer_activity;

-- Referential-integrity style reconciliation. A zero count means every
-- curated customer successfully mapped into the dimensional model.
SELECT COUNT(*) AS unmapped_curated_rows
FROM telecom_curated t
LEFT JOIN fact_customer_activity f ON t.customerID = f.customerID
WHERE f.customerID IS NULL;
