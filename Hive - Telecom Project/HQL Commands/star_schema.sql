
-- Star Schema: Dimensions + Fact

CREATE TABLE dim_contract (
contract_key INT,
contract_name STRING
) STORED AS ORC;

-- Insert data into dim_contract table
INSERT INTO dim_contract
SELECT
ROW_NUMBER() OVER (ORDER BY Contract),
Contract
FROM (
  SELECT DISTINCT Contract
  FROM telecom_staging
) t;


CREATE TABLE dim_payment (
payment_key INT,
payment_method STRING
) STORED AS ORC;

-- Insert data into dim_payment table
INSERT INTO dim_payment
SELECT
ROW_NUMBER() OVER (ORDER BY PaymentMethod),
PaymentMethod
FROM (
  SELECT DISTINCT PaymentMethod
  FROM telecom_staging
) t;


CREATE TABLE dim_service_type (
service_key INT,
internet_service STRING
) STORED AS ORC;

-- Insert data into dim_service_type table
INSERT INTO dim_service_type
SELECT
ROW_NUMBER() OVER (ORDER BY InternetService),
InternetService
FROM (
  SELECT DISTINCT InternetService
  FROM telecom_staging
) t;



CREATE TABLE fact_customer_activity (
customerID STRING,
contract_key INT,
payment_key INT,
service_key INT,
tenure INT,
MonthlyCharges FLOAT,
TotalCharges FLOAT,
Churn STRING
) STORED AS ORC;

-- Insert data into fact_customer_activity table
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
JOIN dim_contract c
  ON t.Contract = c.contract_name
JOIN dim_payment p
  ON t.PaymentMethod = p.payment_method
JOIN dim_service_type s
  ON t.InternetService = s.internet_service;


-- Optional Optimization: Partitioned and Bucketed
CREATE TABLE fact_customer_activity_partitioned (
customerID STRING,
payment_key INT,
service_key INT,
tenure INT,
MonthlyCharges FLOAT,
TotalCharges FLOAT,
Churn STRING
)
PARTITIONED BY (contract_key INT)
CLUSTERED BY (customerID) INTO 4 BUCKETS
STORED AS ORC;



-- business queries:

-- 1. Churn rate by contract type and payment method
SELECT 
dc.contract_name,
ds.internet_service,
dp.payment_method,
COUNT(*) AS total_customers,
SUM(CASE WHEN f.Churn='Yes' THEN 1 ELSE 0 END) AS churned_customers,
SUM(CASE WHEN f.Churn='Yes' THEN 1 ELSE 0 END)/COUNT(*) AS churn_rate
FROM fact_customer_activity f
JOIN dim_contract dc ON f.contract_key = dc.contract_key
JOIN dim_payment dp ON f.payment_key = dp.payment_key
JOIN dim_service_type ds ON f.service_key = ds.service_key
GROUP BY dc.contract_name, ds.internet_service, dp.payment_method
ORDER BY churn_rate DESC
LIMIT 10;



-- 2. Average monthly charges by contract type
