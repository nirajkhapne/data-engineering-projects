-- Create curated layer directory
hadoop fs -mkdir -p /data/curated/telecom

-- Curated Layer: Partition + Bucket

CREATE EXTERNAL TABLE telecom_curated (
customerID STRING,
gender STRING,
SeniorCitizen INT,
Partner STRING,
Dependents STRING,
tenure INT,
PhoneService STRING,
MultipleLines STRING,
InternetService STRING,
OnlineSecurity STRING,
OnlineBackup STRING,
DeviceProtection STRING,
TechSupport STRING,
StreamingTV STRING,
StreamingMovies STRING,
PaperlessBilling STRING,
PaymentMethod STRING,
MonthlyCharges FLOAT,
TotalCharges FLOAT,
Churn STRING
)
PARTITIONED BY (Contract STRING)
CLUSTERED BY (customerID) INTO 4 BUCKETS
STORED AS ORC
LOCATION '/data/curated/telecom/';

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE telecom_curated
PARTITION (Contract)
SELECT 
customerID,
gender,
SeniorCitizen,
Partner,
Dependents,
tenure,
PhoneService,
MultipleLines,
InternetService,
OnlineSecurity,
OnlineBackup,
DeviceProtection,
TechSupport,
StreamingTV,
StreamingMovies,
PaperlessBilling,
PaymentMethod,
MonthlyCharges,
TotalCharges,
Churn,
Contract 
FROM telecom_staging;

-- Check number of rows in curated layer for Month-to-month contract
SELECT COUNT(*) FROM telecom_curated
WHERE Contract = 'Month-to-month';

