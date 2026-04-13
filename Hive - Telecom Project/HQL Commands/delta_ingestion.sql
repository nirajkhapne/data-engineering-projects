-- Create delta raw layer directory
hadoop fs -mkdir -p /data/raw/telecom_delta

-- Create delta raw layer table
CREATE EXTERNAL TABLE telecom_delta_raw (
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
Contract STRING,
PaperlessBilling STRING,
PaymentMethod STRING,
MonthlyCharges FLOAT,
TotalCharges STRING,
Churn STRING,
load_date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/telecom_delta/';

-- Create delta staging layer table
CREATE TABLE telecom_delta_staging
STORED AS ORC
AS
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
Contract,
PaperlessBilling,
PaymentMethod,
MonthlyCharges,
CASE 
  WHEN TotalCharges = '' THEN NULL
  ELSE CAST(TotalCharges AS FLOAT)
END AS TotalCharges,
Churn,
load_date
FROM telecom_delta_raw;


-- Create delta merged temp table
CREATE TABLE telecom_merged_temp
STORED AS ORC
AS
SELECT *,
ROW_NUMBER() OVER (
  PARTITION BY customerID
  ORDER BY load_date DESC
) AS rn
FROM (
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
  Contract,
  PaperlessBilling,
  PaymentMethod,
  MonthlyCharges,
  TotalCharges,
  Churn,
  '2026-03-01' AS load_date
  FROM telecom_staging
  
  UNION ALL
  
  SELECT * FROM telecom_delta_staging
) t;


-- Create delta latest table
CREATE TABLE telecom_latest
STORED AS ORC
AS
SELECT *
FROM telecom_merged_temp
WHERE rn = 1;


-- Insert data from delta latest table to curated layer
--maintain immutability in raw
--version records via load_date
--simulate merge without ACID
--avoid row-level update
--use partition overwrite instead of full table drop

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
FROM telecom_latest;





