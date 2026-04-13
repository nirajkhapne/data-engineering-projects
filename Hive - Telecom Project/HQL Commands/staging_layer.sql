-- Create staging layer directory
hadoop fs -mkdir -p /data/staging/telecom


-- Staging Layer: ORC Conversion

CREATE EXTERNAL TABLE telecom_staging (
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
TotalCharges FLOAT,
Churn STRING
)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY")
LOCATION '/data/staging/telecom/';


-- Insert data from raw layer to staging layer
INSERT OVERWRITE TABLE telecom_staging
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
END,
Churn
FROM telecom_raw;


