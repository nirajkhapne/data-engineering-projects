
-- File Format Benchmark Queries

-- Create text file table
CREATE TABLE telecom_text (
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
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE telecom_text
SELECT * FROM telecom_staging;

--Scale Dataset to ~1 Million Rows
INSERT INTO telecom_text
SELECT * FROM telecom_text;

CREATE TABLE telecom_orc
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY")
AS
SELECT * FROM telecom_text;

CREATE TABLE telecom_parquet
STORED AS PARQUET
AS
SELECT * FROM telecom_text;

--Clear Cache Effect
SET hive.exec.reducers.max=4;


SELECT COUNT(*) FROM telecom_text;
SELECT COUNT(*) FROM telecom_orc;
SELECT COUNT(*) FROM telecom_parquet;

SELECT AVG(MonthlyCharges)
FROM telecom_orc
WHERE Churn = 'Yes';

SELECT AVG(MonthlyCharges)
FROM telecom_orc
WHERE Churn = 'Yes';

SELECT AVG(MonthlyCharges)
FROM telecom_parquet
WHERE Churn = 'Yes';


