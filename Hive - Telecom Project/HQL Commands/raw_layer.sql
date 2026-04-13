-- Create raw layer directory
hadoop fs -mkdir -p /data/raw/telecom
hadoop fs -put Telecom_customer_churn_data.csv /data/raw/telecom

-- Raw Layer: External Table Creation

CREATE EXTERNAL TABLE telecom_raw (
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
TotalCharges STRING, -- TotalCharges as STRING. Because real dataset may contains blanks, casting fails.
Churn STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/telecom/';
