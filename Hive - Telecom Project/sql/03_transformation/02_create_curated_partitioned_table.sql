-- =============================================================================
-- CURATED SNAPSHOT
-- Partition by low-cardinality Contract and bucket by customerID.
-- =============================================================================

USE hive_db;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.auto.convert.join = true;
SET hive.optimize.bucketmapjoin = true;
SET hive.auto.convert.sortmerge.join = true;

DROP TABLE IF EXISTS telecom_curated;

CREATE TABLE telecom_curated (
    customerID          STRING,
    gender              STRING,
    SeniorCitizen       INT,
    Partner             STRING,
    Dependents          STRING,
    tenure              INT,
    PhoneService        STRING,
    MultipleLines       STRING,
    InternetService     STRING,
    OnlineSecurity      STRING,
    OnlineBackup        STRING,
    DeviceProtection    STRING,
    TechSupport         STRING,
    StreamingTV         STRING,
    StreamingMovies     STRING,
    PaperlessBilling    STRING,
    PaymentMethod       STRING,
    MonthlyCharges      FLOAT,
    TotalCharges        FLOAT,
    Churn               STRING
)
PARTITIONED BY (Contract STRING)
CLUSTERED BY (customerID) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE telecom_curated
PARTITION (Contract)
SELECT
    customerID, gender, SeniorCitizen, Partner, Dependents, tenure,
    PhoneService, MultipleLines, InternetService, OnlineSecurity,
    OnlineBackup, DeviceProtection, TechSupport, StreamingTV,
    StreamingMovies, PaperlessBilling, PaymentMethod, MonthlyCharges,
    TotalCharges, Churn, Contract
FROM telecom_staging;

SHOW PARTITIONS telecom_curated;
