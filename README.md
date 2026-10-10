# 👋 Hi, I'm Niraj Khapne

### Data Analytics Engineer | Data Engineering | Python | SQL | PySpark | AWS | Spark | Kafka | Azure Databricks

---

## 🚀 About Me

Data Analytics Engineer with **3.8+ years of experience** building production data pipelines and analytical data platforms across AWS and distributed data ecosystems.

My professional work focuses on:

- Multi-source ETL/ELT pipelines and analytical data marts
- Complex SQL transformations and business-rule-driven allocation workflows
- Amazon Redshift data platforms
- Batch and incremental data processing
- Data quality, deduplication, and pipeline reliability
- Data integration, ranking, and downstream data delivery

Alongside my professional experience, I build portfolio projects covering **stream processing, data warehousing, and lakehouse engineering** using Kafka, Spark Structured Streaming, Hive, HDFS, MongoDB, FastAPI, and Azure Databricks.

---

## 🛠️ Tech Stack

### Languages & Processing
- Python
- SQL
- Pandas
- PySpark
- Apache Spark
- Spark Structured Streaming
- Hive / HQL

### Cloud & Data Platforms
- AWS: Amazon Redshift, Amazon S3, AWS Glue, Amazon Athena
- Azure: Azure Databricks, Azure Data Lake Storage Gen2 (ADLS Gen2)
- Delta Lake
- Unity Catalog

### Data Engineering & Warehousing
- ETL / ELT and data pipelines
- Batch and incremental processing
- Data integration and transformation
- Data quality and validation
- Data deduplication and idempotent processing
- Data warehousing and data marts
- Dimensional modelling and star schemas
- Partitioning and bucketing

### Streaming & Distributed Systems
- Apache Kafka
- Event-time processing and watermarking
- Windowed aggregations and stateful stream processing
- Stream-stream joins
- Checkpointing and recovery patterns
- Event-driven architectures

### Databases & Tools
- MongoDB
- MySQL
- PostgreSQL
- HDFS
- Git
- Linux
- Docker
- FastAPI
- Apache Airflow
- Lakeflow Jobs
- Amazon QuickSight, Jaspersoft, Grafana

---

## 💼 Professional Data Engineering Experience

### Production Data Pipelines
- Engineered Redshift-based ETL pipelines integrating event, relational, campaign, user-engagement, and historical datasets for lead generation and downstream delivery systems.
- Developed complex SQL transformations using CTEs, temporary tables, multi-source joins, aggregations, and window functions for campaign fulfilment, ranking, eligibility, and allocation logic.
- Implemented deduplication, historical-exposure checks, incremental processing, archival and retention controls, and data-quality validations.

### Analytical Data Platforms
- Developed Spark-based pipelines integrating external SEO and search-performance data into curated Redshift data marts.
- Built configuration-driven transformation and ranking workflows for SEO and keyword-intelligence use cases.

---

## 🔥 Featured Projects

### 📌 GizmoBox E-Commerce Lakehouse — Azure Databricks

**Technologies:** `Azure Databricks` `PySpark` `Delta Lake` `ADLS Gen2` `Unity Catalog` `Auto Loader` `Lakeflow Jobs`

Designing a medallion lakehouse for multi-format e-commerce data, with a focus on reliable ingestion, incremental processing, data quality, and analytical modelling.

**Key engineering areas**
- Bronze, Silver, and Gold data layers
- Batch ingestion and Auto Loader-based file discovery
- Explicit schemas, source metadata, and malformed-record handling
- Reusable PySpark transformations and Delta Lake writes
- Customer, order, payment, and refund data modelling
- Data-quality checks, audit logging, and idempotent processing
- Lakeflow Jobs task dependencies and retry configuration
- Unit tests and local simulation using synthetic fixtures

**Status:** Local/static checks and synthetic tests have passed. End-to-end execution in Azure Databricks, including live ADLS/Unity Catalog permissions, Auto Loader recovery, and Lakeflow Jobs execution, remains to be validated.

👉 **[View GizmoBox project](https://github.com/nirajkhapne/data-engineering-projects/tree/main/GizmoBox-Azure-Databricks-Lakehouse)**

### 📌 Real-Time Streaming Analytics Platform

**Technologies:** `Kafka` `PySpark` `Spark Structured Streaming` `MongoDB` `FastAPI` `Docker`

Built a portfolio streaming platform for user, transaction, order, and payment events, focusing on event-time processing and stateful stream correlation.

**Key features**
- Modular Kafka ingestion and Spark Structured Streaming
- Stateless transformations and stateful aggregations
- Event-time windowing and watermarking for late events
- Stream-stream joins for order-payment correlation
- Checkpoint configuration and recovery patterns
- Idempotent MongoDB upserts
- FastAPI analytics and health endpoints
- Docker-based local infrastructure

**Validation note:** Static checks and tests passed; a complete live Kafka-to-Spark-to-MongoDB runtime test was not available in the development environment.

👉 [View project](https://github.com/nirajkhapne/data-engineering-projects/tree/main/streaming-analytics-platform)

### 📌 Telecom Customer Churn Data Warehouse

**Technologies:** `Hive` `HQL` `HDFS` `Tez` `YARN` `ORC` `Parquet` `SQL`

Built a layered Hive warehouse for telecom customer churn analytics, focusing on incremental snapshots, dimensional modelling, and storage-format comparisons.

**Key features**
- Raw, staging, and curated layers
- Incremental snapshot processing and idempotent refreshes
- Latest-record selection and source-priority handling
- ORC/SNAPPY storage, partitioning, and bucketing
- Fact and dimension tables using a star schema
- Data-quality checks and run-over-run row-count validation
- TEXTFILE vs ORC vs Parquet benchmarking on a 901K-row workload

👉 [View project](https://github.com/nirajkhapne/data-engineering-projects/tree/main/Hive%20-%20Telecom%20Project)

### 📌 Kafka + MongoDB Streaming Pipeline

**Technologies:** `Kafka` `MongoDB` `FastAPI` `Python` `Docker`

Built a modular event-driven pipeline to ingest Kafka events, persist processed data in MongoDB, and expose it through REST APIs.

**Key features**
- Kafka producer-consumer architecture
- Event ingestion and input validation
- MongoDB sink integration
- Modular pipeline components
- REST APIs for querying processed data
- Docker-based development environment

👉 [View project](https://github.com/nirajkhapne/data-engineering-projects/tree/main/kafka-mongodb-streaming)

---

## 🧠 Engineering Focus

- Building reliable pipelines rather than one-off scripts
- Data correctness, validation, and reconciliation
- Idempotent and incremental processing
- Failure handling and recovery
- Distributed data processing
- Streaming reliability and event-time semantics
- Storage and query-performance trade-offs
- Maintainable, modular data systems

---

## 📈 Currently Developing

- Azure Databricks lakehouse engineering
- Delta Lake and incremental ingestion patterns
- Auto Loader and Lakeflow Jobs
- Spark Structured Streaming and Kafka
- Data warehouse and lakehouse design
- Testing and operationalising data pipelines

---

## 📫 Connect With Me

- **LinkedIn:** [linkedin.com/in/niraj-khapne](https://www.linkedin.com/in/niraj-khapne/)
- **Email:** [nirajrkhapne@gmail.com](mailto:nirajrkhapne@gmail.com)
- **GitHub:** [github.com/nirajkhapne](https://github.com/nirajkhapne)

---

> Good data systems don't just process data—they make correctness, failures, and recovery observable.
