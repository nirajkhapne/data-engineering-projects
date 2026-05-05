# Real-Time Data Engineering Pipeline (Kafka + Spark + S3)

## Overview
This project implements a production-style real-time data pipeline using Kafka, Avro, Spark Structured Streaming, and a data lake (S3/ADLS).

It demonstrates how to build a scalable, fault-tolerant, and near exactly-once streaming system.

---

## 🏗 Architecture

MySQL → Kafka (Avro + Schema Registry) → Spark Structured Streaming → S3 (Parquet)

### Components:
- **Source**: MySQL (incremental ingestion)
- **Streaming Layer**: Apache Kafka (10 partitions)
- **Serialization**: Avro + Schema Registry
- **Processing Engine**: Spark Structured Streaming
- **Sink**: S3 Data Lake (Parquet format)
- **Orchestration**: Airflow
- **Monitoring**: Prometheus
- **Failure Handling**: Dead Letter Queue (DLQ)

---

## ⚙️ Key Features

### Data Ingestion
- Incremental extraction using timestamp checkpointing
- Transactional Kafka producer (idempotent + exactly-once within Kafka)

### Streaming & Processing
- Spark Structured Streaming for scalable processing
- Schema-based deserialization (Avro)
- Transformation layer:
  - category → lowercase
  - discount logic for Category A

### Storage
- Partitioned Parquet files in S3 (data lake design)
- Checkpointing for fault tolerance

### Reliability
- Retry mechanism in producer
- Dead Letter Queue (DLQ) for failed records
- Effectively-once processing guarantee (no duplicates)

### Orchestration & Monitoring
- Airflow DAG for scheduling
- Prometheus metrics for observability

---

## Data Flow

1. Producer reads incremental data from MySQL
2. Serializes data using Avro schema
3. Publishes messages to Kafka topic `product_updates`
4. Spark consumes Kafka stream
5. Applies transformations
6. Writes output to S3 in Parquet format
7. Failed records are sent to `product_dlq`

---

## Topics

| Topic | Description | Partitions |
|------|------------|-----------|
| product_updates | Main data stream | 10 |
| product_dlq | Failed messages | 3 |

---

## Setup & Run

### 1. Start Infrastructure
```bash
docker-compose up -d
