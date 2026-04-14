# Retail Data Platform

## 🚀 Overview

This project implements a **production-grade real-time data pipeline** using:

* **Apache Kafka (Confluent Cloud)** for event streaming
* **Avro + Schema Registry** for schema enforcement
* **Transactional Kafka Producer** (idempotent + exactly-once semantics)
* **Robust Kafka Consumer** with:

  * Manual offset management
  * Retry mechanism
  * Dead Letter Queue (DLQ)
  * Batch processing
* **MongoDB** as a low-latency serving layer (idempotent upserts)

---

## 🧠 Architecture

```
CSV Data → Kafka Producer → Kafka Topic → Consumer → MongoDB
                                     ↘ DLQ (failed records)
```

---

## ⚙️ Key Features

### ✅ Exactly-Once Producer

* Idempotent producer (`enable.idempotence=true`)
* Transactional messaging (`transactional.id`)
* Safe retries without duplication

### ✅ Schema Enforcement

* Avro schema with Confluent Schema Registry
* Strong typing and compatibility

### ✅ Fault-Tolerant Consumer

* Manual offset commit (no data loss)
* Retry strategy (3 attempts)
* DLQ for failed messages

### ✅ Idempotent Sink (MongoDB)

* Composite unique key: `(Invoice, StockCode)`
* Upsert-based writes to prevent duplicates

### ✅ Batch Processing

* Bulk writes using MongoDB `bulk_write`
* High throughput ingestion

---

## 📊 Data Model

Each event follows Avro schema:

* Invoice
* StockCode
* Description
* Quantity
* InvoiceDate
* Price
* CustomerID
* Country

👉 Unique event key:

```
Invoice + StockCode
```

---

## 🔧 Setup

### 1. Clone repo

```
git clone https://github.com/nirajkhapne/Confluent kafka - Retail data.git
cd Confluent kafka - Retail data
```

### 2. Install dependencies

```
pip install -r requirements.txt
```

### 3. Configure environment

Update `.env` using `configs/env.example`

---

## ▶️ Run Pipeline

### Start Producer

```
python producer/avro_transactional_producer.py
```

### Start Consumer

```
python consumer/kafka_to_mongodb_consumer.py
```

---

## 🧪 Failure Handling

| Scenario            | Handling           |
| ------------------- | ------------------ |
| Serialization error | Sent to DLQ        |
| MongoDB failure     | Retry 3 times      |
| Duplicate event     | Ignored via upsert |
| Consumer crash      | Reprocessed safely |

---

## 📈 What This Demonstrates

* Real-time data engineering design
* Streaming system reliability
* Data consistency guarantees
* Production-ready Kafka usage

---

## 🔮 Future Enhancements

* Spark Structured Streaming → Hive (analytics layer)
* Airflow orchestration
* Monitoring (Prometheus + Grafana)
* Schema evolution handling

---

## 💼 Resume Highlight

> Built a production-grade real-time data pipeline using Kafka, Avro Schema Registry, and MongoDB, implementing transactional producers, idempotent consumers, retry/DLQ mechanisms, and batch processing for high-throughput ingestion.

---

