# Real-Time Streaming Analytics Platform

## Overview

This project demonstrates a production-oriented real-time streaming analytics platform built using:

- Apache Kafka
- Spark Structured Streaming
- MongoDB
- FastAPI
- Docker

The platform consolidates multiple streaming paradigms into a single coherent architecture:

- Stateless Stream Processing
- Stateful Global Aggregations
- Windowed Aggregations
- Stateful Stream Joins
- MongoDB Sink Integration
- REST APIs for analytics consumption

The objective of this project is to simulate how modern event-driven data engineering systems process, aggregate, enrich, and expose streaming data in real time.

---

# Architecture

```text
                    ┌────────────────────┐
                    │ Kafka Producers    │
                    │--------------------│
                    │ User Events        │
                    │ Transactions       │
                    │ Orders             │
                    │ Payments           │
                    └─────────┬──────────┘
                              │
                              ▼
                    ┌────────────────────┐
                    │ Apache Kafka       │
                    │--------------------│
                    │ user_topic         │
                    │ transactions_topic │
                    │ orders_topic       │
                    │ payments_topic     │
                    └─────────┬──────────┘
                              │
        ┌─────────────────────┼──────────────────────┐
        │                     │                      │
        ▼                     ▼                      ▼

┌────────────────┐  ┌────────────────────┐  ┌────────────────────┐
│ Stateless      │  │ Stateful Global    │  │ Stateful Stream    │
│ Processing     │  │ Aggregation        │  │ Join               │
│----------------│  │--------------------│  │--------------------│
│ User Filtering │  │ Running Totals     │  │ Order-Payment Join │
└────────┬───────┘  └─────────┬──────────┘  └─────────┬──────────┘
         │                    │                       │
         └────────────────────┼───────────────────────┘
                              │
                              ▼
                   ┌─────────────────────┐
                   │ Spark Structured    │
                   │ Streaming           │
                   └─────────┬───────────┘
                             │
                             ▼
                   ┌─────────────────────┐
                   │ MongoDB             │
                   │---------------------│
                   │ Enriched Orders     │
                   │ Aggregated Metrics  │
                   └─────────┬───────────┘
                             │
                             ▼
                   ┌─────────────────────┐
                   │ FastAPI             │
                   │ Analytics APIs      │
                   └─────────────────────┘
```

---

# Tech Stack

| Component | Technology |
|---|---|
| Streaming Broker | Apache Kafka |
| Stream Processing | Spark Structured Streaming |
| Database | MongoDB |
| API Layer | FastAPI |
| Containerization | Docker |
| Language | Python |
| Monitoring | Prometheus |

---

# Streaming Concepts Implemented

## 1. Stateless Stream Processing

Processes independent events without maintaining historical state.

### Example:
- Filter users where `age > 25`

### Implemented in:
```bash
streaming/stateless_stream.py
```

---

## 2. Stateful Global Aggregation

Maintains continuously updated aggregates across the entire stream.

### Example:
- Running total transaction amount per user

### Implemented in:
```bash
streaming/stateful_global_stream.py
```

---

## 3. Windowed Aggregation

Performs aggregations over event-time windows.

### Features:
- Event-time processing
- Watermarking
- Late event handling

### Example:
- Total transaction amount every 3 minutes

### Implemented in:
```bash
streaming/stateful_window_stream.py
```

---

## 4. Stateful Stream Join

Joins asynchronous event streams using a common key.

### Example:
- Joining orders and payments streams on `order_id`

### Implemented in:
```bash
streaming/order_payment_join.py
```

---

# Key Features

- Kafka-based event streaming
- Multiple streaming paradigms
- Stateful event processing
- Event-time watermarking
- Fault tolerance via checkpointing
- MongoDB integration
- FastAPI analytics APIs
- Modular project structure
- Dockerized infrastructure
- Config-driven architecture

---

# Project Structure

```bash
streaming-analytics-platform/
│
├── README.md
├── requirements.txt
├── docker-compose.yml
├── .env.example
├── .gitignore
│
├── configs/
│   └── settings.py
│
├── schemas/
│   ├── user_schema.py
│   ├── transaction_schema.py
│   ├── order_schema.py
│
├── producers/
│   ├── user_producer.py
│   ├── transaction_producer.py
│   ├── orders_producer.py
│   ├── payments_producer.py
│
├── streaming/
│   ├── stateless_stream.py
│   ├── stateful_global_stream.py
│   ├── stateful_window_stream.py
│   ├── order_payment_join.py
│
├── mongodb/
│   ├── mongo_client.py
│   ├── indexes.py
│
├── api/
│   ├── main.py
│   ├── routes.py
│
├── monitoring/
│   └── metrics.py
│
├── data/
│   ├── user_data.json
│   ├── user_transactions.json
│
└── checkpoints/
```

---

# Setup Instructions

## 1. Clone Repository

```bash
git clone https://github.com/your-username/streaming-analytics-platform.git
cd streaming-analytics-platform
```

---

## 2. Create Virtual Environment

```bash
python -m venv venv
```

### Windows

```bash
venv\\Scripts\\activate
```

### Linux / Mac

```bash
source venv/bin/activate
```

---

## 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 4. Configure Environment Variables

Create `.env` file using `.env.example`

```env
KAFKA_BOOTSTRAP=localhost:9092

MONGO_URI=mongodb://localhost:27017

MONGO_DB=streaming_db

MONGO_COLLECTION=orders_fact
```

---

# Running the Project

---

## Step 1: Start Infrastructure

```bash
docker-compose up -d
```

---

## Step 2: Create Kafka Topics

```bash
kafka-topics --create \
--topic user_topic \
--bootstrap-server localhost:9092
```

```bash
kafka-topics --create \
--topic transactions_topic \
--bootstrap-server localhost:9092
```

```bash
kafka-topics --create \
--topic orders_topic \
--bootstrap-server localhost:9092
```

```bash
kafka-topics --create \
--topic payments_topic \
--bootstrap-server localhost:9092
```

---

## Step 3: Start Kafka Producers

### User Producer

```bash
python producers/user_producer.py
```

### Transaction Producer

```bash
python producers/transaction_producer.py
```

### Orders Producer

```bash
python producers/orders_producer.py
```

### Payments Producer

```bash
python producers/payments_producer.py
```

---

## Step 4: Start Streaming Jobs

### Stateless Stream

```bash
spark-submit streaming/stateless_stream.py
```

### Stateful Global Aggregation

```bash
spark-submit streaming/stateful_global_stream.py
```

### Stateful Windowed Aggregation

```bash
spark-submit streaming/stateful_window_stream.py
```

### Stateful Stream Join

```bash
spark-submit streaming/order_payment_join.py
```

---

## Step 5: Create MongoDB Index

```bash
python mongodb/indexes.py
```

---

## Step 6: Start FastAPI Server

```bash
uvicorn api.main:app --reload
```

---

# API Examples

## Fetch Order By Order ID

### Endpoint

```bash
GET /orders/{order_id}
```

### Example

```bash
GET /orders/order_1
```

---

# Sample Streaming Output

## Stateless Stream Output

```text
+---+-------+---+
|id |name   |age|
+---+-------+---+
|1  |John   |30 |
|3  |Doe    |35 |
|4  |Alice  |40 |
|6  |Charlie|50 |
+---+-------+---+
```

---

## Stateful Global Aggregation Output

```text
+-------+-------------+
|user_id|total_amount |
+-------+-------------+
|user1  |300          |
|user2  |300          |
|user3  |400          |
+-------+-------------+
```

---

## Windowed Aggregation Output

```text
+------------------------------------------+--------+-------------+
|window                                    |user_id|total_amount |
+------------------------------------------+--------+-------------+
|{2025-07-30 10:00, 2025-07-30 10:03}     |user1  |300          |
+------------------------------------------+--------+-------------+
```

---

# MongoDB Output Example

```json
{
  "order_id": "order_1",
  "order_date": "2025-07-30 10:00:00",
  "created_at": "2025-07-30 10:00:00",
  "customer_id": "customer_5",
  "order_amount": 750,
  "payment_id": "4f2d3f2a",
  "payment_date": "2025-07-30 10:00:05",
  "payment_amount": 750
}
```

---

# Fault Tolerance & Reliability

This project includes several production-oriented reliability mechanisms:

- Spark checkpointing
- Event-time watermarking
- Stateful recovery
- Distributed Kafka ingestion
- Structured streaming fault tolerance

Checkpoint locations:

```bash
checkpoints/
```

---

# Skills Demonstrated

This project demonstrates practical implementation of:

- Apache Kafka
- Spark Structured Streaming
- Stateful Streaming
- Event-Time Processing
- Watermarking
- Stream Joins
- MongoDB Integration
- FastAPI Development
- Distributed Streaming Architectures
- Real-Time Data Processing

---

# Future Improvements

Possible production-grade enhancements:

- Avro + Schema Registry
- Airflow orchestration
- S3 / Parquet sink
- Delta Lake integration
- Prometheus + Grafana dashboards
- CDC ingestion using Debezium
- Kubernetes deployment
- CI/CD pipelines


- operational streaming concepts

It is intended as a strong portfolio project for data engineering transition roles involving Kafka and Spark-based streaming systems.
