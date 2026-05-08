# Kafka + MongoDB Streaming Pipeline

## Overview
This project demonstrates a production-oriented streaming pipeline using Kafka, Avro, MongoDB, FastAPI, and Docker.

## Architecture
CSV → Kafka Producer → Kafka Topic → Consumer Group → MongoDB
                                            ↓
                                           DLQ

FastAPI is used to expose analytics APIs on top of MongoDB.

---

## Features

- Kafka Producer & Consumer
- Avro Serialization
- Schema Registry Integration
- MongoDB Sink
- Data Validation
- Dead Letter Queue (DLQ)
- FastAPI APIs
- Dockerized Infrastructure
- Idempotent MongoDB writes

---

## APIs

### Filter by Vehicle Number


## Skills Demonstrated
- Kafka Streaming
- MongoDB Integration
- Avro + Schema Registry
- FastAPI
- Docker
- Fault Tolerant Streaming
- Data Validation
- Distributed Consumer Design

### GPS Provider Aggregation
```bash
GET /gps-provider-count
```

## Run Steps

### 1. Start Infrastructure
```bash
docker-compose up -d
```

### 2. Run Producer
```bash
python producer/producer.py
```

### 3. Run Consumer
```bash
python consumer/consumer.py
```

### 4. Run API
```bash
uvicorn api.main:app --reload
```

## Designed and implemented a real-time streaming pipeline

