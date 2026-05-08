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
Kafka Streaming
MongoDB Integration
Avro + Schema Registry
FastAPI
Docker
Fault Tolerant Streaming
Data Validation
Distributed Consumer Design

```bash
GET /vehicle/{vehicle_no}
