# Kafka Real-Time Pipeline (Production Style)

## Architecture
MySQL → Kafka (Avro) → Consumer Group → JSON Output

## Features
- Incremental ingestion (timestamp-based)
- Avro serialization
- Kafka partitioning (key = product ID)
- Consumer group scaling
- Transformation layer
- Checkpointing
- Dockerized infra

## Run Steps

### 1. Setup
```bash
pip install -r requirements.txt
