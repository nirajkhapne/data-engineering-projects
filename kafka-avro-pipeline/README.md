# Kafka Avro Streaming Pipeline

## Overview
This project implements a real-time data pipeline using Kafka, Avro, MySQL, and a consumer group architecture.

## Architecture
MySQL → Kafka (Avro + Schema Registry) → Consumer Group → JSON Sink

## Key Features
- Incremental ingestion using timestamp checkpointing
- Avro serialization with schema registry
- Kafka topic with 10 partitions
- Consumer group parallelism
- Retry mechanism in producer
- Dead Letter Queue (DLQ) for failure handling
- Data transformation layer

## Data Flow
1. Producer fetches incremental data from MySQL
2. Serializes using Avro
3. Publishes to Kafka topic `product_updates`
4. Consumers read messages in parallel
5. Apply transformation logic:
   - category → lowercase
   - discount for category A
6. Write output to JSON files
7. Failures go to `product_dlq`

## Topics
- product_updates (10 partitions)
- product_dlq (DLQ)

## Run Instructions

### Start infra
docker-compose up -d

### Create topics
kafka-topics --create --topic product_updates --partitions 10 --bootstrap-server localhost:9092

### Run producer
python producer/producer.py

### Run consumers
python consumer/consumer.py 1
python consumer/consumer.py 2

## Output
Stored in:
data/output/

Each consumer writes to a separate file.

## Improvements Possible
- Add Airflow orchestration
- Add monitoring (Prometheus)
- Add cloud deployment (AWS/GCP)
- Exactly-once processing
