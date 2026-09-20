# Incremental MySQL → Kafka (Avro) → Spark → Data Lake Pipeline

## Overview

This project demonstrates a modular batch-to-stream Data Engineering pipeline:

```text
MySQL
  ↓ incremental extraction
Python producer
  ↓ Avro + Schema Registry
Kafka
  ↓ Structured Streaming
Spark
  ↓ transformation
Parquet data lake
```

The project focuses on reliable incremental ingestion, schema management, Kafka delivery semantics, streaming transformations, checkpoint recovery, and reproducible local infrastructure.

> **Guarantee note:** Kafka producer transactions provide transactional/idempotent delivery within Kafka. The complete MySQL → Kafka → Spark → Parquet pipeline is not claimed as end-to-end exactly-once because the source checkpoint and downstream object-store commit are separate systems.

## Project structure

```text
configs/settings.py        Centralized environment-driven configuration
producer/db.py             Incremental MySQL extraction
producer/checkpoint.py     Atomic source checkpoint persistence
producer/producer.py       Avro serialization + transactional Kafka publishing
schemas/product.avsc       Avro data contract
spark/stream.py            Kafka → Avro → Spark streaming pipeline
spark/transform.py         Business transformations
monitoring/metrics.py      Prometheus counters
dags/pipeline_dag.py       Hourly source-ingestion orchestration
scripts/validate_project.py Static project validation
mysql/init/                Reproducible local MySQL source
```

## Incremental ingestion design

The producer does not use only `last_updated` as its checkpoint. It tracks:

```text
(last_updated, ID)
```

The query uses:

```sql
WHERE (last_updated > :last_updated)
   OR (last_updated = :last_updated AND ID > :last_id)
ORDER BY last_updated, ID
```

This prevents records from being skipped when several rows share the same timestamp.

The checkpoint is updated **only after the Kafka transaction commits successfully**. This means a failed Kafka transaction does not advance the source position. If the process crashes after Kafka commit but before the local checkpoint is persisted, the source batch can be replayed; Kafka idempotence does not deduplicate an application-level resend across separate transactions. The project therefore does **not** claim end-to-end exactly-once semantics.

## Kafka reliability

The producer uses:

- `enable.idempotence=true`
- `acks=all`
- `transactional.id`
- explicit `init_transactions()` / `begin_transaction()` / `commit_transaction()` / `abort_transaction()` lifecycle

This gives transactional publishing semantics within Kafka. Consumers that need Kafka EOS semantics must read committed records and participate in the corresponding transaction model.

## Avro and Schema Registry

`schemas/product.avsc` defines the contract for product events. The producer uses Confluent's Avro serializer with Schema Registry, while Spark decodes the Kafka value using the same schema definition.

## Spark processing

Spark Structured Streaming performs:

1. Kafka consumption
2. Avro deserialization
3. Business transformations
4. Parquet append writes
5. Checkpoint-based query recovery

Current business logic:

- normalize `category` to lowercase
- apply a 50% price adjustment to `category a`

## Local development

Copy `.env.example` to `.env` and adjust values if needed.

Start the infrastructure:

```bash
docker compose up -d
```

The local stack provides Kafka, Schema Registry, MySQL and MinIO. The Python producer connects through `localhost:9092`; containers communicate using the internal Kafka listener.

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Run static validation:

```bash
python scripts/validate_project.py
```

Run the producer:

```bash
python -m producer.producer
```

Run the Spark stream locally:

```bash
spark-submit \
  --packages org.apache.spark:spark-avro_2.12:3.5.6 \
  spark/stream.py
```

For an S3-compatible deployment, configure `OUTPUT_PATH` and the required Hadoop S3A credentials/connectors for the target environment.

## Data simulation

The repository includes:

- `mysql/init/01_create_product.sql` for a reproducible MySQL source
- `data/mock_product.csv` for lightweight transformation/checkpoint testing
- `scripts/simulate_pipeline.py` for a dependency-light end-to-end logic simulation

The sample source contains six records with repeated timestamps specifically to exercise the composite `(last_updated, ID)` checkpoint design.

## Orchestration

The Airflow DAG schedules the **finite incremental producer task**. Spark Structured Streaming is intentionally treated as a long-running service rather than an hourly Airflow task.

## Validation scope

The repository includes dependency-light static checks for:

- required project files
- Python syntax
- Avro JSON/schema structure
- accidental `.env` inclusion

Unit tests cover checkpoint persistence and source-row normalization.

A full runtime test requires Kafka, Schema Registry, MySQL and Spark to be running. The Docker Compose file provides the local infrastructure required for that integration test.
