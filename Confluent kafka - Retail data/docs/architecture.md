# Architecture Design

## System Components

### 1. Producer

* Reads CSV data
* Serializes using Avro
* Sends events to Kafka
* Uses transactions for exactly-once delivery

### 2. Kafka

* Topic: `retail_data_dev`
* Partitioned by `CustomerID`
* Guarantees ordering per customer

### 3. Consumer

* Reads events using Avro deserialization
* Processes in batches
* Applies retry + DLQ strategy
* Commits offsets only after success

### 4. MongoDB

* Serves as operational datastore
* Uses composite key `(Invoice, StockCode)`
* Supports idempotent upserts

---

## Data Flow

1. Producer pushes events to Kafka
2. Kafka buffers and distributes
3. Consumer reads and processes
4. Data written to MongoDB
5. Failures routed to DLQ

---

## Guarantees

| Layer    | Guarantee                  |
| -------- | -------------------------- |
| Producer | Exactly-once (Kafka-level) |
| Consumer | At-least-once              |
| MongoDB  | Idempotent writes          |

---

## Tradeoffs

* Chose MongoDB for fast writes over strict ACID
* Used batch processing for throughput
* Accepted eventual consistency for scalability
