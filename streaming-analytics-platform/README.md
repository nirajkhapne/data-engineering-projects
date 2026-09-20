# Real-Time Streaming Analytics Platform

A modular streaming data engineering project built with **Kafka, Spark Structured Streaming, MongoDB, FastAPI, and Docker**.

The project demonstrates four core Structured Streaming patterns:

- Stateless event processing
- Unbounded/stateful aggregation
- Event-time windowed aggregation with watermarks
- Stateful order-payment stream joins with idempotent MongoDB writes

## Architecture

```text
                         Kafka Producers
                              |
            +-----------------+------------------+
            |                 |                  |
       User Events       Transactions      Orders + Payments
            |                 |                  |
            v                 v                  v
     +-------------+   +-------------+   +----------------------+
     | Stateless   |   | Stateful    |   | Stream-Stream Join   |
     | Filtering   |   | Aggregation |   | Order + Payment      |
     +-------------+   +-------------+   +----------+-----------+
                                                     |
                                                     v
                                                  MongoDB
                                                     |
                                                     v
                                                  FastAPI
                                                     |
                                                     v
                                              Analytics / Metrics
```

## Project Structure

```text
streaming-analytics-platform/
├── api/
│   ├── main.py
│   └── routes.py
├── configs/
│   └── settings.py
├── data/
│   ├── user_data.json
│   └── user_transactions.json
├── mongodb/
│   ├── indexes.py
│   ├── mongo_client.py
│   └── writer.py
├── monitoring/
│   └── metrics.py
├── producers/
│   ├── orders_producer.py
│   ├── payments_producer.py
│   ├── transaction_producer.py
│   └── user_producer.py
├── schemas/
│   ├── order_schema.py
│   ├── transaction_schema.py
│   └── user_schema.py
├── scripts/
│   └── validate_project.py
├── streaming/
│   ├── order_payment_join.py
│   ├── stateful_global_stream.py
│   ├── stateful_window_stream.py
│   └── stateless_stream.py
├── utils/
│   ├── kafka.py
│   ├── logging.py
│   └── spark.py
├── .env.example
├── docker-compose.yml
└── requirements.txt
```

## Requirements

- Python 3.10+
- Docker and Docker Compose
- Java 17 recommended for Spark 3.5.x

## Setup

```bash
git clone https://github.com/nirajkhapne/data-engineering-projects/tree/main/streaming-analytics-platform
cd streaming-analytics-platform
python -m venv venv
```

### Windows

```bash
venv\\Scripts\\activate
```

### Linux/macOS

```bash
source venv/bin/activate
```

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Create the local configuration:

```bash
cp .env.example .env
```

On Windows, copy `.env.example` to `.env` manually if `cp` is unavailable.

## Start Kafka and MongoDB

```bash
docker compose up -d
```

The stack exposes:

- Kafka: `localhost:9092`
- MongoDB: `localhost:27017`

Kafka topics are auto-created by the local broker when the producers/consumers first access them.

Create the MongoDB indexes once:

```bash
python -m mongodb.indexes
```

## Run the streaming jobs

Run each Spark job from the project root. The Spark session automatically loads the Kafka connector required by Spark Structured Streaming.

### Stateless stream

```bash
spark-submit streaming/stateless_stream.py
```

### Global aggregation

```bash
spark-submit streaming/stateful_global_stream.py
```

### Event-time window aggregation

```bash
spark-submit streaming/stateful_window_stream.py
```

### Order-payment stream join

```bash
spark-submit streaming/order_payment_join.py
```

The order-payment pipeline writes joined records to MongoDB using idempotent `upsert` operations keyed by `order_id`.

## Run producers

Run the producers from the project root in separate terminals.

```bash
python -m producers.user_producer
python -m producers.transaction_producer
python -m producers.orders_producer
python -m producers.payments_producer
```

For the order-payment join, start the order and payment producers independently. The Spark stream uses event-time constraints and watermarks so either stream can arrive asynchronously within the configured interval.

## API

Start the API after MongoDB is running:

```bash
uvicorn api.main:app --reload
```

Endpoints:

```text
GET /health
GET /orders/{order_id}
GET /metrics
```

Example:

```bash
curl http://localhost:8000/orders/order_1
```

## Data Engineering Patterns Demonstrated

### Modular Structured Streaming

Each streaming job follows the same flow:

```text
read_stream()
      |
parse_stream()
      |
transform / aggregate()
      |
write_stream()
      |
process()
```

This keeps ingestion, transformation, state management, and sink behavior separate and easier to test.

### Stateful processing

The global aggregation maintains a running total by `user_id` using Structured Streaming's state management and `complete` output mode.

### Event-time processing

The windowed pipeline uses:

- 3-minute tumbling windows
- 5-minute watermarking
- `update` output mode

This allows late events to update their event-time window while eventually allowing Spark to remove old state.

### Stream-stream join

The order-payment pipeline uses a native Structured Streaming stream-stream join with:

- `order_id` as the correlation key
- 10-minute watermarks
- a 10-minute event-time join interval

This avoids keeping an unbounded Python-side state store and allows either stream to arrive before the other.

### Idempotent serving layer

Joined records are written to MongoDB using `UpdateOne(..., upsert=True)`. A unique index on `order_id` prevents duplicate serving records when a micro-batch is retried.

## Validation

Run the dependency-free validation script before committing changes:

```bash
python scripts/validate_project.py
```

It checks Python syntax and JSON data files without requiring Spark or Kafka to be running.

## Configuration

All runtime configuration is centralized in `configs/settings.py` and can be overridden through `.env`:

```text
KAFKA_BOOTSTRAP
MONGO_URI
MONGO_DB
MONGO_COLLECTION
CHECKPOINT_DIR
LOG_LEVEL
USER_TOPIC
TRANSACTION_TOPIC
ORDER_TOPIC
PAYMENT_TOPIC
SPARK_KAFKA_PACKAGE
```

Do not commit `.env` or credentials to the repository.
