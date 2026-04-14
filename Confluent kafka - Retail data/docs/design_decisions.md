# Design Decisions

## Why Kafka?

* High throughput
* Decoupled architecture
* Fault tolerance

## Why Avro + Schema Registry?

* Schema enforcement
* Backward compatibility
* Smaller payload size

## Why MongoDB?

* Fast ingestion
* Flexible schema
* Good for serving layer

## Why Upsert?

* Handles duplicates from reprocessing
* Ensures idempotency

## Why Manual Offset Commit?

* Prevents data loss
* Ensures processing correctness

## Why DLQ?

* Isolates bad records
* Prevents pipeline failure

## Why Batch Processing?

* Improves performance
* Reduces network overhead
