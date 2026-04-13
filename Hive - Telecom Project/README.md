# Enterprise Telecom Data Lake using Hive

## Architecture

Raw → Staging (ORC) → Curated (Partition + Bucket) → Star Schema → Data
Quality

## Features Implemented

-   External raw ingestion (TEXT)
-   ORC staging conversion with compression
-   Partitioning and bucketing strategy
-   File format benchmarking (TEXT vs ORC vs Parquet)
-   Incremental load simulation
-   Star schema (fact + dimensions)
-   Automated data quality governance
-   Threshold validation logic

## Benchmark Summary (\~900K rows)

-   ORC achieved \~26x storage reduction vs TEXT
-   Parquet performed best in selective aggregation test
-   Columnar formats outperformed TEXT in filtered workloads

## Governance

-   Row count validation
-   Null checks
-   Duplicate detection
-   Historical metric storage

## Scalability Considerations

-   Reducer parallelism tuning
-   ORC stripe optimization
-   Small file mitigation strategy
-   Skew detection
-   Idempotent partition overwrite strategy
