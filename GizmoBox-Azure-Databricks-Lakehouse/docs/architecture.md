# Architecture and design decisions

## Scope

The project demonstrates a batch-first lakehouse with a separate Auto Loader path for newly arriving customer JSON. Both paths write into the same Bronze customer table and use shared Silver/Gold transformations.

## Data flow

1. **Landing:** immutable historical source files live under `backfill/gizmobox`; new customer files arrive under `incoming/customers`.
2. **Bronze batch:** explicit schemas read JSON Lines, headerless CSV, TSV, refunds CSV and binary membership files. Metadata includes source path, ingestion timestamp and record hash. Malformed JSON is preserved using Spark's corrupt-record column.
3. **Auto Loader:** watches only `*.json` under the incoming customer directory. Schema and file discovery state live in a durable ADLS checkpoint location. `availableNow` makes the job task finite.
4. **Silver:** applies types and standardisation, keeps a deterministic current customer version, validates business keys and writes rejected records to separate tables.
5. **Gold:** aggregates payments/refunds to order grain before joining to orders, then publishes facts, a customer dimension and a monthly summary.
6. **Quality gate:** blocking technical/business checks fail the job; source referential mismatches are recorded as warnings because the supplied sample intentionally contains a payment referencing a malformed order.
7. **Audit:** each stage emits append-only operational records.

## Idempotency boundaries

- Bronze raw records: `(source_file, record_hash)`.
- Silver current-state entities: declared natural keys; upsert updates the current representation.
- Gold facts/dimensions: business keys (`order_id`, `payment_id`, `refund_id`, `customer_id`).
- Monthly summary: `order_month`.
- Auto Loader file-discovery state: separate schema/checkpoint paths under ADLS.

The project assumes historical source files are immutable. If a source file is overwritten in place, the new payload hash is inserted but old rows from the prior file version are not automatically removed. If mutable files are a real source contract, add explicit source versioning or file-level replacement semantics.

## Governance

The selected catalog must be governed by Unity Catalog. ADLS access should use a managed identity/storage credential and external location approved for the workspace. Do not embed account keys in Spark configs or source files. Direct customer contact fields remain in Silver; the Gold customer dimension omits email and telephone.

## Deliberate non-goals

- No SCD Type 2 until source history semantics are proven.
- No OCR for membership images; only filename and folder metadata are used.
- No live Azure SQL dependency in the default run. A corrected SQL seed script is supplied, while the repeatable project input is a CSV extract.
- No performance claim without a measured baseline and repeatable comparison.
