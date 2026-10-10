# Data dictionary and grain

## Source contracts

| Source | Format | Key candidates | Notes |
|---|---|---|---|
| Customers | JSON Lines | `customer_id` | Monthly files may contain multiple versions. Current model selects latest `created_timestamp`, then `_record_hash` as deterministic tie-breaker. |
| Orders | JSON Lines with nested array/object | `order_id` | `items` is an array; `details` is nested. Invalid JSON is retained in `_corrupt_record`. |
| Payments | Headerless CSV | `payment_id` | Column order: payment_id, order_id, payment_timestamp, payment_status, payment_method. `payment_status` remains a numeric code. |
| Addresses | Headered TSV | customer_id + address_type + address_line_1 + postcode | Multiple address types per customer are allowed. |
| Refunds | Headered CSV extract | `refund_id` | Linked to payment by `payment_id`; the optional SQL DDL is in `sql/azure_sql_refunds.sql`. |
| Membership files | PNG | customer ID parsed from filename; month from parent folder | Image content is not interpreted. |

## Bronze metadata

- `_source_file`: source path or an explicit derived-source label.
- `_ingested_at`: ingestion timestamp.
- `_record_hash`: SHA-256 hash of the record payload or derived key fields.
- `_corrupt_record`: raw malformed JSON where Spark can capture it.
- `_rescued_data`: Auto Loader rescued fields where available.

## Silver tables

| Table | Grain | Key | Important transformations |
|---|---|---|---|
| `customers` | Current customer version | customer_id | Parse dates/timestamp, normalise email/name, choose latest record. |
| `orders` | One order | order_id | Parse date/timestamp/amount and standardise status. |
| `order_items` | One item within an order | order_id + item_id | Explode the nested item array; retain parent customer/order/date. |
| `payments` | One payment | payment_id | Parse timestamp and retain numeric status code. |
| `addresses` | One distinct customer address/type | customer_id + address_type + address_line_1 + postcode | Standardise address type. |
| `refunds` | One refund | refund_id | Parse timestamp and decimal amount. |
| `memberships` | One customer/month file observation | customer_id + membership_month | File metadata only; no inferred membership tier. |

Rejected rows are written to entity-specific tables such as `orders_rejected`, with `_rejection_reason` and source lineage.

## Gold tables

| Table | Grain | Definition |
|---|---|---|
| `dim_customer` | One customer | Customer ID, membership date, creation timestamp, address count and membership-file coverage; excludes direct contact fields. |
| `fact_orders` | One order | Order attributes, payment count, refund totals and net completed-order amount. |
| `fact_payments` | One payment | Payment status/method plus order/customer/date context. No payment amount is available in the source. |
| `fact_refunds` | One refund | Refund attributes plus linked payment/order/customer context where present. |
| `monthly_order_summary` | One month | Order count, completed order count, gross completed-order amount, refund amount and net completed-order amount. |

## Metrics caveat

These are portfolio definitions, not approved financial accounting definitions. Confirm cancellation, payment status, partial-refund and revenue recognition rules with a business owner before using these metrics for financial reporting.
