"""Silver cleansing, typing, deduplication and quarantine rules."""

from pyspark.sql import Window
from pyspark.sql import functions as F
from .spark_utils import merge_idempotent, merge_on_keys


def _deduplicate(df, key, order_columns):
    keys = key if isinstance(key, (list, tuple)) else [key]
    window = Window.partitionBy(*keys).orderBy(*[F.col(c).desc_nulls_last() for c in order_columns])
    return df.withColumn("_row_number", F.row_number().over(window)).filter("_row_number = 1").drop("_row_number")


def _publish(spark, accepted, rejected, config, name, keys):
    merge_on_keys(spark, accepted, config.table("silver", name), keys)
    if rejected is not None and rejected.limit(1).count() > 0:
        merge_idempotent(spark, rejected, config.table("silver", f"{name}_rejected"))


def transform_all(spark, config):
    # Customers: latest valid source version wins; deterministic tie-breaker is the payload hash.
    customers = spark.table(config.table("bronze", "customers_raw"))
    customer_rejects = customers.filter(F.col("_corrupt_record").isNotNull() | F.col("customer_id").isNull()).withColumn(
        "_rejection_reason", F.when(F.col("_corrupt_record").isNotNull(), "MALFORMED_JSON").otherwise("MISSING_CUSTOMER_ID"))
    customers_valid = customers.filter(F.col("_corrupt_record").isNull() & F.col("customer_id").isNotNull())
    customers_valid = (customers_valid
        .withColumn("date_of_birth", F.to_date("date_of_birth"))
        .withColumn("member_since", F.to_date("member_since"))
        .withColumn("created_timestamp", F.to_timestamp("created_timestamp"))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("customer_name", F.trim("customer_name")))
    customers_valid = _deduplicate(customers_valid, "customer_id", ["created_timestamp", "_record_hash"])
    _publish(spark, customers_valid, customer_rejects, config, "customers", ["customer_id"])

    # Orders with malformed JSON or missing keys are quarantined before item expansion.
    orders = spark.table(config.table("bronze", "orders_raw"))
    order_rejects = orders.filter(F.col("_corrupt_record").isNotNull() | F.col("order_id").isNull()).withColumn(
        "_rejection_reason", F.when(F.col("_corrupt_record").isNotNull(), "MALFORMED_JSON").otherwise("MISSING_ORDER_ID"))
    orders_valid = orders.filter(F.col("_corrupt_record").isNull() & F.col("order_id").isNotNull())
    orders_valid = (orders_valid.withColumn("order_date", F.to_date("order_date"))
        .withColumn("transaction_timestamp", F.to_timestamp("transaction_timestamp"))
        .withColumn("total_amount", F.col("total_amount").cast("decimal(12,2)"))
        .withColumn("order_status", F.initcap(F.trim("order_status")))
        .withColumn("payment_method", F.trim("payment_method")))
    orders_valid = _deduplicate(orders_valid, "order_id", ["transaction_timestamp", "_record_hash"])
    _publish(spark, orders_valid, order_rejects, config, "orders", ["order_id"])

    # Items are a separate grain. Keep the parent order ID with each item row.
    order_items = (
        orders_valid.select("order_id", "customer_id", "order_date", F.explode_outer("items").alias("item"))
        .select("order_id", "customer_id", "order_date", F.col("item.item_id").alias("item_id"),
                F.col("item.name").alias("item_name"), F.col("item.category").alias("item_category"),
                F.col("item.price").cast("decimal(12,2)").alias("item_price"),
                F.col("item.quantity").cast("int").alias("quantity"), F.col("item.details.brand").alias("brand"),
                F.col("item.details.color").alias("color"))
        .filter(F.col("item_id").isNotNull())
        .withColumn("_source_file", F.lit("derived:silver.orders"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in ["order_id", "item_id", "item_name", "item_price", "quantity", "brand", "color"]])), 256))
    )
    merge_on_keys(spark, order_items, config.table("silver", "order_items"), ["order_id", "item_id"])

    payments = spark.table(config.table("bronze", "payments_raw"))
    payment_rejects = payments.filter(F.col("payment_id").isNull() | F.col("order_id").isNull()).withColumn("_rejection_reason", F.lit("MISSING_PAYMENT_OR_ORDER_ID"))
    payments_valid = (payments.filter(F.col("payment_id").isNotNull() & F.col("order_id").isNotNull())
        .withColumn("payment_timestamp", F.to_timestamp("payment_timestamp"))
        .withColumn("payment_status", F.col("payment_status").cast("int"))
        .withColumn("payment_method", F.trim("payment_method")))
    payments_valid = _deduplicate(payments_valid, "payment_id", ["payment_timestamp", "_record_hash"])
    _publish(spark, payments_valid, payment_rejects, config, "payments", ["payment_id"])

    addresses = spark.table(config.table("bronze", "addresses_raw"))
    address_rejects = addresses.filter(F.col("customer_id").isNull() | F.col("address_type").isNull()).withColumn("_rejection_reason", F.lit("MISSING_ADDRESS_KEY_OR_TYPE"))
    addresses_valid = addresses.filter(F.col("customer_id").isNotNull() & F.col("address_type").isNotNull())
    addresses_valid = addresses_valid.withColumn("address_type", F.lower(F.trim("address_type")))
    addresses_valid = _deduplicate(addresses_valid, ["customer_id", "address_type", "address_line_1", "postcode"], ["_ingested_at", "_record_hash"])
    _publish(spark, addresses_valid, address_rejects, config, "addresses", ["customer_id", "address_type", "address_line_1", "postcode"])

    refunds = spark.table(config.table("bronze", "refunds_raw"))
    refund_rejects = refunds.filter(F.col("refund_id").isNull() | F.col("payment_id").isNull()).withColumn("_rejection_reason", F.lit("MISSING_REFUND_OR_PAYMENT_ID"))
    refunds_valid = (refunds.filter(F.col("refund_id").isNotNull() & F.col("payment_id").isNotNull())
        .withColumn("refund_timestamp", F.to_timestamp("refund_timestamp"))
        .withColumn("refund_amount", F.col("refund_amount").cast("decimal(12,2)"))
        .withColumn("refund_reason", F.trim("refund_reason")))
    refunds_valid = _deduplicate(refunds_valid, "refund_id", ["refund_timestamp", "_record_hash"])
    _publish(spark, refunds_valid, refund_rejects, config, "refunds", ["refund_id"])

    memberships = spark.table(config.table("bronze", "membership_files"))
    memberships_valid = memberships.filter(F.col("customer_id").isNotNull())
    memberships_rejects = memberships.filter(F.col("customer_id").isNull()).withColumn("_rejection_reason", F.lit("CUSTOMER_ID_NOT_FOUND_IN_FILENAME"))
    memberships_valid = _deduplicate(memberships_valid, ["customer_id", "membership_month"], ["file_modified_at", "_record_hash"])
    _publish(spark, memberships_valid, memberships_rejects, config, "memberships", ["customer_id", "membership_month"])

    return {
        "customers": customers_valid.count(),
        "orders": orders_valid.count(),
        "order_items": order_items.count(),
        "payments": payments_valid.count(),
        "addresses": addresses_valid.count(),
        "refunds": refunds_valid.count(),
        "memberships": memberships_valid.count(),
        "customers_rejected": customer_rejects.count(),
        "orders_rejected": order_rejects.count(),
    }
