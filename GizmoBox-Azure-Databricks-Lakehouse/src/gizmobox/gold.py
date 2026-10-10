"""Gold analytical models with explicit grains and safe join cardinality."""

from pyspark.sql import functions as F
from .spark_utils import merge_on_keys


def build_gold(spark, config):
    customers = spark.table(config.table("silver", "customers"))
    orders = spark.table(config.table("silver", "orders"))
    payments = spark.table(config.table("silver", "payments"))
    refunds = spark.table(config.table("silver", "refunds"))
    addresses = spark.table(config.table("silver", "addresses"))
    memberships = spark.table(config.table("silver", "memberships"))

    # Aggregate child entities before joining them to order grain to prevent amount multiplication.
    payment_by_order = payments.groupBy("order_id").agg(
        F.countDistinct("payment_id").alias("payment_count"),
        F.countDistinct("payment_status").alias("distinct_payment_status_count"),
        F.collect_set("payment_status").alias("payment_status_codes"),
    )
    refund_by_order = (refunds.join(payments.select("payment_id", "order_id"), "payment_id", "left")
        .groupBy("order_id").agg(F.sum("refund_amount").cast("decimal(14,2)").alias("refund_amount"),
                                  F.countDistinct("refund_id").alias("refund_count")))

    fact_orders = (orders.join(payment_by_order, "order_id", "left")
        .join(refund_by_order, "order_id", "left")
        .withColumn("payment_count", F.coalesce("payment_count", F.lit(0)))
        .withColumn("refund_count", F.coalesce("refund_count", F.lit(0)))
        .withColumn("refund_amount", F.coalesce("refund_amount", F.lit(0).cast("decimal(14,2)")))
        .withColumn("net_order_amount", (F.when(F.col("order_status") == "Completed", F.col("total_amount") - F.col("refund_amount")).otherwise(F.lit(0).cast("decimal(14,2)"))).cast("decimal(14,2)"))
        .withColumn("_source_file", F.lit("derived:silver.orders+payments+refunds"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_hash", F.sha2(F.to_json(F.struct("order_id", "customer_id", "order_status", "total_amount", "refund_amount", "payment_count")), 256)))
    merge_on_keys(spark, fact_orders, config.table("gold", "fact_orders"), ["order_id"])

    fact_payments = payments.join(orders.select("order_id", "customer_id", "order_date", "order_status"), "order_id", "left")
    fact_payments = (fact_payments.withColumn("_source_file", F.lit("derived:silver.payments+orders"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_hash", F.sha2(F.to_json(F.struct("payment_id", "order_id", "payment_status", "payment_timestamp")), 256)))
    merge_on_keys(spark, fact_payments, config.table("gold", "fact_payments"), ["payment_id"])

    fact_refunds = refunds.join(payments.select("payment_id", "order_id"), "payment_id", "left").join(
        orders.select("order_id", "customer_id", "order_date"), "order_id", "left")
    fact_refunds = (fact_refunds.withColumn("_source_file", F.lit("derived:silver.refunds+payments+orders"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_hash", F.sha2(F.to_json(F.struct("refund_id", "payment_id", "refund_amount", "refund_timestamp")), 256)))
    merge_on_keys(spark, fact_refunds, config.table("gold", "fact_refunds"), ["refund_id"])

    address_counts = addresses.groupBy("customer_id").agg(F.count(F.lit(1)).alias("address_count"))
    membership_counts = memberships.groupBy("customer_id").agg(F.countDistinct("membership_month").alias("membership_months_seen"))
    # Gold intentionally excludes direct contact fields; they remain in governed Silver.
    dim_customer = (customers.select("customer_id", "member_since", "created_timestamp")
        .join(address_counts, "customer_id", "left")
        .join(membership_counts, "customer_id", "left")
        .withColumn("address_count", F.coalesce("address_count", F.lit(0)))
        .withColumn("membership_months_seen", F.coalesce("membership_months_seen", F.lit(0)))
        .withColumn("has_membership_file", F.col("membership_months_seen") > 0)
        .withColumn("_source_file", F.lit("derived:silver.customers+addresses+memberships"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_hash", F.sha2(F.to_json(F.struct("customer_id", "member_since", "created_timestamp", "address_count", "membership_months_seen")), 256)))
    merge_on_keys(spark, dim_customer, config.table("gold", "dim_customer"), ["customer_id"])

    monthly = (fact_orders.groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
        .agg(F.countDistinct("order_id").alias("order_count"),
             F.countDistinct(F.when(F.col("order_status") == "Completed", F.col("order_id"))).alias("completed_order_count"),
             F.sum(F.when(F.col("order_status") == "Completed", F.col("total_amount")).otherwise(F.lit(0))).cast("decimal(16,2)").alias("gross_completed_order_amount"),
             F.sum("refund_amount").cast("decimal(16,2)").alias("refund_amount"),
             F.sum("net_order_amount").cast("decimal(16,2)").alias("net_order_amount") )
        .withColumn("_source_file", F.lit("derived:gold.fact_orders"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_record_hash", F.sha2(F.to_json(F.struct("order_month", "order_count", "completed_order_count", "gross_completed_order_amount", "refund_amount", "net_order_amount")), 256)))
    merge_on_keys(spark, monthly, config.table("gold", "monthly_order_summary"), ["order_month"])

    return {
        "fact_orders": fact_orders.count(),
        "fact_payments": fact_payments.count(),
        "fact_refunds": fact_refunds.count(),
        "dim_customer": dim_customer.count(),
        "monthly_order_summary": monthly.count(),
    }
