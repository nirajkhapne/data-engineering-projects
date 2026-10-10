"""Business and technical quality checks for the curated lakehouse."""

from pyspark.sql import functions as F


def run_quality_checks(spark, config):
    checks = []

    def add(name, layer, df, passed, observed, detail, blocking=True):
        status = "PASS" if passed else ("FAIL" if blocking else "WARN")
        checks.append({"check_name": name, "layer": layer, "status": status,
                       "observed_value": str(observed), "details": detail})

    customers = spark.table(config.table("silver", "customers"))
    orders = spark.table(config.table("silver", "orders"))
    payments = spark.table(config.table("silver", "payments"))
    refunds = spark.table(config.table("silver", "refunds"))
    order_items = spark.table(config.table("silver", "order_items"))
    fact_orders = spark.table(config.table("gold", "fact_orders"))

    for name, df, key in [
        ("silver_customers_unique_key", customers, "customer_id"),
        ("silver_orders_unique_key", orders, "order_id"),
        ("silver_payments_unique_key", payments, "payment_id"),
        ("silver_refunds_unique_key", refunds, "refund_id"),
    ]:
        duplicate_keys = df.groupBy(key).count().filter(F.col("count") > 1).count()
        add(name, "silver", df, duplicate_keys == 0, duplicate_keys, f"Duplicate {key} groups must be zero.")

    negative_orders = orders.filter(F.col("total_amount") < 0).count()
    add("orders_non_negative_amount", "silver", orders, negative_orders == 0, negative_orders, "Negative order amounts require investigation.")
    invalid_items = order_items.filter(F.col("quantity") <= 0).count()
    add("order_items_positive_quantity", "silver", order_items, invalid_items == 0, invalid_items, "Item quantity must be greater than zero.")
    invalid_refunds = refunds.filter(F.col("refund_amount") < 0).count()
    add("refunds_non_negative_amount", "silver", refunds, invalid_refunds == 0, invalid_refunds, "Negative refund amounts require investigation.")

    orphan_payments = payments.join(orders.select("order_id"), "order_id", "left_anti").count()
    add("payments_reference_known_order", "silver", payments, orphan_payments == 0, orphan_payments, "Payment order IDs not found in Silver orders; investigate source coverage before changing records.", blocking=False)
    orphan_refunds = refunds.join(payments.select("payment_id"), "payment_id", "left_anti").count()
    add("refunds_reference_known_payment", "silver", refunds, orphan_refunds == 0, orphan_refunds, "Refund payment IDs not found in Silver payments.", blocking=False)

    order_count = orders.count()
    fact_count = fact_orders.count()
    add("gold_order_fact_row_count", "gold", fact_orders, order_count == fact_count, f"silver={order_count};gold={fact_count}", "Fact order grain should match the unique Silver order grain.")

    result_df = spark.createDataFrame(checks).withColumn("run_id", F.expr("uuid()")).withColumn("checked_at", F.current_timestamp())
    result_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(config.table("ops", "quality_results"))
    failed = [check for check in checks if check["status"] == "FAIL"]
    return checks, failed
