"""Explicit Spark schemas for source contracts."""

from pyspark.sql.types import (
    ArrayType, DecimalType, IntegerType, LongType, StringType,
    StructField, StructType,
)

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", LongType(), True),
    StructField("customer_name", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("telephone", StringType(), True),
    StructField("email", StringType(), True),
    StructField("member_since", StringType(), True),
    StructField("created_timestamp", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

ORDER_ITEM_SCHEMA = StructType([
    StructField("item_id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DecimalType(12, 2), True),
    StructField("quantity", IntegerType(), True),
    StructField("details", StructType([
        StructField("brand", StringType(), True),
        StructField("color", StringType(), True),
    ]), True),
])

ORDER_SCHEMA = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("order_date", StringType(), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("total_amount", DecimalType(12, 2), True),
    StructField("payment_method", StringType(), True),
    StructField("items", ArrayType(ORDER_ITEM_SCHEMA), True),
    StructField("order_status", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

PAYMENT_SCHEMA = StructType([
    StructField("payment_id", LongType(), True),
    StructField("order_id", LongType(), True),
    StructField("payment_timestamp", StringType(), True),
    StructField("payment_status", IntegerType(), True),
    StructField("payment_method", StringType(), True),
])

ADDRESS_SCHEMA = StructType([
    StructField("customer_id", LongType(), True),
    StructField("address_type", StringType(), True),
    StructField("address_line_1", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postcode", StringType(), True),
])

REFUND_SCHEMA = StructType([
    StructField("refund_id", LongType(), True),
    StructField("payment_id", LongType(), True),
    StructField("refund_timestamp", StringType(), True),
    StructField("refund_amount", DecimalType(12, 2), True),
    StructField("refund_reason", StringType(), True),
])
