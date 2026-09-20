from pyspark.sql.types import IntegerType, StringType, StructField, StructType

order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_date", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", IntegerType(), True),
])

payment_schema = StructType([
    StructField("payment_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("payment_date", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("amount", IntegerType(), True),
])
