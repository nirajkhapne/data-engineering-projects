from pyspark.sql.types import *

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", IntegerType(), True)
])

payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payment_date", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("amount", IntegerType(), True)
])
