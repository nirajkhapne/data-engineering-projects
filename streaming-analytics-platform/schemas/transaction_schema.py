from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("amount", IntegerType(), False),
    StructField("timestamp", TimestampType(), False),
])

