from pyspark.sql.types import *

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])
