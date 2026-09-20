from pyspark.sql.types import IntegerType, StringType, StructField, StructType

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
])
