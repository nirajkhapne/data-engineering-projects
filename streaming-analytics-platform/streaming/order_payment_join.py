import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit

from pyspark.sql.streaming.state import GroupStateTimeout

from schemas.order_schema import order_schema, payment_schema

spark = SparkSession.builder \
    .appName("OrderPaymentJoin") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "org.mongodb.spark:mongo-spark-connector_2.12:10.5.0"
    ) \
    .getOrCreate()

orders = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders_topic") \
    .load()

payments = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "payments_topic") \
    .load()

orders_df = orders.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), order_schema).alias("data")) \
    .select("data.*") \
    .withColumn("type", lit("order"))

payments_df = payments.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), payment_schema).alias("data")) \
    .select("data.*") \
    .withColumn("type", lit("payment"))

combined = orders_df.unionByName(
    payments_df,
    allowMissingColumns=True
)

def process_stateful(key, pdfs, state):

    (order_id,) = key

    if state.exists:
        stored = state.get
    else:
        stored = None

    output = []

    for pdf in pdfs:

        for _, row in pdf.iterrows():

            if row["type"] == "order":

                state.update((
                    row["order_date"],
                    row["created_at"],
                    row["customer_id"],
                    row["amount"]
                ))

            elif row["type"] == "payment":

                if state.exists:

                    order_date, created_at, customer_id, order_amount = state.get

                    output.append({
                        "order_id": order_id,
                        "order_date": order_date,
                        "created_at": created_at,
                        "customer_id": customer_id,
                        "order_amount": order_amount,
                        "payment_id": row["payment_id"],
                        "payment_date": row["payment_date"],
                        "payment_amount": row["amount"]
                    })

                    state.remove()

    return iter([pd.DataFrame(output)])

stateful_query = combined.groupBy("order_id").applyInPandasWithState(
    func=process_stateful,
    outputStructType="""
        order_id STRING,
        order_date STRING,
        created_at STRING,
        customer_id STRING,
        order_amount INT,
        payment_id STRING,
        payment_date STRING,
        payment_amount INT
    """,
    stateStructType="""
        order_date STRING,
        created_at STRING,
        customer_id STRING,
        amount INT
    """,
    outputMode="append",
    timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
)

query = stateful_query.writeStream \
    .format("mongodb") \
    .option(
        "spark.mongodb.connection.uri",
        "mongodb://localhost:27017"
    ) \
    .option("spark.mongodb.database", "streaming_db") \
    .option("spark.mongodb.collection", "orders_fact") \
    .option("checkpointLocation", "checkpoints/join") \
    .start()

query.awaitTermination()
