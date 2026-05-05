from pyspark.sql.functions import col, lower, when

def transform_df(df):
    return df.withColumn("category", lower(col("category"))) \
        .withColumn(
            "price",
            when(col("category") == "category a", col("price") * 0.5)
            .otherwise(col("price"))
        )
