from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lower, when


def transform_df(df: DataFrame) -> DataFrame:
    """Apply business transformations to the decoded product stream."""
    return (
        df.withColumn("category", lower(col("category")))
        .withColumn(
            "price",
            when(col("category") == "category a", col("price") * 0.5)
            .otherwise(col("price")),
        )
    )
