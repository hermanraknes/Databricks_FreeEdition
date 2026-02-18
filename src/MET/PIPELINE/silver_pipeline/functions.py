from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, sha2, concat_ws


def transform_cleaned(df: DataFrame) -> DataFrame:
    """
    Cleans and types MET airquality rows:
    - parses time_from/time_to to timestamps
    - casts value to double and renames to value_double
    - creates record_hash from business key fields
    """
    return (
        df
        .withColumn("time_from_ts", to_timestamp(col("time_from")))
        .withColumn("time_to_ts", to_timestamp(col("time_to")))
        .withColumn("value_double", col("value").cast("double"))
        .drop("value")
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    col("time_from").cast("string"),
                    col("time_to").cast("string"),
                    col("variable").cast("string"),
                    col("station_eoi").cast("string"),
                ),
                256,
            ),
        )
    )
