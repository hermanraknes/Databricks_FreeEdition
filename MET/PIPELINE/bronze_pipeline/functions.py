from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp


def add_ingest_ts(df: DataFrame, ingest_col: str = "_ingest_ts") -> DataFrame:
    """
    Adds an ingestion timestamp column.
    Keeps this pure (df in -> df out), and lets DLT handle reading/writing/streaming.
    """
    return df.withColumn(ingest_col, current_timestamp())
