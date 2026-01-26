from pyspark import pipelines as dp
import dlt
from pyspark.sql.functions import col, to_timestamp, current_timestamp, sha2, concat_ws

# =========================
# SILVER
# =========================
@dp.table(
    name="main_uc.silver.met_airquality_silver",
    #schema="main_uc.silver",
    comment="Typed and cleaned MET air quality data"
)
@dp.expect_or_drop("time_from_present", "time_from IS NOT NULL")
@dp.expect_or_drop("variable_present", "variable IS NOT NULL")
def met_airquality_silver():
    df = dp.read_stream("met_airquality_bronze")

    cleaned = (
        df
        .withColumn("time_from_ts", to_timestamp(col("time_from")))
        .withColumn("time_to_ts", to_timestamp(col("time_to")))
        .withColumn("value_double", col("value").cast("double"))
        .drop("value")
    )

    cleaned = cleaned.withColumn(
        "record_hash",
        sha2(
            concat_ws(
                "||",
                col("time_from").cast("string"),
                col("time_to").cast("string"),
                col("variable").cast("string")
            ),
            256
        )
    )

    return cleaned