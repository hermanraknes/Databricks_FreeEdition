from pyspark import pipelines as dp
import dlt
from pyspark.sql.functions import col, to_timestamp, current_timestamp, sha2, concat_ws

# -------------------------
# 1) Cleaned staging view
# -------------------------
@dp.view(name="met_airquality_cleaned")
@dp.expect_or_drop("time_from_present", "time_from IS NOT NULL")
@dp.expect_or_drop("variable_present", "variable IS NOT NULL")
def met_airquality_cleaned():
    df = dp.read_stream("met_airquality_bronze")

    cleaned = (
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
                ),
                256,
            ),
        )
    )
    return cleaned


# -------------------------
# 2) Declare the target
# -------------------------
dp.create_streaming_table(
    name="main_uc.silver.met_airquality_silver",
    comment="Typed, cleaned, and deduplicated MET air quality data"
)

# -------------------------
# 3) Upsert/deduplicate
# -------------------------
dp.apply_changes(
    target="main_uc.silver.met_airquality_silver",
    source="met_airquality_cleaned",
    keys=["time_from", "time_to", "variable"],
    sequence_by=col("_ingest_ts"),
    stored_as_scd_type=1
)