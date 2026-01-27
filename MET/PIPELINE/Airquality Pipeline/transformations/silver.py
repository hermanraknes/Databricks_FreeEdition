from pyspark import pipelines as dp
import dlt
from pyspark.sql.functions import col, to_timestamp, current_timestamp, sha2, concat_ws

# -------------------------
# 1) Cleaned staging view
# -------------------------
@dp.view(name="met_airquality_cleaned_vw")
@dp.expect_or_drop("time_from_present", "time_from IS NOT NULL")
@dp.expect_or_drop("variable_present", "variable IS NOT NULL")
def met_airquality_cleaned_vw():
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
                    col("station_eoi").cast("string"),
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
    name="main_uc.silver.met_airquality_silver_scd1",
    comment="Typed, cleaned, and deduplicated MET air quality data scd1"
)

dp.create_streaming_table(
    name="main_uc.silver.met_airquality_silver_scd2",
    comment="Typed, cleaned, and deduplicated MET air quality data scd2"
)

# -------------------------
# 3) Upsert/deduplicate scd1
# -------------------------
dp.apply_changes(
    target="main_uc.silver.met_airquality_silver_scd1",
    source="met_airquality_cleaned_vw",
    keys=["station_eoi", "time_from", "time_to", "variable"],
    sequence_by=col("_ingest_ts"),
    stored_as_scd_type=1
)

# -------------------------
# 4) Upsert/deduplicate scd2
# -------------------------
dp.apply_changes(
    target="main_uc.silver.met_airquality_silver_scd2",
    source="met_airquality_cleaned_vw",
    keys=["station_eoi", "time_from", "time_to", "variable"],
    sequence_by=col("_ingest_ts"),
    stored_as_scd_type=2
)