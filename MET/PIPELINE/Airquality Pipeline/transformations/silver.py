from pyspark import pipelines as dp
from pyspark.sql.functions import col
from PIPELINE.silver.functions import transform_cleaned

# -------------------------
# 1) Cleaned staging view
# -------------------------
@dp.view(name="met_airquality_cleaned_vw")
@dp.expect_or_drop("time_from_present", "time_from IS NOT NULL")
@dp.expect_or_drop("variable_present", "variable IS NOT NULL")
def met_airquality_cleaned_vw():
    df = dp.read_stream("main_uc.bronze.met_airquality_bronze")
    return transform_cleaned(df)


# -------------------------
# 3) Upsert/deduplicate scd1
# -------------------------
dp.apply_changes(
    target="main_uc.silver.met_airquality_silver_scd1",
    source="met_airquality_cleaned_vw",
    keys=["station_eoi", "time_from", "time_to", "variable"],
    sequence_by=col("_ingest_ts"),
    stored_as_scd_type=1,
)

# -------------------------
# 4) Upsert/deduplicate scd2
# -------------------------
dp.apply_changes(
    target="main_uc.silver.met_airquality_silver_scd2",
    source="met_airquality_cleaned_vw",
    keys=["station_eoi", "time_from", "time_to", "variable"],
    sequence_by=col("_ingest_ts"),
    stored_as_scd_type=2,
)
