from pyspark import pipelines as dp
from pyspark.sql.functions import col
from MET.PIPELINE.silver_pipeline.functions import transform_cleaned

# Read catalog passed from DAB (via your pipeline parameters/config)
catalog = spark.conf.get("catalog", "main_uc_dev")

bronze_tbl = f"{catalog}.bronze.met_airquality_bronze"
silver_scd1 = f"{catalog}.silver.met_airquality_silver_scd1"
silver_scd2 = f"{catalog}.silver.met_airquality_silver_scd2"

# -------------------------
# 1) Cleaned staging view
# -------------------------
@dp.view(name="met_airquality_cleaned_vw")
@dp.expect_or_drop("time_from_present", "time_from IS NOT NULL")
@dp.expect_or_drop("variable_present", "variable IS NOT NULL")
def met_airquality_cleaned_vw():
    df = dp.read_stream(bronze_tbl)
    return transform_cleaned(df)

# -------------------------
# 2) Declare the targets (required for apply_changes)
# -------------------------
dp.create_streaming_table(
    name=silver_scd1,
    comment="Typed, cleaned, and deduplicated MET air quality data (SCD1)"
)

dp.create_streaming_table(
    name=silver_scd2,
    comment="Typed, cleaned, and historized MET air quality data (SCD2)"
)

# -------------------------
# 3) Upsert/deduplicate scd1
# -------------------------
dp.apply_changes(
    target=silver_scd1,
    source="met_airquality_cleaned_vw",
    keys=["station_eoi", "time_from", "time_to", "variable"],
    sequence_by=col("_ingest_ts"),
    stored_as_scd_type=1,
)

# -------------------------
# 4) Upsert/deduplicate scd2
# -------------------------
dp.apply_changes(
    target=silver_scd2,
    source="met_airquality_cleaned_vw",
    keys=["station_eoi", "time_from", "time_to", "variable"],
    sequence_by=col("_ingest_ts"),
    stored_as_scd_type=2,
)
