from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp, current_timestamp, sha2, concat_ws

SOURCE_ROOT = "/Volumes/main_uc/bronze/met_bergen_airquality_jsondumps"

# =========================
# BRONZE
# =========================
@dp.table(
    name="main_uc.bronze.met_airquality_bronze",
    #schema="main_uc.bronze",
    comment="Raw MET air quality JSONL ingested from UC Volume using Auto Loader"
)
def met_airquality_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(SOURCE_ROOT)
        .withColumn("_ingest_ts", current_timestamp())
    )