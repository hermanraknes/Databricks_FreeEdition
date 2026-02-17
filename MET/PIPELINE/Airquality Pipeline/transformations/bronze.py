from pyspark import pipelines as dp
from MET.PIPELINE.bronze_pipeline.functions import add_ingest_ts

catalog = spark.conf.get("pipeline_catalog")

SOURCE_ROOT = f"/Volumes/{catalog}/bronze/met_bergen_airquality_jsondumps"
BRONZE_TABLE = f"{catalog}.bronze.met_airquality_bronze"

# =========================
# BRONZE
# =========================
@dp.table(
    name=BRONZE_TABLE,
    comment="Raw MET air quality JSONL ingested from UC Volume using Auto Loader"
)
def met_airquality_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(SOURCE_ROOT)
    )
    return add_ingest_ts(df)