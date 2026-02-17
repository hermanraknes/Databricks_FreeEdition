from pyspark import pipelines as dp
from MET.PIPELINE.gold_pipeline.functions import (
    build_gold_hourly,
    build_gold_daily_avg,
    build_gold_kpis,
    build_forecast_changes_hourly,
    build_geo_hourly,
    build_geo_latest_with_trend,
)

# Read catalog passed from DAB (via pipeline config/parameters)
catalog = spark.conf.get("pipeline_catalog")

silver_scd1 = f"{catalog}.silver.met_airquality_silver_scd1"
silver_scd2 = f"{catalog}.silver.met_airquality_silver_scd2"

gold_hourly = f"{catalog}.gold.met_airquality_gold_hourly"
gold_daily_avg = f"{catalog}.gold.met_airquality_gold_daily_avg"
gold_kpis = f"{catalog}.gold.met_airquality_gold_kpis"
gold_forecast_changes = f"{catalog}.gold.met_airquality_forecast_changes_hourly"
gold_geo_hourly = f"{catalog}.gold.met_airquality_geo_hourly"
gold_geo_latest_trend = f"{catalog}.gold.met_airquality_geo_latest_with_trend"


@dp.table(
    name=gold_hourly,
    comment="Hourly air quality metrics wide (one row per hour) - pivot-free for Lakeflow"
)
def met_airquality_gold_hourly():
    df = dp.read(silver_scd1)
    return build_gold_hourly(df)


@dp.table(
    name=gold_daily_avg,
    comment="Daily average air quality per variable (valid records: 1-hour interval)"
)
def met_airquality_gold_daily_avg():
    df = dp.read(silver_scd1)
    return build_gold_daily_avg(df)


@dp.table(
    name=gold_kpis,
    comment="Hourly KPIs + completeness flags"
)
def met_airquality_gold_kpis():
    df = dp.read(gold_hourly)
    return build_gold_kpis(df)


@dp.table(
    name=gold_forecast_changes,
    comment="Forecast changes per target hour+variable between runs (uses only rows where duration < 1 hour)"
)
def met_airquality_forecast_changes_hourly():
    df = dp.read(silver_scd2)
    return build_forecast_changes_hourly(df)


@dp.table(
    name=gold_geo_hourly,
    comment="Hourly air quality per station + variable (keeps lat/lon for maps)"
)
def met_airquality_geo_hourly():
    df = dp.read(silver_scd1)
    return build_geo_hourly(df)


@dp.table(
    name=gold_geo_latest_trend,
    comment="Latest per station×variable with trend label and size metrics (for maps)"
)
def met_airquality_geo_latest_with_trend():
    df = dp.read(gold_geo_hourly)
    return build_geo_latest_with_trend(df)
