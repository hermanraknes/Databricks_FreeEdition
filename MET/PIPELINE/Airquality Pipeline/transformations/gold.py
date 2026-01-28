from pyspark import pipelines as dp
from PIPELINE.gold_pipeline.functions import (
    build_gold_hourly,
    build_gold_daily_avg,
    build_gold_kpis,
    build_forecast_changes_hourly,
    build_geo_hourly,
    build_geo_latest_with_trend,
)

@dp.table(
    name="main_uc.gold.met_airquality_gold_hourly",
    comment="Hourly air quality metrics wide (one row per hour) - pivot-free for Lakeflow"
)
def met_airquality_gold_hourly():
    df = dp.read("main_uc.silver.met_airquality_silver_scd1")
    return build_gold_hourly(df)


@dp.table(
    name="main_uc.gold.met_airquality_gold_daily_avg",
    comment="Daily average air quality per variable (valid records: 1-hour interval)"
)
def met_airquality_gold_daily_avg():
    df = dp.read("main_uc.silver.met_airquality_silver_scd1")
    return build_gold_daily_avg(df)


@dp.table(
    name="main_uc.gold.met_airquality_gold_kpis",
    comment="Hourly KPIs + completeness flags"
)
def met_airquality_gold_kpis():
    df = dp.read("main_uc.gold.met_airquality_gold_hourly")
    return build_gold_kpis(df)


@dp.table(
    name="main_uc.gold.met_airquality_forecast_changes_hourly",
    comment="Forecast changes per target hour+variable between runs (uses only rows where duration < 1 hour)"
)
def met_airquality_forecast_changes_hourly():
    df = dp.read("main_uc.silver.met_airquality_silver_scd2")
    return build_forecast_changes_hourly(df)


@dp.table(
    name="main_uc.gold.met_airquality_geo_hourly",
    comment="Hourly air quality per station + variable (keeps lat/lon for maps)"
)
def met_airquality_geo_hourly():
    df = dp.read("main_uc.silver.met_airquality_silver_scd1")
    return build_geo_hourly(df)


@dp.table(
    name="main_uc.gold.met_airquality_geo_latest_with_trend",
    comment="Latest per station×variable with trend label and size metrics (for maps)"
)
def met_airquality_geo_latest_with_trend():
    df = dp.read("main_uc.gold.met_airquality_geo_hourly")
    return build_geo_latest_with_trend(df)
