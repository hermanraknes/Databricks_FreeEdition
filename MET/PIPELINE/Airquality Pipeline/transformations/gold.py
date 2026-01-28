from pyspark import pipelines as dp
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_timestamp, current_timestamp, sha2, concat_ws, max as fmax, expr, date_trunc, to_date, avg, count, when, lag, abs as fabs, lit, row_number

@dp.table(
    name="main_uc.gold.met_airquality_gold_hourly",
    comment="Hourly air quality metrics wide (one row per hour) - pivot-free for Lakeflow"
)
def met_airquality_gold_hourly():
    df = dp.read("main_uc.silver.met_airquality_silver_scd1")

    base = df.withColumn("hour_ts", date_trunc("hour", col("time_from_ts")))

    wide = (
        base.groupBy("hour_ts")
        .agg(
            fmax(when(col("variable") == "AQI", col("value_double"))).alias("AQI"),
            fmax(when(col("variable") == "pm25_concentration", col("value_double"))).alias("pm25_concentration"),
            fmax(when(col("variable") == "pm10_concentration", col("value_double"))).alias("pm10_concentration"),
            fmax(when(col("variable") == "no2_concentration", col("value_double"))).alias("no2_concentration"),
            fmax(when(col("variable") == "o3_concentration", col("value_double"))).alias("o3_concentration"),
            fmax(when(col("variable") == "so2_concentration", col("value_double"))).alias("so2_concentration"),
            fmax(col("_ingest_ts")).alias("_ingest_ts"),
        )
    )

    out = wide.withColumn(
        "is_prediction",
        col("hour_ts") > col("_ingest_ts")
    )

    return out


@dp.table(
    name="main_uc.gold.met_airquality_gold_daily_avg",
    comment="Daily average air quality per variable (valid records: 1-hour interval)"
)
def met_airquality_gold_daily_avg():
    df = dp.read("main_uc.silver.met_airquality_silver_scd1")

    # Valid if less than 1 hour between from/to
    valid = df.filter(
        (col("time_from_ts").isNotNull()) &
        (col("time_to_ts").isNotNull()) &
        (expr("time_to_ts - time_from_ts < INTERVAL 1 HOUR"))
    )

    # Daily average per variable
    daily = (
        valid
        .withColumn("date", to_date(col("time_from_ts")))
        .groupBy("date", "variable")
        .agg(
            avg(col("value_double")).alias("avg_value"),
            count(col("value_double")).alias("n_values")   # hours with non-null values
        )
    )

    return daily

@dp.table(
    name="main_uc.gold.met_airquality_gold_kpis",
    comment="Hourly KPIs + completeness flags"
)
def met_airquality_gold_kpis():
    df = dp.read("main_uc.gold.met_airquality_gold_hourly")

    required = ["AQI","pm25_concentration","pm10_concentration","no2_concentration","o3_concentration"]
    missing_expr = "filter(array({}), x -> x is not null)".format(
        ",".join([f"IF({c} IS NULL, '{c}', NULL)" for c in required])
    )

    return (
        df
        .withColumn("missing_metrics", expr(missing_expr))
        .withColumn("is_complete", expr("size(missing_metrics)=0"))
    )


@dp.table(
    name="main_uc.gold.met_airquality_forecast_changes_hourly",
    comment="Forecast changes per target hour+variable between runs (uses only rows where duration < 1 hour)"
)
def met_airquality_forecast_changes_hourly():
    df = dp.read("main_uc.silver.met_airquality_silver_scd2")

    # 1) Keep only 'valid' rows (drop long time_to windows)
    valid = df.filter(
        col("time_from_ts").isNotNull()
        & col("time_to_ts").isNotNull()
        & expr("time_to_ts - time_from_ts < INTERVAL 1 HOUR")
    )

    # 2) Normalize to run + target hour + variable and collapse any duplicates
    base = (
        valid
        .withColumn("forecast_run_ts", col("__START_AT"))
        .withColumn("forecast_run_date", to_date(col("__START_AT")))
        .withColumn("target_hour_ts", date_trunc("hour", col("time_from_ts")))
        .groupBy("forecast_run_ts", "forecast_run_date", "target_hour_ts", "variable")
        .agg(fmax("value_double").alias("value_double"))
        .filter(col("forecast_run_ts").isNotNull() & col("target_hour_ts").isNotNull())
    )

    # 3) Compare successive runs for the same target hour + variable
    w = Window.partitionBy("target_hour_ts", "variable").orderBy(col("forecast_run_ts"))

    out = (
        base
        .withColumn("prev_forecast_run_ts", lag("forecast_run_ts").over(w))
        .withColumn("prev_value", lag("value_double").over(w))
        .withColumn("delta", col("value_double") - col("prev_value"))
        .withColumn("abs_delta", fabs(col("delta")))
        .withColumn(
            "pct_change",
            when(
                col("prev_value").isNotNull() & (col("prev_value") != 0),
                col("delta") / col("prev_value")
            )
        )
        .withColumn("abs_pct_change", fabs(col("pct_change")))
    )

    return out


@dp.table(
    name="main_uc.gold.met_airquality_geo_hourly",
    comment="Hourly air quality per station + variable (keeps lat/lon for maps)"
)
def met_airquality_geo_hourly():
    df = dp.read("main_uc.silver.met_airquality_silver_scd1")

    valid = (
        df
        .withColumn("latitude_d", col("latitude").cast("double"))
        .withColumn("longitude_d", col("longitude").cast("double"))
        .filter(
            col("time_from_ts").isNotNull() &
            col("time_to_ts").isNotNull() &
            expr("time_to_ts - time_from_ts <= INTERVAL 1 HOUR")  # <= is important
        )
        .filter(col("latitude_d").isNotNull() & col("longitude_d").isNotNull())
        .withColumn("hour_ts", date_trunc("hour", col("time_from_ts")))
    )

    out = (
        valid
        .groupBy(
            "hour_ts",
            "station_eoi",
            "station_name",
            col("latitude_d").alias("latitude"),
            col("longitude_d").alias("longitude"),
            "variable",
            "kommune"
        )
        .agg(fmax("value_double").alias("value"))
    )

    return out

@dp.table(
    name="main_uc.gold.met_airquality_geo_latest_with_trend",
    comment="Latest per station×variable with trend label and size metrics (for maps)"
)
def met_airquality_geo_latest_with_trend():
    df = dp.read("main_uc.gold.met_airquality_geo_hourly")

    now_hour = date_trunc("hour", current_timestamp())

    df_past = df.filter(col("hour_ts") <= now_hour)

    w_asc  = Window.partitionBy("station_eoi", "variable").orderBy("hour_ts")
    w_desc = Window.partitionBy("station_eoi", "variable").orderBy(col("hour_ts").desc())

    epsilon = 1e-3

    base = (
        df_past
        .withColumn("prev_value", lag("value").over(w_asc))
        .withColumn("delta", col("value") - col("prev_value"))
        .withColumn("abs_delta", fabs(col("delta")))
        .withColumn(
            "trend",
            when(col("prev_value").isNull(), lit("No trend"))
            .when(fabs(col("delta")) <= epsilon, lit("Flat"))
            .when(col("delta") > 0, lit("Increasing"))
            .otherwise(lit("Decreasing"))
        )
        .withColumn("rn", row_number().over(w_desc))
    )

    latest = (
        base.filter(col("rn") == 1)
            .select(
                "hour_ts",
                "station_eoi",
                "station_name",
                "latitude",
                "longitude",
                "variable",
                col("value").alias("value"),
                "delta",
                "abs_delta",
                "trend",
                "kommune"
            )
    )
    return latest