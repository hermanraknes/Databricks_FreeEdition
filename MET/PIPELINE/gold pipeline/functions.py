from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, expr, date_trunc, to_date,
    max as fmax, avg, count, when, lag,
    abs as fabs, lit, row_number, current_timestamp
)


def build_gold_hourly(df_scd1: DataFrame) -> DataFrame:
    """
    Hourly wide metrics (one row per hour) using pivot-free max(when()).
    Adds is_prediction flag: hour_ts > _ingest_ts
    """
    base = df_scd1.withColumn("hour_ts", date_trunc("hour", col("time_from_ts")))

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

    return wide.withColumn("is_prediction", col("hour_ts") > col("_ingest_ts"))


def build_gold_daily_avg(df_scd1: DataFrame) -> DataFrame:
    """
    Daily average per variable using only valid rows (duration < 1 hour).
    """
    valid = df_scd1.filter(
        (col("time_from_ts").isNotNull()) &
        (col("time_to_ts").isNotNull()) &
        expr("time_to_ts - time_from_ts < INTERVAL 1 HOUR")
    )

    return (
        valid
        .withColumn("date", to_date(col("time_from_ts")))
        .groupBy("date", "variable")
        .agg(
            avg(col("value_double")).alias("avg_value"),
            count(col("value_double")).alias("n_values"),
        )
    )


def build_gold_kpis(df_hourly: DataFrame) -> DataFrame:
    """
    Completeness KPI:
    - missing_metrics: array of missing required metric names
    - is_complete: True if none missing
    """
    required = ["AQI", "pm25_concentration", "pm10_concentration", "no2_concentration", "o3_concentration"]

    missing_expr = "filter(array({}), x -> x is not null)".format(
        ",".join([f"IF({c} IS NULL, '{c}', NULL)" for c in required])
    )

    return (
        df_hourly
        .withColumn("missing_metrics", expr(missing_expr))
        .withColumn("is_complete", expr("size(missing_metrics)=0"))
    )


def build_forecast_changes_hourly(df_scd2: DataFrame) -> DataFrame:
    """
    Forecast deltas between runs using SCD2 (__START_AT as run timestamp).
    Keeps only valid rows where duration < 1 hour.
    Produces delta, abs_delta, pct_change, abs_pct_change.
    """
    valid = df_scd2.filter(
        col("time_from_ts").isNotNull()
        & col("time_to_ts").isNotNull()
        & expr("time_to_ts - time_from_ts < INTERVAL 1 HOUR")
    )

    base = (
        valid
        .withColumn("forecast_run_ts", col("__START_AT"))
        .withColumn("forecast_run_date", to_date(col("__START_AT")))
        .withColumn("target_hour_ts", date_trunc("hour", col("time_from_ts")))
        .groupBy("forecast_run_ts", "forecast_run_date", "target_hour_ts", "variable")
        .agg(fmax("value_double").alias("value_double"))
        .filter(col("forecast_run_ts").isNotNull() & col("target_hour_ts").isNotNull())
    )

    w = Window.partitionBy("target_hour_ts", "variable").orderBy(col("forecast_run_ts"))

    return (
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


def build_geo_hourly(df_scd1: DataFrame) -> DataFrame:
    """
    Hourly air quality per station + variable (keeps lat/lon for maps).
    Valid rows require timestamps and duration <= 1 hour and lat/lon present.
    """
    valid = (
        df_scd1
        .withColumn("latitude_d", col("latitude").cast("double"))
        .withColumn("longitude_d", col("longitude").cast("double"))
        .filter(
            col("time_from_ts").isNotNull() &
            col("time_to_ts").isNotNull() &
            expr("time_to_ts - time_from_ts <= INTERVAL 1 HOUR")
        )
        .filter(col("latitude_d").isNotNull() & col("longitude_d").isNotNull())
        .withColumn("hour_ts", date_trunc("hour", col("time_from_ts")))
    )

    return (
        valid
        .groupBy(
            "hour_ts",
            "station_eoi",
            "station_name",
            col("latitude_d").alias("latitude"),
            col("longitude_d").alias("longitude"),
            "variable",
            "kommune",
        )
        .agg(fmax("value_double").alias("value"))
    )


def build_geo_latest_with_trend(df_geo_hourly: DataFrame, epsilon: float = 1e-3) -> DataFrame:
    """
    Latest row per station×variable with trend label and delta:
    - No trend if no previous value
    - Flat if abs(delta) <= epsilon
    - Increasing/Decreasing otherwise
    Only considers hours <= current hour.
    """
    now_hour = date_trunc("hour", current_timestamp())
    df_past = df_geo_hourly.filter(col("hour_ts") <= now_hour)

    w_asc = Window.partitionBy("station_eoi", "variable").orderBy("hour_ts")
    w_desc = Window.partitionBy("station_eoi", "variable").orderBy(col("hour_ts").desc())

    base = (
        df_past
        .withColumn("prev_value", lag("value").over(w_asc))
        .withColumn("delta", col("value") - col("prev_value"))
        .withColumn("abs_delta", fabs(col("delta")))
        .withColumn(
            "trend",
            when(col("prev_value").isNull(), lit("No trend"))
            .when(fabs(col("delta")) <= lit(epsilon), lit("Flat"))
            .when(col("delta") > 0, lit("Increasing"))
            .otherwise(lit("Decreasing"))
        )
        .withColumn("rn", row_number().over(w_desc))
    )

    return (
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
            "kommune",
        )
    )
