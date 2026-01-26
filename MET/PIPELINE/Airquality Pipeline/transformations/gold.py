from pyspark import pipelines as dp
import dlt
from pyspark.sql.functions import col, to_timestamp, current_timestamp, sha2, concat_ws, max as fmax, expr, date_trunc, to_date, avg, count

@dp.table(
    name="main_uc.gold.met_airquality_gold_hourly",
    comment="Hourly air quality metrics pivoted wide (one row per hour)"
)
def met_airquality_gold_hourly():
    df = dp.read("main_uc.silver.met_airquality_silver_scd1")

    # Ensure hour grain (your data already looks hourly, but this makes it robust)
    base = df.withColumn("hour_ts", date_trunc("hour", col("time_from_ts")))

    # If duplicates ever slip through on same hour+variable, max() picks one deterministically
    wide = (
        base.groupBy("hour_ts")
        .pivot("variable", ["AQI","pm25_concentration","pm10_concentration",
                           "no2_concentration","o3_concentration","so2_concentration"])
        .agg(fmax("value_double"))
    )

    return wide


@dp.table(
    name="main_uc.gold.met_airquality_gold_daily_avg",
    comment="Daily average air quality per variable (valid records: 1-hour interval)"
)
def met_airquality_gold_daily_avg():
    df = dp.read("main_uc.silver.met_airquality_silver_scd1")

    # Valid if exactly 1 hour between from/to
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