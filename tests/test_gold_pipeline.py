from pyspark.sql import Row
from PIPELINE.gold_pipeline.functions import build_gold_hourly, build_gold_kpis


def test_build_gold_hourly_pivots_to_wide_row(spark):
    rows = [
        Row(hour_src="2026-01-28 10:15:00", variable="AQI", value_double=10.0, _ingest_ts="2026-01-28 12:00:00"),
        Row(hour_src="2026-01-28 10:30:00", variable="pm25_concentration", value_double=2.5, _ingest_ts="2026-01-28 12:00:00"),
        Row(hour_src="2026-01-28 10:45:00", variable="no2_concentration", value_double=7.0, _ingest_ts="2026-01-28 12:00:00"),
    ]
    # build_gold_hourly expects time_from_ts already typed as timestamp
    df = spark.createDataFrame(rows).withColumnRenamed("hour_src", "time_from_ts")

    out = build_gold_hourly(df).collect()
    assert len(out) == 1

    r = out[0]
    assert r["AQI"] == 10.0
    assert r["pm25_concentration"] == 2.5
    assert r["no2_concentration"] == 7.0
    # hour_ts should be hour-aligned (minute/second = 0)
    assert r["hour_ts"].minute == 0
    assert r["hour_ts"].second == 0
    # is_prediction: hour_ts (10:00) is not > ingest (12:00)
    assert r["is_prediction"] is False


def test_build_gold_kpis_missing_metrics_and_is_complete(spark):
    df = spark.createDataFrame([
        Row(
            hour_ts="2026-01-28 10:00:00",
            _ingest_ts="2026-01-28 12:00:00",
            AQI=10.0,
            pm25_concentration=2.5,
            pm10_concentration=None,   # missing
            no2_concentration=7.0,
            o3_concentration=None,     # missing
            so2_concentration=0.2
        )
    ])

    out = build_gold_kpis(df).collect()[0]
    missing = out["missing_metrics"]

    assert "pm10_concentration" in missing
    assert "o3_concentration" in missing
    assert out["is_complete"] is False
