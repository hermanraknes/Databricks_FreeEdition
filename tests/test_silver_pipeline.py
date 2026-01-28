from pyspark.sql import Row
from MET.PIPELINE.silver_pipeline.functions import transform_cleaned


def test_transform_cleaned_casts_and_hash(spark):
    df = spark.createDataFrame([
        Row(
            time_from="2026-01-28T10:00:00Z",
            time_to="2026-01-28T11:00:00Z",
            variable="AQI",
            station_eoi="NO123",
            value="42",
        )
    ])

    out = transform_cleaned(df).collect()[0]

    assert out["time_from_ts"] is not None
    assert out["time_to_ts"] is not None
    assert out["value_double"] == 42.0
    assert out["record_hash"] is not None
    assert len(out["record_hash"]) == 64  # sha256 hex


def test_transform_cleaned_value_bad_cast_becomes_null(spark):
    df = spark.createDataFrame([
        Row(
            time_from="2026-01-28T10:00:00Z",
            time_to="2026-01-28T11:00:00Z",
            variable="AQI",
            station_eoi="NO123",
            value="not_a_number",
        )
    ])

    out = transform_cleaned(df).collect()[0]
    assert out["value_double"] is None


def test_transform_cleaned_drops_value_column(spark):
    df = spark.createDataFrame([
        Row(
            time_from="2026-01-28T10:00:00Z",
            time_to="2026-01-28T11:00:00Z",
            variable="AQI",
            station_eoi="NO123",
            value="1",
        )
    ])

    out_df = transform_cleaned(df)
    assert "value" not in out_df.columns
    assert "value_double" in out_df.columns
