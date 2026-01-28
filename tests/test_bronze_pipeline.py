from MET.PIPELINE.bronze_pipeline.functions import add_ingest_ts

def test_add_ingest_ts(spark):
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    out = add_ingest_ts(df)

    assert "_ingest_ts" in out.columns
    assert out.filter("_ingest_ts IS NULL").count() == 0