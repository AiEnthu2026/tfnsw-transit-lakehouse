import sys, os
sys.path.append(os.path.abspath("../.."))
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from utils.silver_transforms import upsert_entity_to_silver

TEST_BRONZE = f"{CATALOG_NAME}.{SCHEMA_NAME}.test_bronze_gtfs_routes"
TEST_SILVER = f"{CATALOG_NAME}.{SCHEMA_NAME}.test_silver_gtfs_routes"

ROUTES_SCHEMA = StructType([
    StructField("route_id", StringType()),
    StructField("route_short_name", StringType()),
])



import pytest

@pytest.fixture
def clean_table():
    spark.sql(f"DROP TABLE IF EXISTS {TEST_BRONZE}")
    spark.sql(f"DROP TABLE IF EXISTS {TEST_SILVER}")
    yield (TEST_BRONZE, TEST_SILVER)
    spark.sql(f"DROP TABLE IF EXISTS {TEST_BRONZE}")
    spark.sql(f"DROP TABLE IF EXISTS {TEST_SILVER}")



def test_upsert_dedupes_by_recency_and_skips_unchanged(clean_table):
    # Two Bronze rows, same key, different ingestion_timestamp -- only the latest should survive
    bronze_df = spark.createDataFrame(
        [("sydneytrains", "T1", "T1-OLD", "2026-09-23T10:00:00"),
            ("sydneytrains", "T1", "T1-NEW", "2026-09-23T12:00:00")],
        "gtfs_mode STRING, route_id STRING, route_short_name STRING, ingestion_timestamp STRING",
    ).withColumn("ingestion_timestamp", F.col("ingestion_timestamp").cast("timestamp"))
    bronze_df.write.format("delta").saveAsTable(TEST_BRONZE)

    result = upsert_entity_to_silver(spark, TEST_BRONZE, TEST_SILVER, ROUTES_SCHEMA, ["route_id"], [])
    assert result == "created"

    silver_rows = spark.table(TEST_SILVER).collect()
    assert len(silver_rows) == 1, f"FAIL: expected 1 deduped row, got {len(silver_rows)}"
    assert silver_rows[0]["route_short_name"] == "T1-NEW", "FAIL: recency dedup picked the wrong row"

    # Re-run with identical latest data -- must be a no-op (row_hash unchanged)
    original_ts = spark.table(TEST_SILVER).collect()[0]["silver_updated_at"]
    import time; time.sleep(2)
    upsert_entity_to_silver(spark, TEST_BRONZE, TEST_SILVER, ROUTES_SCHEMA, ["route_id"], [])
    after_ts = spark.table(TEST_SILVER).collect()[0]["silver_updated_at"]
    assert after_ts == original_ts, "FAIL: unchanged row was rewritten"

    print("PASS: Bronze->Silver dedup + hash-diff MERGE works end-to-end for this entity")
