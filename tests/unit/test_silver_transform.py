from pyspark.sql import functions as F
from delta.tables import DeltaTable
import sys, os
sys.path.append(os.path.abspath("../.."))

from utils.silver_transforms import upsert_current_position

TEST_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.test_silver_vehicle_positions_current"


import pytest

@pytest.fixture
def clean_table():
    spark.sql(f"DROP TABLE IF EXISTS {TEST_TABLE}")
    yield TEST_TABLE
    spark.sql(f"DROP TABLE IF EXISTS {TEST_TABLE}")


def test_stale_position_does_not_overwrite_newer_row(clean_table):
    seed_df = spark.createDataFrame(
        [("sydneytrains", "V123", "T1", 10.0, 20.0, "2026-09-23T18:00:00")],
        "gtfs_mode STRING, vehicle_id STRING, trip_id STRING, latitude DOUBLE, longitude DOUBLE, position_timestamp STRING",
    ).withColumn("position_timestamp", F.col("position_timestamp").cast("timestamp"))
    seed_df.write.format("delta").saveAsTable(TEST_TABLE)

    stale_df = spark.createDataFrame(
        [("sydneytrains", "V123", "T1", 99.0, 99.0, "2026-09-23T17:00:00")],
        "gtfs_mode STRING, vehicle_id STRING, trip_id STRING, latitude DOUBLE, longitude DOUBLE, position_timestamp STRING",
    ).withColumn("position_timestamp", F.col("position_timestamp").cast("timestamp"))

    upsert_current_position(spark, stale_df, TEST_TABLE)

    result = spark.table(TEST_TABLE).filter("vehicle_id = 'V123'").collect()[0]
    assert result["latitude"] == 10.0, f"FAIL: stale update overwrote a newer row! latitude={result['latitude']}"
    print("PASS: stale position correctly rejected by MERGE guard")