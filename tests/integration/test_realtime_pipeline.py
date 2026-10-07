import sys, os, time, uuid
sys.path.append(os.path.abspath("../.."))
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F
from databricks.sdk import WorkspaceClient
import pytest

TEST_VEHICLE_ID = f"TEST_V_CDC_{uuid.uuid4().hex[:8]}"
BRONZE_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.bronze_gtfs_vehicle_positions"
SILVER_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.silver_gtfs_vehicle_positions"
CURRENT_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.silver_gtfs_vehicle_positions_current"
PIPELINE_NAME = f"tfnsw_ldp_realtime_pipeline_{CATALOG_NAME}"
UPDATE_TIMEOUT_SECONDS = 600
UPDATE_POLL_SECONDS = 15
ENTITY_ID = f"E_{TEST_VEHICLE_ID}"

def _find_pipeline_id(w):
    for p in w.pipelines.list_pipelines():
        if p.name == PIPELINE_NAME:
            return p.pipeline_id
    raise RuntimeError(f"Pipeline '{PIPELINE_NAME}' not found -- has it been deployed to this target?")

def _run_pipeline_update_and_wait(w, pipeline_id):
    update = w.pipelines.start_update(pipeline_id=pipeline_id)
    deadline = time.monotonic() + UPDATE_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        status = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update.update_id)
        state = status.update.state.value
        if state == "COMPLETED":
            return
        if state in ("FAILED", "CANCELED"):
            raise RuntimeError(f"Pipeline update ended in state {state}")
        time.sleep(UPDATE_POLL_SECONDS)
    raise TimeoutError(f"Pipeline update did not complete within {UPDATE_TIMEOUT_SECONDS}s")

def _wait_until_idle(w, pipeline_id, timeout=600):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        latest = w.pipelines.get(pipeline_id=pipeline_id).latest_updates or []
        if not latest or latest[0].state.value in ("COMPLETED", "FAILED", "CANCELED"):
            return
        time.sleep(UPDATE_POLL_SECONDS)
    raise TimeoutError("Pipeline still had an active update after waiting")

def _seed_bronze_row(position_timestamp, latitude, longitude):
    raw_json = (
        '{"vehicle": {"id": "%s"}, "position": {"latitude": %s, "longitude": %s}, "timestamp": "%d"}'
        % (TEST_VEHICLE_ID, latitude, longitude, int(position_timestamp.timestamp()))
    )
    (spark.createDataFrame(
        [(ENTITY_ID, "sydneytrains", position_timestamp, raw_json)],
        "entity_id STRING, gtfs_mode STRING, poll_timestamp TIMESTAMP, raw_json STRING",
    )
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .write.format("delta").mode("append").saveAsTable(BRONZE_TABLE))

NEWER_LAT = -33.77
STALE_LAT = -33.00   # valid coordinate, so the row is accepted into Silver


@pytest.fixture(scope="module")
def seeded_run():
    newer_ts = datetime.now(timezone.utc)
    stale_ts = newer_ts - timedelta(minutes=5)

    # Newer lands first, stale second, so the winner can't be arrival order.
    _seed_bronze_row(newer_ts, NEWER_LAT, 151.11)
    _seed_bronze_row(stale_ts, STALE_LAT, 151.00)

    w = WorkspaceClient()
    pipeline_id = _find_pipeline_id(w)
    _wait_until_idle(w, pipeline_id)
    _run_pipeline_update_and_wait(w, pipeline_id)   # raises if the update fails
    return pipeline_id


def test_pipeline_update_completes(seeded_run):
    assert seeded_run


def test_both_rows_reach_silver(seeded_run):
    n = spark.table(SILVER_TABLE).filter(f"vehicle_id = '{TEST_VEHICLE_ID}'").count()
    assert n == 2, f"expected both rows in Silver history, got {n}"


def test_current_has_one_row(seeded_run):
    n = spark.table(CURRENT_TABLE).filter(f"vehicle_id = '{TEST_VEHICLE_ID}'").count()
    assert n == 1, f"expected 1 Current row, got {n}"


def test_current_reflects_latest_not_stale(seeded_run):
    row = spark.table(CURRENT_TABLE).filter(f"vehicle_id = '{TEST_VEHICLE_ID}'").collect()[0]
    assert row["latitude"] == NEWER_LAT, f"stale row won, latitude={row['latitude']}"