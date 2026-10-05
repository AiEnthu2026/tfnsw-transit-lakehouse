import sys, os, time
sys.path.append(os.path.abspath("../.."))
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F
from databricks.sdk import WorkspaceClient
import pytest

TEST_VEHICLE_ID = "TEST_V_CDC_CHECK"
BRONZE_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.bronze_gtfs_vehicle_positions"
SILVER_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.silver_gtfs_vehicle_positions"
CURRENT_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.silver_gtfs_vehicle_positions_current"
PIPELINE_NAME = f"tfnsw_ldp_realtime_pipeline_{CATALOG_NAME}"
UPDATE_TIMEOUT_SECONDS = 600
UPDATE_POLL_SECONDS = 15
ENTITY_ID = f"E_{TEST_VEHICLE_ID}"


@pytest.fixture
def clean_test_vehicle():
    def _cleanup():
        spark.sql(f"DELETE FROM {BRONZE_TABLE} WHERE entity_id = '{ENTITY_ID}'")
        spark.sql(f"DELETE FROM {SILVER_TABLE} WHERE vehicle_id = '{TEST_VEHICLE_ID}'")
        spark.sql(f"DELETE FROM {CURRENT_TABLE} WHERE vehicle_id = '{TEST_VEHICLE_ID}'")
    _cleanup()   # in case a previous run left rows behind
    yield
    _cleanup()   # don't leave synthetic test data in a table you'd show in an interview


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


def test_realtime_current_position_reflects_latest_not_stale(clean_test_vehicle):
    newer_ts = datetime.now(timezone.utc)
    stale_ts = newer_ts - timedelta(minutes=5)

    # Written in this order deliberately -- newer row lands first, stale
    # row second -- so a pass here proves create_auto_cdc_flow is deciding
    # the winner by sequence_by (position_timestamp), not arrival order.
    _seed_bronze_row(newer_ts, -33.90, 151.20)
    _seed_bronze_row(stale_ts, 99.0, 99.0)

    w = WorkspaceClient()
    pipeline_id = _find_pipeline_id(w)
    _run_pipeline_update_and_wait(w, pipeline_id)

    rows = spark.table(CURRENT_TABLE).filter(f"vehicle_id = '{TEST_VEHICLE_ID}'").collect()
    assert len(rows) == 1, f"FAIL: expected exactly one current row, got {len(rows)}"
    assert rows[0]["latitude"] == -33.90, f"FAIL: stale row overwrote the newer one, latitude={rows[0]['latitude']}"
    print("PASS: silver_gtfs_vehicle_positions_current reflects the newest position via create_auto_cdc_flow")
