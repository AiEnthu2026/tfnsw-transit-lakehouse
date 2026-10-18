from pyspark.sql import functions as F
import sys, os, time
sys.path.append(os.path.abspath("../.."))
from databricks.sdk import WorkspaceClient

JOB_NAMES = {
    "static": f"tfnsw_static_refresh_{CATALOG_NAME}",
    "realtime": f"tfnsw_realtime_pipeline_{CATALOG_NAME}",
}
RUN_TIMEOUT_SECONDS = 1800  # static download + realtime's poll window + both LDP updates
RUN_POLL_SECONDS = 20

def form_table_name(name): 
    return f"{CATALOG_NAME}.{SCHEMA_NAME}.{name}"

def get_count(name): 
    return spark.table(form_table_name(name)).count()

def find_duplicate_keys(table_name, key_cols):
    df = spark.table(form_table_name(table_name))
    return (
        df.groupBy(*key_cols)
        .agg(F.count("*").alias("record_count"))
        .filter(F.col("record_count") > 1)
    )

def _find_job_id(w, job_name):
    for j in w.jobs.list(name=job_name):
        return j.job_id
    raise RuntimeError(f"Job '{job_name}' not found -- has it been deployed to this target?")

def _run_job_and_wait(w, job_id):
    run = w.jobs.run_now(job_id=job_id)
    deadline = time.monotonic() + RUN_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        run_status = w.jobs.get_run(run_id=run.run_id)
        state = run_status.state.life_cycle_state.value
        if state == "TERMINATED":
            result_state = run_status.state.result_state.value
            if result_state != "SUCCESS":
                raise RuntimeError(f"Job run ended with result_state={result_state}")
            return
        if state in ("INTERNAL_ERROR", "SKIPPED"):
            raise RuntimeError(f"Job run ended in state {state}")
        time.sleep(RUN_POLL_SECONDS)
    raise TimeoutError(f"Job did not finish within {RUN_TIMEOUT_SECONDS}s")


def test_static_job_runs_end_to_end_and_populates_dims():
    w = WorkspaceClient()
    _run_job_and_wait(w, _find_job_id(w, JOB_NAMES["static"]))
    for table in ["silver_gtfs_routes", "silver_gtfs_trips", "silver_gtfs_stops"]:
        table_name = form_table_name(table)
        count = spark.table(table_name).count()
        assert count > 0, f"FAIL: {table} is empty after a full static job run"
    print("PASS: static job ran end-to-end and populated the static dimension tables")


def test_realtime_job_runs_end_to_end_and_produces_queryable_gold():
    w = WorkspaceClient()
    _run_job_and_wait(w, _find_job_id(w, JOB_NAMES["realtime"]))
    for view_or_table in ["gold_vehicle_positions_enriched", "gold_trip_delays_enriched",
                           "gold_vehicle_positions_current_v", "gold_route_delay_summary_v"]:
        spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.{view_or_table} LIMIT 1").collect()
    print("PASS: realtime job ran end-to-end; all Gold tables and BI views are queryable")


# Data checks: keep below the job-run tests; pytest runs tests in file order.
def test_gtfs_vehicle_positions_table_counts_match():
    
    counts = {
        name: get_count(name)
        for name in [
            "bronze_gtfs_vehicle_positions",
            "silver_gtfs_vehicle_positions",
            "silver_gtfs_vehicle_positions_quarantine",
        ]
    }
    assert counts["bronze_gtfs_vehicle_positions"] > 0, "bronze_gtfs_vehicle_positions is empty"
    silver_total = counts["silver_gtfs_vehicle_positions"] + counts["silver_gtfs_vehicle_positions_quarantine"]
    bronze_total = counts["bronze_gtfs_vehicle_positions"]
    assert silver_total == bronze_total, f"silver + quarantine ({silver_total}) != bronze ({bronze_total})"


def test_current_position_has_one_row_per_vehicle():    
    table = "silver_gtfs_vehicle_positions_current"
    assert get_count(table) > 0, f"{table} is empty"

    duplicates = find_duplicate_keys(table, ["gtfs_mode", "vehicle_id"])
    duplicate_count = duplicates.count()
    assert duplicate_count == 0, (
        f"Expected 0 duplicates, found {duplicate_count}. Examples: {duplicates.limit(5).collect()}"
    )


def test_gold_vehicle_positions_row_count_matches_silver():
    silver_gtfs_vehicle_positions_total=get_count("silver_gtfs_vehicle_positions")
    gold_gtfs_vehicle_positions_total=get_count("gold_vehicle_positions_enriched")
    assert silver_gtfs_vehicle_positions_total > 0, "current silver_gtfs_vehicle_positions table is empty"
    assert gold_gtfs_vehicle_positions_total > 0, "current gold_gtfs_vehicle_positions table is empty"
    assert silver_gtfs_vehicle_positions_total == gold_gtfs_vehicle_positions_total, (f"silver_gtfs_vehicle_positions_total ({silver_gtfs_vehicle_positions_total}) != gold_gtfs_vehicle_positions_total ({gold_gtfs_vehicle_positions_total})")


def test_gold_trip_updates_row_count_matches_silver():
    silver_gtfs_trip_updates_total=get_count("silver_gtfs_trip_updates")
    gold_gtfs_trip_updates_total=get_count("gold_trip_delays_enriched")
    assert silver_gtfs_trip_updates_total > 0, "current silver_gtfs_trip_updates table is empty"
    assert gold_gtfs_trip_updates_total > 0, "current gold_gtfs_trip_updates table is empty"
    assert silver_gtfs_trip_updates_total == gold_gtfs_trip_updates_total, (f"silver_gtfs_trip_updates_total ({silver_gtfs_trip_updates_total}) != gold_gtfs_trip_updates_total ({gold_gtfs_trip_updates_total})")


def test_static_dimension_keys_are_unique():
    key_columns = {
        "silver_gtfs_trips": ["trip_id"],
        "silver_gtfs_routes": ["route_id"],
        "silver_gtfs_stops": ["stop_id"],
    }
    for table, keys in key_columns.items():
        duplicates = find_duplicate_keys(table, ["gtfs_mode"] + keys)
        duplicate_count = duplicates.count()
        assert duplicate_count == 0, (
            f"{table}: {duplicate_count} duplicate keys. Examples: {duplicates.limit(5).collect()}"
        )
