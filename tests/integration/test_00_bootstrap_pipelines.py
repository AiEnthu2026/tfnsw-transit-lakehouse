# Bootstrap: builds the static dims and realtime Gold tables that the integration
# tests below depend on, so this gate passes on a fresh environment.
# Same tests as the e2e tier; the fixture-based replacement is on the backlog.

import sys, os, time
sys.path.append(os.path.abspath("../.."))
from databricks.sdk import WorkspaceClient

JOB_NAMES = {
    "static": f"tfnsw_static_refresh_{CATALOG_NAME}",
    "realtime": f"tfnsw_realtime_pipeline_{CATALOG_NAME}",
}
RUN_TIMEOUT_SECONDS = 1800  # static download + realtime's poll window + both LDP updates
RUN_POLL_SECONDS = 20


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
        count = spark.table(f"{CATALOG_NAME}.{SCHEMA_NAME}.{table}").count()
        assert count > 0, f"FAIL: {table} is empty after a full static job run"
    print("PASS: static job ran end-to-end and populated the static dimension tables")


def test_realtime_job_runs_end_to_end_and_produces_queryable_gold():
    w = WorkspaceClient()
    _run_job_and_wait(w, _find_job_id(w, JOB_NAMES["realtime"]))
    for view_or_table in ["gold_vehicle_positions_enriched", "gold_trip_delays_enriched",
                           "gold_vehicle_positions_current_v", "gold_route_delay_summary_v"]:
        spark.sql(f"SELECT * FROM {CATALOG_NAME}.{SCHEMA_NAME}.{view_or_table} LIMIT 1").collect()
    print("PASS: realtime job ran end-to-end; all Gold tables and BI views are queryable")
