import sys, os
sys.path.append(os.path.abspath("../.."))
import json
from pyspark.sql import functions as F
from utils.silver_transforms import parse_vehicle_positions, explode_trip_updates


def test_parse_vehicle_positions_extracts_nested_fields():
    raw_json = json.dumps({
        "trip": {"trip_id": "TRIP1", "route_id": "T1", "schedule_relationship": "SCHEDULED"},
        "vehicle": {"id": "V1", "label": "Train 1"},
        "position": {"latitude": -33.95, "longitude": 150.78},
        "timestamp": "1790030848",
        "stop_id": "STOP1",
        "congestion_level": "RUNNING_SMOOTHLY",
        "occupancy_status": "MANY_SEATS_AVAILABLE",
    })

    bronze_df = spark.createDataFrame(
        [("ENTITY1", "sydneytrains", "2026-09-27T10:00:00", "2026-09-27T10:00:05", raw_json)],
        "entity_id STRING, gtfs_mode STRING, poll_timestamp STRING, ingestion_timestamp STRING, raw_json STRING",
    )

    parsed = parse_vehicle_positions(bronze_df)
    row = parsed.collect()[0]

    assert row["vehicle_id"] == "V1"
    assert row["route_id"] == "T1"
    assert row["latitude"] == -33.95
    assert row["longitude"] == 150.78
    assert row["stop_id"] == "STOP1"
    assert row["position_timestamp"] is not None
    assert row["raw_json"] == raw_json, "FAIL: raw_json should pass through unchanged"
    print("PASS: raw_json correctly parsed into typed vehicle-position columns")

def test_explode_trip_updates_multiple_stops_and_empty_array():
    schema = (
        "gtfs_mode STRING, entity_id STRING, trip_id STRING, route_id STRING, "
        "start_date STRING, vehicle_id STRING, poll_timestamp STRING, ingestion_timestamp STRING, "
        "stop_time_updates ARRAY<STRUCT<stop_sequence:INT, stop_id:STRING, arrival_delay:INT, departure_delay:INT>>"
    )
    bronze_df = spark.createDataFrame(
        [
            # Trip with two stop-time updates -- should explode to two rows.
            ("sydneytrains", "E1", "TRIP1", "T1", "20260927", "V1", "2026-09-27T10:00:00", "2026-09-27T10:00:05",
             [(10, "STOP_A", 60, 90), (11, "STOP_B", 120, 150)]),
            # Trip with an empty stop_time_updates array -- must still survive as one row, nulls not a dropped row.
            ("sydneytrains", "E2", "TRIP2", "T2", "20260927", "V2", "2026-09-27T10:00:00", "2026-09-27T10:00:05",
             []),
        ],
        schema,
    )

    exploded = explode_trip_updates(bronze_df).orderBy("trip_id", "stop_sequence").collect()

    assert len(exploded) == 3, f"FAIL: expected 3 rows (2 exploded + 1 empty-array survivor), got {len(exploded)}"

    trip1_rows = [r for r in exploded if r["trip_id"] == "TRIP1"]
    assert len(trip1_rows) == 2
    assert trip1_rows[0]["stop_id"] == "STOP_A" and trip1_rows[0]["arrival_delay_seconds"] == 60
    assert trip1_rows[1]["stop_id"] == "STOP_B" and trip1_rows[1]["departure_delay_seconds"] == 150

    trip2_row = [r for r in exploded if r["trip_id"] == "TRIP2"][0]
    assert trip2_row["stop_id"] is None and trip2_row["stop_sequence"] is None, \
        "FAIL: empty stop_time_updates array should survive as one null-stop row, not be dropped"

    print("PASS: explode_outer correctly fans out multi-stop trips and preserves empty-array trips")
