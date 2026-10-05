import sys, os
sys.path.append(os.path.abspath("../.."))
from pyspark.sql import functions as F
from utils.gold_transforms import enrich_vehicle_positions, enrich_trip_delays


def test_gold_join_resolves_route_and_stop_names():
    routes_dim = spark.createDataFrame([("sydneytrains", "T1", "T1", "Central - Emu Plains")],
        "gtfs_mode STRING, route_id STRING, route_short_name STRING, route_long_name STRING")
    trips_dim = spark.createDataFrame([("sydneytrains", "TRIP1", "T1", "Emu Plains", 0)],
        "gtfs_mode STRING, trip_id STRING, trip_route_id STRING, trip_headsign STRING, direction_id INT")
    stops_dim = spark.createDataFrame([("sydneytrains", "STOP1", "Central Station")],
        "gtfs_mode STRING, stop_id STRING, stop_name STRING")

    vp_batch = spark.createDataFrame(
        [("E1", "sydneytrains", "V1", "Train 1", "TRIP1", None, "SCHEDULED",
          -33.95, 150.78, "2026-09-27T10:00:00", "STOP1", "RUNNING_SMOOTHLY", None,
          "2026-09-27T10:00:00", "2026-09-27T10:00:05")],
        "entity_id STRING, gtfs_mode STRING, vehicle_id STRING, vehicle_label STRING, trip_id STRING, "
        "route_id STRING, schedule_relationship STRING, latitude DOUBLE, longitude DOUBLE, "
        "position_timestamp STRING, stop_id STRING, congestion_level STRING, occupancy_status STRING, "
        "poll_timestamp STRING, ingestion_timestamp STRING",
    )

    enriched = enrich_vehicle_positions(vp_batch, trips_dim, routes_dim, stops_dim)
    row = enriched.collect()[0]
    assert row["route_short_name"] == "T1", f"FAIL: route not resolved, got {row['route_short_name']}"
    assert row["stop_name"] == "Central Station", f"FAIL: stop not resolved, got {row['stop_name']}"
    assert row["route_id"] == "T1", f"FAIL: route_id should resolve via coalesce, got {row['route_id']}"
    assert row["occupancy_status"] == "Not Reported", f"FAIL: null occupancy_status should default, got {row['occupancy_status']}"
    print("PASS: Gold join resolves route/stop names, resolves route_id, defaults occupancy_status")


def test_gold_join_resolves_route_and_stop_for_trip_delays():
    routes_dim = spark.createDataFrame([("sydneytrains", "T1", "T1", "Central - Emu Plains")],
        "gtfs_mode STRING, route_id STRING, route_short_name STRING, route_long_name STRING")
    trips_dim = spark.createDataFrame([("sydneytrains", "TRIP1", "T1", "Emu Plains", 0)],
        "gtfs_mode STRING, trip_id STRING, trip_route_id STRING, trip_headsign STRING, direction_id INT")
    stops_dim = spark.createDataFrame([("sydneytrains", "STOP1", "Central Station")],
        "gtfs_mode STRING, stop_id STRING, stop_name STRING")

    tu_batch = spark.createDataFrame(
        [("sydneytrains", "TRIP1", None, "V1", "20260927", 10, "STOP1", 60, 90,
          "2026-09-27T10:00:00", "2026-09-27T10:00:05")],
        "gtfs_mode STRING, trip_id STRING, route_id STRING, vehicle_id STRING, start_date STRING, "
        "stop_sequence INT, stop_id STRING, arrival_delay_seconds INT, departure_delay_seconds INT, "
        "poll_timestamp STRING, ingestion_timestamp STRING",
    )

    enriched = enrich_trip_delays(tu_batch, trips_dim, routes_dim, stops_dim)
    row = enriched.collect()[0]
    assert row["route_short_name"] == "T1", f"FAIL: route not resolved, got {row['route_short_name']}"
    assert row["stop_name"] == "Central Station", f"FAIL: stop not resolved, got {row['stop_name']}"
    assert row["arrival_delay_seconds"] == 60, "FAIL: arrival_delay_seconds should pass through unchanged"
    assert row["departure_delay_seconds"] == 90, "FAIL: departure_delay_seconds should pass through unchanged"
    print("PASS: Gold join resolves route/stop for trip delays, delay figures pass through unchanged")


def test_gold_join_trip_delays_preserves_unresolved_rows():
    """
    LEFT joins must not drop rows for unresolved static references -- an
    unscheduled/NonTimetabled trip has no matching static record, and per
    the documented Gold design (every join is LEFT), it should still
    produce one row with nulls, not vanish.
    """
    empty_routes = spark.createDataFrame([], "gtfs_mode STRING, route_id STRING, route_short_name STRING, route_long_name STRING")
    empty_trips = spark.createDataFrame([], "gtfs_mode STRING, trip_id STRING, trip_route_id STRING, trip_headsign STRING, direction_id INT")
    empty_stops = spark.createDataFrame([], "gtfs_mode STRING, stop_id STRING, stop_name STRING")

    tu_batch = spark.createDataFrame(
        [("sydneytrains", "UNSCHEDULED_TRIP", None, "V1", "20260927", None, None, 30, None,
          "2026-09-27T10:00:00", "2026-09-27T10:00:05")],
        "gtfs_mode STRING, trip_id STRING, route_id STRING, vehicle_id STRING, start_date STRING, "
        "stop_sequence INT, stop_id STRING, arrival_delay_seconds INT, departure_delay_seconds INT, "
        "poll_timestamp STRING, ingestion_timestamp STRING",
    )

    enriched = enrich_trip_delays(tu_batch, empty_trips, empty_routes, empty_stops)
    rows = enriched.collect()
    assert len(rows) == 1, f"FAIL: unresolved trip should still produce one row, got {len(rows)}"
    assert rows[0]["trip_id"] == "UNSCHEDULED_TRIP"
    assert rows[0]["route_short_name"] is None and rows[0]["stop_name"] is None
    assert rows[0]["arrival_delay_seconds"] == 30, "FAIL: delay figures should survive even when route/stop are unresolved"
    print("PASS: unresolved trip still produces a row with nulls, delay figures intact")
