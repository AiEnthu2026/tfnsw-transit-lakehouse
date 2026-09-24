import sys, os
sys.path.append(os.path.abspath("../.."))
from pyspark.sql import functions as F
from utils.gold_transforms import enrich_vehicle_positions


def test_gold_join_resolves_route_and_stop_names():
    routes_dim = spark.createDataFrame([("sydneytrains", "T1", "T1", "Central - Emu Plains")],
        "gtfs_mode STRING, route_id STRING, route_short_name STRING, route_long_name STRING")
    trips_dim = spark.createDataFrame([("sydneytrains", "TRIP1", "T1", "Emu Plains", 0)],
        "gtfs_mode STRING, trip_id STRING, trip_route_id STRING, trip_headsign STRING, direction_id INT")
    stops_dim = spark.createDataFrame([("sydneytrains", "STOP1", "Central Station")],
        "gtfs_mode STRING, stop_id STRING, stop_name STRING")
    vp_batch = spark.createDataFrame(
        [("sydneytrains", "V1", "TRIP1", None, "STOP1")],
        "gtfs_mode STRING, vehicle_id STRING, trip_id STRING, route_id STRING, stop_id STRING")

    enriched = enrich_vehicle_positions(vp_batch, trips_dim, routes_dim, stops_dim)
    row = enriched.collect()[0]
    assert row["route_short_name"] == "T1", f"FAIL: route not resolved, got {row['route_short_name']}"
    assert row["stop_name"] == "Central Station", f"FAIL: stop not resolved, got {row['stop_name']}"
    print("PASS: Gold join correctly resolves route and stop names, using the real production function")