from pyspark import pipelines as dp
from pyspark.sql import functions as F
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from utils.gold_transforms import enrich_vehicle_positions, enrich_trip_delays


def _load_static_dims():
    routes_dim = spark.table("silver_gtfs_routes").select(
        "gtfs_mode", "route_id", "route_short_name", "route_long_name")
    trips_dim = spark.table("silver_gtfs_trips").select(
        "gtfs_mode", "trip_id", F.col("route_id").alias("trip_route_id"),
        "trip_headsign", "direction_id")
    stops_dim = spark.table("silver_gtfs_stops").select(
        "gtfs_mode", "stop_id", "stop_name")
    return trips_dim, routes_dim, stops_dim


dp.create_streaming_table(
    name="gold_vehicle_positions_enriched",
    comment="Vehicle positions enriched with static route/trip/stop context.",
    partition_cols=["gtfs_mode"],
)

@dp.append_flow(
    target="gold_vehicle_positions_enriched",
    name="gold_vehicle_positions_enriched_flow",
    comment="Stream-static join of Silver vehicle positions against static dims.",
)
def gold_vehicle_positions_flow():
    trips_dim, routes_dim, stops_dim = _load_static_dims()
    vp_stream = spark.readStream.table("silver_gtfs_vehicle_positions")
    return enrich_vehicle_positions(vp_stream, trips_dim, routes_dim, stops_dim)


dp.create_streaming_table(
    name="gold_trip_delays_enriched",
    comment="Trip delays enriched with static route/trip/stop context.",
    partition_cols=["gtfs_mode"],
)

@dp.append_flow(
    target="gold_trip_delays_enriched",
    name="gold_trip_delays_enriched_flow",
    comment="Stream-static join of Silver trip updates against static dims.",
)
def gold_trip_delays_flow():
    trips_dim, routes_dim, stops_dim = _load_static_dims()
    tu_stream = spark.readStream.table("silver_gtfs_trip_updates")
    return enrich_trip_delays(tu_stream, trips_dim, routes_dim, stops_dim)