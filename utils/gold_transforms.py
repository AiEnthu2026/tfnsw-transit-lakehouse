from pyspark.sql import functions as F

def enrich_vehicle_positions(vp_df, trips_dim, routes_dim, stops_dim):
    """
    Join a vehicle-positions DataFrame against the static GTFS dimensions
    to resolve human-readable route, trip, and stop context.

    All joins are LEFT: a vehicle position whose trip/route/stop can't be
    resolved (e.g. an unscheduled/NonTimetabled movement with no matching
    static trip) still produces a row here, with the unresolved fields NULL,
    rather than silently disappearing from the output. route_id is resolved
    as coalesce(the position's own RT-feed route_id, the static trip's
    route_id), since NonTimetabled trips often carry neither.

    Dimension DataFrames are expected to already be broadcast by the caller
    (they're small, fixed-for-the-run reference tables) -- this function
    only defines the join logic, not the broadcast/read strategy, so it can
    be exercised directly with small in-memory DataFrames in tests.

    Args:
        vp_df: vehicle-positions DataFrame (streaming or batch) with at
            least gtfs_mode, trip_id, route_id, stop_id, vehicle_id.
        trips_dim: static trips dimension, aliased columns gtfs_mode,
            trip_id, trip_route_id, trip_headsign, direction_id.
        routes_dim: static routes dimension, columns gtfs_mode, route_id,
            route_short_name, route_long_name.
        stops_dim: static stops dimension, columns gtfs_mode, stop_id,
            stop_name.

    Returns:
        A DataFrame at the same grain as vp_df (one row per input row),
        with route_short_name, trip_headsign, and stop_name resolved
        wherever possible.
    """
    return (
        vp_df.alias("vp")
        .join(F.broadcast(trips_dim).alias("t"),
              on=[F.col("vp.gtfs_mode") == F.col("t.gtfs_mode"), F.col("vp.trip_id") == F.col("t.trip_id")], how="left")
        .withColumn("resolved_route_id", F.coalesce(F.col("vp.route_id"), F.col("t.trip_route_id")))
        .join(F.broadcast(routes_dim).alias("r"),
              on=[F.col("vp.gtfs_mode") == F.col("r.gtfs_mode"), F.col("resolved_route_id") == F.col("r.route_id")], how="left")
        .join(F.broadcast(stops_dim).alias("s"),
              on=[F.col("vp.gtfs_mode") == F.col("s.gtfs_mode"), F.col("vp.stop_id") == F.col("s.stop_id")], how="left")
        .select("vp.vehicle_id", "r.route_short_name", "t.trip_headsign", "s.stop_name")
    )




