from pyspark.sql import functions as F

def enrich_vehicle_positions(vp_df, trips_dim, routes_dim, stops_dim):
    """
    Join a vehicle-positions DataFrame against the static GTFS dimensions
    to resolve human-readable route, trip, and stop context, and project
    the full enriched row Gold serves downstream (BI, current-position view).

    All joins are LEFT: a vehicle position whose trip/route/stop can't be
    resolved (e.g. an unscheduled/NonTimetabled movement with no matching
    static trip) still produces a row here, with the unresolved fields NULL,
    rather than silently disappearing from the output. route_id is resolved
    as coalesce(the position's own RT-feed route_id, the static trip's
    route_id), since NonTimetabled trips often carry neither.
    occupancy_status defaults to "Not Reported" rather than staying NULL,
    since GTFS-RT frequently omits it for older rolling stock.

    Dimension DataFrames are expected to already be broadcast by the caller
    (they're small, fixed-for-the-run reference tables) -- this function
    only defines the join logic, not the broadcast/read strategy, so it can
    be exercised directly with small in-memory DataFrames in tests.

    Args:
        vp_df: vehicle-positions DataFrame (streaming or batch) with at
            least entity_id, gtfs_mode, trip_id, route_id, stop_id,
            vehicle_id, vehicle_label, schedule_relationship, latitude,
            longitude, position_timestamp, congestion_level,
            occupancy_status, poll_timestamp, ingestion_timestamp.
        trips_dim: static trips dimension, columns gtfs_mode, trip_id,
            trip_route_id, trip_headsign, direction_id.
        routes_dim: static routes dimension, columns gtfs_mode, route_id,
            route_short_name, route_long_name.
        stops_dim: static stops dimension, columns gtfs_mode, stop_id,
            stop_name.

    Returns:
        A DataFrame at the same grain as vp_df (one row per input row),
        with entity_id, gtfs_mode, vehicle_id, vehicle_label, trip_id,
        route_id (resolved), route_short_name, route_long_name,
        trip_headsign, direction_id, schedule_relationship, latitude,
        longitude, position_timestamp, stop_id, stop_name, congestion_level,
        occupancy_status, poll_timestamp, and ingestion_timestamp.
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
        .select(
            F.col("vp.entity_id"), F.col("vp.gtfs_mode"), F.col("vp.vehicle_id"), F.col("vp.vehicle_label"),
            F.col("vp.trip_id"), F.col("resolved_route_id").alias("route_id"),
            F.col("r.route_short_name"), F.col("r.route_long_name"),
            F.col("t.trip_headsign"), F.col("t.direction_id"), F.col("vp.schedule_relationship"),
            F.col("vp.latitude"), F.col("vp.longitude"), F.col("vp.position_timestamp"),
            F.col("vp.stop_id"), F.col("s.stop_name"), F.col("vp.congestion_level"),
            F.coalesce(F.col("vp.occupancy_status"), F.lit("Not Reported")).alias("occupancy_status"),
            F.col("vp.poll_timestamp"), F.col("vp.ingestion_timestamp"),
        )
    )

def enrich_trip_delays(tu_df, trips_dim, routes_dim, stops_dim):
    """
    Join a trip-delays DataFrame against the static GTFS dimensions to
    resolve human-readable route, trip, and stop context, and project the
    full enriched row Gold serves downstream (BI, route-delay summary view).

    Same join shape as enrich_vehicle_positions: all joins LEFT, route_id
    resolved as coalesce(the RT feed's own route_id, the static trip's
    route_id). Delay figures (arrival_delay_seconds, departure_delay_seconds)
    are passed through unchanged -- they're TfNSW's own reported values,
    this function makes them legible, it doesn't recompute them.

    Dimension DataFrames are expected pre-broadcast by the caller, same
    reason as enrich_vehicle_positions -- keeps this testable with small
    in-memory DataFrames.

    Args:
        tu_df: trip-delays DataFrame (streaming or batch) with at least
            gtfs_mode, trip_id, route_id, vehicle_id, start_date,
            stop_sequence, stop_id, arrival_delay_seconds,
            departure_delay_seconds, poll_timestamp, ingestion_timestamp.
        trips_dim: static trips dimension, columns gtfs_mode, trip_id,
            trip_route_id, trip_headsign, direction_id.
        routes_dim: static routes dimension, columns gtfs_mode, route_id,
            route_short_name, route_long_name.
        stops_dim: static stops dimension, columns gtfs_mode, stop_id,
            stop_name.

    Returns:
        A DataFrame at the same grain as tu_df (one row per input row),
        with gtfs_mode, trip_id, route_id (resolved), route_short_name,
        route_long_name, trip_headsign, direction_id, vehicle_id,
        start_date, stop_sequence, stop_id, stop_name,
        arrival_delay_seconds, departure_delay_seconds, poll_timestamp,
        and ingestion_timestamp.
    """
    return (
        tu_df.alias("tu")
        .join(F.broadcast(trips_dim).alias("t"),
              on=[F.col("tu.gtfs_mode") == F.col("t.gtfs_mode"), F.col("tu.trip_id") == F.col("t.trip_id")], how="left")
        .withColumn("resolved_route_id", F.coalesce(F.col("tu.route_id"), F.col("t.trip_route_id")))
        .join(F.broadcast(routes_dim).alias("r"),
              on=[F.col("tu.gtfs_mode") == F.col("r.gtfs_mode"), F.col("resolved_route_id") == F.col("r.route_id")], how="left")
        .join(F.broadcast(stops_dim).alias("s"),
              on=[F.col("tu.gtfs_mode") == F.col("s.gtfs_mode"), F.col("tu.stop_id") == F.col("s.stop_id")], how="left")
        .select(
            F.col("tu.gtfs_mode"), F.col("tu.trip_id"), F.col("resolved_route_id").alias("route_id"),
            F.col("r.route_short_name"), F.col("r.route_long_name"),
            F.col("t.trip_headsign"), F.col("t.direction_id"), F.col("tu.vehicle_id"), F.col("tu.start_date"),
            F.col("tu.stop_sequence"), F.col("tu.stop_id"), F.col("s.stop_name"),
            F.col("tu.arrival_delay_seconds"), F.col("tu.departure_delay_seconds"),
            F.col("tu.poll_timestamp"), F.col("tu.ingestion_timestamp"),
        )
    )
