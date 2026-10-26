from pyspark.sql import functions as F

def enrich_vehicle_positions(vp_df, trips_dim, routes_dim, stops_dim):
    """
    Left-join vehicle positions to the static trips, routes and stops dimensions.

    Unresolved trips, routes or stops stay as rows with nulls. route_id is the feed's own
    value, falling back to the static trip's; a missing occupancy_status becomes 'Not Reported'.
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
    Left-join trip delays to the static trips, routes and stops dimensions.

    Unresolved trips, routes or stops stay as rows with nulls. route_id is the feed's own
    value, falling back to the static trip's. Delay values are passed through from the feed
    unchanged.
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
