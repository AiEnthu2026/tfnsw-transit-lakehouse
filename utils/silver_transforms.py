from pyspark.sql import functions as F

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

VEHICLE_POSITION_RAW_JSON_SCHEMA = StructType([
    StructField("trip", StructType([
        StructField("trip_id", StringType()),
        StructField("route_id", StringType()),
        StructField("schedule_relationship", StringType()),
    ])),
    StructField("vehicle", StructType([
        StructField("id", StringType()),
        StructField("label", StringType()),
    ])),
    StructField("position", StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
    ])),
    StructField("timestamp", StringType()),
    StructField("stop_id", StringType()),
    StructField("congestion_level", StringType()),
    StructField("occupancy_status", StringType()),
])

def parse_vehicle_positions(bronze_df):
    """
    Parse Bronze's raw_json blob into typed vehicle-position columns.

    Pure function: takes any DataFrame with entity_id, gtfs_mode,
    poll_timestamp, ingestion_timestamp, raw_json columns (streaming
    or a plain in-memory batch for tests) and returns the parsed shape,
    with no quality filtering and no I/O -- that's the caller's job.
    """
    return (
        bronze_df
        .withColumn("parsed", F.from_json(F.col("raw_json"), VEHICLE_POSITION_RAW_JSON_SCHEMA))
        .select(
            F.col("entity_id"),
            F.col("gtfs_mode"),
            F.col("poll_timestamp").cast("timestamp").alias("poll_timestamp"),
            F.col("ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
            F.col("parsed.vehicle.id").alias("vehicle_id"),
            F.col("parsed.vehicle.label").alias("vehicle_label"),
            F.col("parsed.trip.trip_id").alias("trip_id"),
            F.col("parsed.trip.route_id").alias("route_id"),
            F.col("parsed.trip.schedule_relationship").alias("schedule_relationship"),
            F.col("parsed.position.latitude").alias("latitude"),
            F.col("parsed.position.longitude").alias("longitude"),
            F.to_timestamp(F.col("parsed.timestamp").cast("long")).alias("position_timestamp"),
            F.col("parsed.stop_id").alias("stop_id"),
            F.col("parsed.congestion_level").alias("congestion_level"),
            F.col("parsed.occupancy_status").alias("occupancy_status"),
            F.col("raw_json"),   # kept so the quarantine branch can retain the original payload
        )
    )

def explode_trip_updates(bronze_df):
    """
    Explode Bronze's nested stop_time_updates array to one row per
    (trip, stop) -- the grain delay metrics actually need.

    Pure function, no filtering: explode_outer (not explode) so a trip
    with an empty stop_time_updates array still produces one row, with
    nulls for the stop fields, instead of vanishing.
    """
    return (
        bronze_df
        .withColumn("stu", F.explode_outer("stop_time_updates"))
        .select(
            F.col("gtfs_mode"),
            F.col("entity_id"),
            F.col("trip_id"),
            F.col("route_id"),
            F.col("start_date"),
            F.col("vehicle_id"),
            F.col("poll_timestamp").cast("timestamp").alias("poll_timestamp"),
            F.col("ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
            F.col("stu.stop_sequence").alias("stop_sequence"),
            F.col("stu.stop_id").alias("stop_id"),
            F.col("stu.arrival_delay").alias("arrival_delay_seconds"),
            F.col("stu.departure_delay").alias("departure_delay_seconds"),
        )
    )
