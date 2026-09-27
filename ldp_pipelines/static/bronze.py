from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import os
import re

BASE_LANDING = spark.conf.get("tfnsw.base_landing")
GTFS_MODES = spark.conf.get("tfnsw.gtfs_modes").split(",")
CHECKPOINT_BASE = spark.conf.get("tfnsw.checkpoint_base")

agency_schema = StructType([
    StructField("agency_id", StringType()),
    StructField("agency_name", StringType()),
    StructField("agency_url", StringType()),
    StructField("agency_timezone", StringType()),
    StructField("agency_lang", StringType()),
    StructField("agency_phone", StringType()),
])

routes_schema = StructType([
    StructField("route_id", StringType()),
    StructField("agency_id", StringType()),
    StructField("route_short_name", StringType()),
    StructField("route_long_name", StringType()),
    StructField("route_desc", StringType()),
    StructField("route_type", IntegerType()),
    StructField("route_url", StringType()),
    StructField("route_color", StringType()),
    StructField("route_text_color", StringType()),
])

trips_schema = StructType([
    StructField("route_id", StringType()),
    StructField("service_id", StringType()),
    StructField("trip_id", StringType()),
    StructField("trip_headsign", StringType()),
    StructField("trip_short_name", StringType()),
    StructField("direction_id", IntegerType()),
    StructField("block_id", StringType()),
    StructField("shape_id", StringType()),
    StructField("wheelchair_accessible", IntegerType()),
    StructField("bikes_allowed", IntegerType()),
])

stops_schema = StructType([
    StructField("stop_id", StringType()),
    StructField("stop_code", StringType()),
    StructField("stop_name", StringType()),
    StructField("stop_desc", StringType()),
    StructField("stop_lat", DoubleType()),
    StructField("stop_lon", DoubleType()),
    StructField("zone_id", StringType()),
    StructField("stop_url", StringType()),
    StructField("location_type", IntegerType()),
    StructField("parent_station", StringType()),
    StructField("wheelchair_boarding", IntegerType()),
    StructField("platform_code", StringType()),
])

calendar_schema = StructType([
    StructField("service_id", StringType()),
    StructField("monday", IntegerType()),
    StructField("tuesday", IntegerType()),
    StructField("wednesday", IntegerType()),
    StructField("thursday", IntegerType()),
    StructField("friday", IntegerType()),
    StructField("saturday", IntegerType()),
    StructField("sunday", IntegerType()),
    StructField("start_date", StringType()),
    StructField("end_date", StringType()),
])

shapes_schema = StructType([
    StructField("shape_id", StringType()),
    StructField("shape_pt_lat", DoubleType()),
    StructField("shape_pt_lon", DoubleType()),
    StructField("shape_pt_sequence", IntegerType()),
    StructField("shape_dist_traveled", DoubleType()),
])

stop_times_schema = StructType([
    StructField("trip_id", StringType()),
    StructField("arrival_time", StringType()),
    StructField("departure_time", StringType()),
    StructField("stop_id", StringType()),
    StructField("stop_sequence", IntegerType()),
    StructField("stop_headsign", StringType()),
    StructField("pickup_type", IntegerType()),
    StructField("drop_off_type", IntegerType()),
    StructField("shape_dist_traveled", DoubleType()),
])

ENTITY_REGISTRY = {
    "agency": {
        "schema": agency_schema,
        "key_cols": ["agency_id"],
        "date_cols": [],
    },
    "routes": {
        "schema": routes_schema,
        "key_cols": ["route_id"],
        "date_cols": [],
    },
    "trips": {
        "schema": trips_schema,
        "key_cols": ["trip_id"],
        "date_cols": [],
    },
    "stops": {
        "schema": stops_schema,
        "key_cols": ["stop_id"],
        "date_cols": [],
    },
    "calendar": {
        "schema": calendar_schema,
        "key_cols": ["service_id"],
        "date_cols": ["start_date", "end_date"],
    },
    "shapes": {
        "schema": shapes_schema,
        "key_cols": ["shape_id", "shape_pt_sequence"],
        "date_cols": [],
    },
    "stop_times": {
        "schema": stop_times_schema,
        "key_cols": ["trip_id", "stop_sequence"],
        "date_cols": [],
    },
}

def _make_bronze_flow(entity: str, mode: str, schema, has_files: bool):
    def flow():
        reader = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/{mode}/bronze_gtfs_{entity}/schema")
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .option("header", "true")
            .option("pathGlobFilter", f"{entity}_????????.*")
        )
        if not has_files:
            reader = reader.schema(schema)  # nothing to infer from, so read as an empty stream
        df = reader.load(f"{BASE_LANDING}/{mode}/")
        present = set(df.columns)
        typed = [
            (F.expr(f"try_cast(`{f.name}` AS {f.dataType.simpleString()})")
             if f.name in present else F.lit(None).cast(f.dataType)).alias(f.name)
            for f in schema.fields
        ]
        return df.select(
            *typed,
            F.col("_rescued_data"),
            F.lit(mode).alias("gtfs_mode"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.col("_metadata.file_path").alias("source_file"),
        )
    return flow

# Columns the flow adds on top of the CSV columns. They must be in the
# declared table schema, or Silver can't see them.
AUDIT_FIELDS = [
    StructField("_rescued_data", StringType()),
    StructField("gtfs_mode", StringType()),
    StructField("ingestion_timestamp", TimestampType()),
    StructField("source_file", StringType()),
]

for entity, cfg in ENTITY_REGISTRY.items():
    key_not_null_expr = " AND ".join(f"{k} IS NOT NULL" for k in cfg["key_cols"])

    dp.create_streaming_table(
        name=f"bronze_gtfs_{entity}",
        comment=f"Raw, append-only {entity} records from both GTFS modes (Bronze layer)",
        schema=StructType(cfg["schema"].fields + AUDIT_FIELDS),
        expect_all_or_drop={f"valid_{entity}_key": key_not_null_expr},
    )

    for mode in GTFS_MODES:
        schema=cfg["schema"]
        dp.append_flow(
            target=f"bronze_gtfs_{entity}",
            name=f"bronze_{entity}_{mode}",
            comment=f"{mode} {entity} ingestion via Auto Loader",
        )(_make_bronze_flow(entity, mode, schema, True))