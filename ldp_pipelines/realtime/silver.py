from pyspark import pipelines as dp
from pyspark.sql import functions as F
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.getcwd(), "..", "..")))
from utils.silver_transforms import parse_vehicle_positions, explode_trip_updates

VP_VALID_CONDITION = "vehicle_id IS NOT NULL AND latitude IS NOT NULL AND longitude IS NOT NULL"

dp.create_streaming_table(
    name="silver_gtfs_vehicle_positions",
    comment="Parsed, valid vehicle-position history (Silver layer) -- append-only.",
)

@dp.append_flow(
    target="silver_gtfs_vehicle_positions",
    name="silver_vehicle_positions_valid",
    comment="Parsed vehicle positions with a usable vehicle_id and coordinates.",
)
def vehicle_positions_valid():
    parsed = parse_vehicle_positions(spark.readStream.option("skipChangeCommits", "true").table("bronze_gtfs_vehicle_positions"))
    return parsed.filter(VP_VALID_CONDITION).drop("raw_json")


dp.create_streaming_table(
    name="silver_gtfs_vehicle_positions_quarantine",
    comment="Vehicle positions missing a usable id/coordinates -- kept, not dropped, for inspection.",
)

@dp.append_flow(
    target="silver_gtfs_vehicle_positions_quarantine",
    name="silver_vehicle_positions_invalid",
    comment="Parsed vehicle positions failing the validity check, original payload retained.",
)
def vehicle_positions_invalid():
    parsed = parse_vehicle_positions(spark.readStream.option("skipChangeCommits", "true").table("bronze_gtfs_vehicle_positions"))
    return parsed.filter(f"NOT ({VP_VALID_CONDITION})").withColumnRenamed("raw_json", "_raw_json")


dp.create_streaming_table(
    name="silver_gtfs_vehicle_positions_current",
    comment="Latest known position per (gtfs_mode, vehicle_id) -- SCD-1 current-state table.",
)

@dp.temporary_view(name="silver_vehicle_positions_changes_skipped")
def vp_cdc_source():
    return spark.readStream.option("skipChangeCommits", "true").table("silver_gtfs_vehicle_positions")
dp.create_auto_cdc_flow(
    target="silver_gtfs_vehicle_positions_current",
    source="silver_vehicle_positions_changes_skipped",
    keys=["gtfs_mode", "vehicle_id"],
    sequence_by="position_timestamp",
    stored_as_scd_type="1",
)

TU_VALID_CONDITION = "trip_id IS NOT NULL AND (stop_sequence IS NOT NULL OR stop_id IS NOT NULL)"

dp.create_streaming_table(
    name="silver_gtfs_trip_updates",
    comment="Exploded (trip, stop) grain trip-update history (Silver layer) -- append-only.",
)

@dp.append_flow(
    target="silver_gtfs_trip_updates",
    name="silver_trip_updates_valid",
    comment="Exploded trip updates that can be attributed to a trip/stop.",
)
def trip_updates_valid():
    exploded = explode_trip_updates(spark.readStream.option("skipChangeCommits", "true").table("bronze_gtfs_trip_updates"))
    return exploded.filter(TU_VALID_CONDITION)


dp.create_streaming_table(
    name="silver_gtfs_trip_updates_quarantine",
    comment="Trip updates that can't be attributed to a trip/stop -- kept, not dropped, for inspection.",
)

@dp.append_flow(
    target="silver_gtfs_trip_updates_quarantine",
    name="silver_trip_updates_invalid",
    comment="Exploded trip updates failing the validity check.",
)
def trip_updates_invalid():
    exploded = explode_trip_updates(spark.readStream.option("skipChangeCommits", "true").table("bronze_gtfs_trip_updates"))
    return exploded.filter(f"NOT ({TU_VALID_CONDITION})")