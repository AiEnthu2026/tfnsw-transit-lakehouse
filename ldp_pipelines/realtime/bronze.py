from pyspark import pipelines as dp
from pyspark.sql import functions as F

BASE_LANDING = spark.conf.get("tfnsw.base_landing")

RAW_JSON_ENTITIES = ["vehicle_positions", "trip_updates"]

def _make_forldp_bronze_flow(feed_type: str):
    def flow():
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "text")
            .option("cloudFiles.schemaLocation", f"{BASE_LANDING}/realtime_raw_forldp/{feed_type}/_schema")
            .load(f"{BASE_LANDING}/realtime_raw_forldp/{feed_type}/")
            .select(
                F.col("value").alias("raw_json_line"),
                F.col("_metadata.file_path").alias("source_file"),
                F.current_timestamp().alias("ingestion_timestamp"),
            )
        )
    return flow

for feed_type in RAW_JSON_ENTITIES:
    dp.create_streaming_table(
        name=f"bronze_gtfs_{feed_type}_forldp",
        comment=f"Raw {feed_type} JSON lines landed by the polling notebook, discovered via Auto Loader (LDP-native Bronze experiment).",
    )
    dp.append_flow(
        target=f"bronze_gtfs_{feed_type}_forldp",
        name=f"bronze_{feed_type}_forldp_flow",
        comment=f"Auto Loader discovery of landed {feed_type} JSON files.",
    )(_make_forldp_bronze_flow(feed_type))