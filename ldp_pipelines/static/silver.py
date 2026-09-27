from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Mirrors ENTITY_REGISTRY's key_cols/date_cols from bronze and silver_gtfs_static notebook.
# ENTITY_REGISTRY duplicating GTFS_ENTITIES from Bronze -- fine for now, fold
# into one shared config source later.
ENTITY_KEYS = {
    "agency": ["agency_id"],
    "routes": ["route_id"],
    "trips": ["trip_id"],
    "stops": ["stop_id"],
    "calendar": ["service_id"],
    "shapes": ["shape_id", "shape_pt_sequence"],
    "stop_times": ["trip_id", "stop_sequence"],
}

ENTITY_DATE_COLS = {
    "calendar": ["start_date", "end_date"],
    "calendar_dates": ["date"],
}

def _make_typed_view(entity: str, date_cols: list):
    def view():
        df = spark.readStream.table(f"bronze_gtfs_{entity}")
        for date_col in date_cols:
            df = df.withColumn(date_col, F.to_date(F.col(date_col), "yyyyMMdd"))
        return df
    return view

for entity, key_cols in ENTITY_KEYS.items():
    date_cols = ENTITY_DATE_COLS.get(entity, [])
    keys = ["gtfs_mode"] + key_cols

    if date_cols:
        typed_view_name = f"_silver_gtfs_{entity}_typed"
        dp.temporary_view(
            name=typed_view_name,
            comment=f"Date-parsed staging view for {entity}, feeding its CDC flow",
        )(_make_typed_view(entity, date_cols))
        cdc_source = typed_view_name
    else:
        cdc_source = f"bronze_gtfs_{entity}"
    
    dp.create_streaming_table(
        name=f"silver_gtfs_{entity}",
        comment=f"Deduplicated {entity} records (Silver layer) — latest row per key",
    )

    dp.create_auto_cdc_flow(
        target=f"silver_gtfs_{entity}",
        source=cdc_source,
        keys=keys,
        sequence_by="ingestion_timestamp",
        stored_as_scd_type="1",
    )