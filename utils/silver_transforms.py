from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def upsert_current_position(spark, batch_df, target_table):
    """
    Upsert the latest known position per (gtfs_mode, vehicle_id) into a
    'current state' Delta table, via foreachBatch + MERGE.

    Deduplicates the incoming micro-batch to one row per vehicle (keeping
    the row with the latest position_timestamp), then MERGEs into
    target_table -- a matched row is only overwritten if the incoming
    position is genuinely newer, so a stale/out-of-order update never
    clobbers a more recent one.

    Args:
        spark: active SparkSession.
        batch_df: micro-batch DataFrame of vehicle position rows.
        target_table: fully-qualified Delta table name to upsert into.
    """
    
    w = Window.partitionBy("gtfs_mode", "vehicle_id").orderBy(F.col("position_timestamp").desc())
    latest_in_batch = (
        batch_df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    if not spark.catalog.tableExists(target_table):
        latest_in_batch.write.format("delta").saveAsTable(target_table)
        return
    
    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("target")
        .merge(latest_in_batch.alias("source"),
               "target.gtfs_mode = source.gtfs_mode AND target.vehicle_id = source.vehicle_id")
        .whenMatchedUpdate(
            condition="source.position_timestamp > target.position_timestamp",
            set={c: f"source.{c}" for c in latest_in_batch.columns}
        )
        .whenNotMatchedInsertAll()
        .execute()
    )



def upsert_entity_to_silver(spark, bronze_table: str, silver_table: str,
                             schema, key_cols: list, date_cols: list) -> str:
    """
    Conform one GTFS static entity from Bronze into a deduplicated, typed
    Silver table via a hash-diff MERGE.

    Reads the full Bronze table for this entity, keeps only the most
    recently ingested row per (gtfs_mode, key_cols) using ingestion_timestamp
    (Bronze is append-only and duplicate-tolerant, so the same key can appear
    many times across daily drops), casts business columns to their declared
    Silver types, and computes a SHA-256 hash over those business columns.

    On first run for this entity, writes the deduplicated/typed rows
    directly. On subsequent runs, MERGEs into the existing Silver table:
    a matched row is only updated if its row_hash changed, so unchanged
    rows are never rewritten -- only genuinely new or changed data touches
    the target table.

    Args:
        spark: active SparkSession.
        bronze_table: fully-qualified source Bronze table
            (e.g. "{catalog}.{schema}.bronze_gtfs_routes").
        silver_table: fully-qualified target Silver table to upsert into.
        schema: StructType defining this entity's business columns and types.
        key_cols: business key column names (gtfs_mode is always prepended).
        date_cols: subset of schema columns holding GTFS "YYYYMMDD" date
            strings, parsed explicitly into DATE after the generic cast.

    Returns:
        "created" if this was the first write for the entity,
        "merged" if an existing Silver table was upserted,
        "skipped_no_bronze" if the source Bronze table doesn't exist yet.
    """
    
    if not spark.catalog.tableExists(bronze_table):
        return "skipped_no_bronze"

    bronze_df = spark.table(bronze_table)
    typed_df = bronze_df.select(
        F.col("gtfs_mode"), F.col("ingestion_timestamp"),
        *[(F.col(f.name).cast(f.dataType).alias(f.name) if f.name in bronze_df.columns
           else F.lit(None).cast(f.dataType).alias(f.name)) for f in schema.fields],
    )
    for date_col in date_cols:
        typed_df = typed_df.withColumn(date_col, F.to_date(F.col(date_col), "yyyyMMdd"))

    full_key = ["gtfs_mode"] + key_cols
    recency_window = Window.partitionBy(*full_key).orderBy(F.col("ingestion_timestamp").desc())
    latest_df = (typed_df.withColumn("_rn", F.row_number().over(recency_window))
                 .filter(F.col("_rn") == 1).drop("_rn"))

    hash_cols = [c for c in typed_df.columns if c not in full_key + ["ingestion_timestamp"]]
    latest_df = (latest_df
        .withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in hash_cols]), 256))
        .withColumn("silver_updated_at", F.current_timestamp()))

    if not spark.catalog.tableExists(silver_table):
        latest_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        return "created"

    target = DeltaTable.forName(spark, silver_table)
    merge_condition = " AND ".join(f"target.{c} = source.{c}" for c in full_key)
    (target.alias("target").merge(latest_df.alias("source"), merge_condition)
        .whenMatchedUpdate(condition="target.row_hash <> source.row_hash",
                            set={c: f"source.{c}" for c in latest_df.columns})
        .whenNotMatchedInsertAll().execute())
    return "merged"