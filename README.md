# TfNSW Transit Lakehouse

A medallion-architecture pipeline on Databricks + Azure that ingests Transport for NSW's GTFS static and real-time feeds, conforms them into query-ready tables, and serves a live delay/positions dashboard in Power BI.

Built as a portfolio project to demonstrate an end-to-end Databricks + Azure data engineering build: streaming and batch ingestion, Delta Lake, Unity Catalog, and a BI layer on top.

## What it does

TfNSW publishes two kinds of GTFS data for Sydney Trains and buses:

- **Static** — routes, trips, stops, schedules. Changes daily at most.
- **Real-time** — vehicle positions and trip delays, polled from a live protobuf feed.

This pipeline lands both, conforms them through Bronze → Silver → Gold, and exposes a Power BI dashboard showing current vehicle positions and route-level delay summaries.

## Architecture

```
TfNSW API
   │
   ├─ 01_download_gtfs_static.py ──────► raw_landing volume (dated CSVs, per mode)
   │                                            │
   │                                     02a_Bronze_batch_ingestion.py (Auto Loader, append-only)
   │                                            │
   └─ 02b_Bronze_Streaming_ingestion.py ─► bronze_gtfs_vehicle_positions / bronze_gtfs_trip_updates
                                                 │
                        ┌────────────────────────┴────────────────────────┐
                        │                                                  │
              silver_gtfs_static.py                          silver_gtfs_vehicle_positions.py
              (hash-diff MERGE, batch)                        silver_gtfs_trip_updates.py
                        │                                     (streaming, append-only)
                        └────────────────────────┬────────────────────────┘
                                                  │
                                    gold_realtime_enriched.py
                                    (stream-static join, broadcast dims)
                                                  │
                                          Power BI (powerbi/)
```

Everything runs on Databricks Workflows, orchestrated as scheduled tasks. Bronze and Gold streaming jobs use `trigger(availableNow=True)` rather than a continuously-running stream — each run processes what's arrived and finishes, which is what lets a Workflow gate one layer on the previous one's success.

Trains and buses share the same tables throughout, distinguished by a `gtfs_mode` column, rather than one table per mode.

## Repo layout

| Path | Purpose |
|---|---|
| `commonsetup/` | One-time environment provisioning (`00 provision environment.py`) and shared config every notebook `%run`s (`Setup-Common.py`) |
| `download/` | Pulls the GTFS static bundle from TfNSW into the landing volume |
| `ingestion/bronze/` | Auto Loader batch ingestion (static) and a bounded poll loop (real-time) |
| `notebooks/silver/` | Conforms Bronze into typed, deduplicated tables |
| `notebooks/gold/` | Enriched, BI-facing tables and views |
| `utils/` | The actual transform logic (`silver_transforms.py`, `gold_transforms.py`), pulled out of the notebooks so it's unit-testable |
| `tests/` | pytest suite — unit, integration, and e2e, run via `tests/runtests.py` inside a Databricks cluster |
| `powerbi/` | The `.pbix` dashboard and a PDF snapshot of it |

## A few design decisions worth knowing about

**Bronze is append-only, always.** No dedup, no MERGE, no overwrites. A duplicate or corrected row from a later ingestion just becomes another row. Silver is where dedup happens.

**Silver static uses a hash-diff MERGE**, not a full overwrite: take the latest Bronze row per key, hash its business columns, and only touch a Silver row if that hash changed. Silver real-time (vehicle positions, trip updates) stays append-only instead — checkpoint + Delta's transactional write already make re-runs idempotent, so there's nothing to MERGE.

**Bad rows are quarantined, not dropped.** A vehicle position missing coordinates, or a trip update that can't be tied to a stop, goes to a `_quarantine` table with the original payload intact rather than disappearing silently.

**Every join in Gold is LEFT.** An unscheduled/NonTimetabled trip often has no matching static record — that's expected GTFS behavior, not bad data — and it should still produce a Gold row with nulls, not vanish.

**One table per entity, not one per mode.** `bronze_gtfs_trips` holds both trains and buses, split by `gtfs_mode`. Querying across modes means filtering a column, not writing a UNION.

## Known gaps

- Buses' real-time feed is wired up but not yet validated against live traffic — see `ACTIVE_MODES_TONIGHT` in the streaming ingestion notebook.
- The static entity schemas haven't been diffed against a real downloaded file's headers yet; a column-name mismatch will currently surface as nulls rather than a hard failure.
- Environment provisioning (`00 provision environment.py`) is a manual notebook run for now — a Terraform/DABs pass would replace it.

## Running the tests

```
tests/runtests.py
```

runs pytest against `tests/unit`, `tests/integration`, and `tests/e2e` from inside a Databricks cluster (needs `spark`, `CATALOG_NAME`, and `SCHEMA_NAME` in scope — see `tests/conftest.py`).
