# Legacy

Retired implementations, kept as reference for how the design evolved. Nothing here is run by CI or any job, and it is not guaranteed to run as-is.

| Path | What it was |
|---|---|
| `notebooks/silver/`, `notebooks/gold/` | The original notebook-orchestrated Silver and Gold. Static Silver used a hash-diff MERGE; realtime Silver was append-only on a checkpoint. Replaced by the LDP pipelines. |
| `ingestion/` | The batch Bronze ingestion notebook and the landing-only realtime notebook. Replaced by Auto Loader in the static pipeline and the configurable streaming notebook for realtime. |
| `resources/` | The old static refresh job definition. |

The live pipelines are in `ldp_pipelines/`, and their definitions are in `resources/` at the repo root.