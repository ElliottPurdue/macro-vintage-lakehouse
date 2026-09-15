# Macro Vintage Lakehouse

Economic data gets revised. A quarter's first GDP estimate is followed by a second and a third, then changed again in annual and comprehensive updates, and payrolls, retail sales and industrial production are revised the same way. A model or backtest built on today's numbers is using figures nobody had at the time.

This project builds a lakehouse that keeps every published version of every observation, using ALFRED, the archive of past releases kept by the Federal Reserve Bank of St. Louis. Any question can then be asked as of a date: what was known, and when it became known.

## Status

Phase 1 of 5 is done: object storage, a pinned Python environment, and a check that exercises the whole storage path. Ingestion is next.

## How versions are stored

For each observation, the FRED API reports the stretch of real time during which a value was the published one, from `realtime_start` to `realtime_end`. A value that is still current has a `realtime_end` of `9999-12-31`. The lake keeps those intervals instead of overwriting old values, so the value known on date D is the row where `realtime_start <= D <= realtime_end`.

Planned layers:

- **Bronze**: raw ALFRED responses as Parquet, append only.
- **Silver**: one row per value per knowledge interval, with tests that an observation's versions never overlap.
- **Gold**: as-of snapshots, first release against latest value, and revision statistics per series.

## Stack

| Role | Tool |
|---|---|
| Object storage | SeaweedFS, S3 API, in Docker |
| Query engine | DuckDB, reading and writing Parquet over S3 |
| Transformations and tests | dbt Core with dbt-duckdb |
| Orchestration | Dagster, with dagster-dbt for lineage |
| Ingestion | Python with requests, Polars and boto3 |

MinIO was the original choice for storage, but its community edition was archived in April 2026. The Python code reaches storage only through the S3 API, configured by the `LAKE_S3_*` settings in `.env`.

## Running it

Requires Docker and Python 3.11 or newer.

```bash
cp .env.example .env                  # then set LAKE_S3_SECRET_ACCESS_KEY
docker compose up -d --wait
python -m venv .venv
source .venv/bin/activate             # Windows: .venv\Scripts\activate
pip install -e .
python -m macro_lake.check_stack
```

```
object store  ok    bucket 'lake' at http://127.0.0.1:8333
duckdb        ok    wrote 2 Parquet objects, read 3 rows back, 3 as-of lookups correct
fred api      skip  FRED_API_KEY is not set
```

The check writes a small made-up table containing one revised value, reads it back through DuckDB, looks the value up before its first release, after it, and after the revision, then deletes what it wrote. With `FRED_API_KEY` set, it also confirms FRED accepts the key.

## Roadmap

1. Storage, environment and stack check (done)
2. Incremental, idempotent ALFRED ingestion into bronze
3. dbt silver and gold models, with tests on version intervals
4. Dagster assets, schedules and a backfill
5. CI on GitHub Actions against a SeaweedFS service container

## License and data terms

The code is MIT licensed; see [LICENSE](LICENSE).

This product uses the FRED® API but is not endorsed or certified by the Federal Reserve Bank of St. Louis. Some FRED series are owned by third parties, whose permission is needed for anything beyond personal use, so the lake stays local and no raw data is published here.
