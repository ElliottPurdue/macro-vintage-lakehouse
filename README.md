# Macro Vintage Lakehouse

Economic data gets revised. A quarter's first GDP estimate is followed by a second and a third, then changed again in annual and comprehensive updates, and payrolls, retail sales and industrial production are revised the same way. A model or backtest built on today's numbers is using figures nobody had at the time.

This project builds a lakehouse that keeps every published version of every observation, using ALFRED, the archive of past releases kept by the Federal Reserve Bank of St. Louis. Any question can then be asked as of a date: what was known, and when it became known.

## Status

Phases 1 and 2 of 5 are done: object storage, and incremental ingestion of every ALFRED vintage of 20 series into the bronze layer. The dbt models for silver and gold are next.

## How versions are stored

For each observation, the FRED API reports the stretch of real time during which a value was the published one, from `realtime_start` to `realtime_end`. A value that is still current has a `realtime_end` of `9999-12-31`. The lake keeps those intervals instead of overwriting old values, so the value known on date D is the row where `realtime_start <= D <= realtime_end`.

A version can also be a gap. Real GDP for the first quarter of 1947 (`GDPC1`) read 1239.5 from 1992-12-22 to 1996-01-18, then was not published at all, shown as `.`, from 1996-01-19 to 1997-05-06. It returned on 1997-05-07 as 1402.5, and its 14th and current version, published 2023-09-28, reads 2182.681.

Layers:

- **Bronze** (built): raw ALFRED responses as Parquet, append only.
- **Silver** (next): one row per value per knowledge interval, with tests that an observation's versions never overlap.
- **Gold**: as-of snapshots, first release against latest value, and revision statistics per series.

## Bronze

Measured on 2026-09-15:

- **20 series** from BEA, BLS, the Census Bureau, the Department of Labor and the Federal Reserve Board, listed in [config/series.toml](config/series.toml)
- **158,307 version rows**, covering 15,860 observations and 10,794 vintage dates
- **60 Parquet objects**, 1.1 MB in total

How ingestion stays cheap and safe to rerun:

- **It downloads only what changed.** One request per series asks ALFRED for its newest vintage date, and the series is pulled only if that date is newer than the newest snapshot in bronze. The first pull took 80 requests in 40.2 s. A rerun straight after took 20 requests in 9.9 s and wrote nothing.
- **Snapshots are content-addressed.** Objects live under `series_id=.../vintage_through=...` partitions and are named by a hash of their content, so identical data is never written twice. A forced re-download of GDPC1 and PAYEMS wrote nothing.
- **Values stay raw, with lineage.** Values are stored exactly as the API sent them, as strings with `.` for a missing value, next to the run id, ingest time, content hash and source request.
- **Pulls are checked before anything is written.** Pages must add up to the count the API reports, a pull whose observations and vintage dates end on different releases fails, and a series whose FRED notes carry a copyright notice is refused.
- **The client is careful.** Requests are spaced out, rate limits and server errors are retried with backoff, and the API key is kept out of errors and logs.

## Tests

20 unit tests run without a network, against fakes of the FRED API and S3. [tools/mutate.py](tools/mutate.py) breaks 11 of the safeguards above on purpose, one at a time, and checks that a test fails each time. All 11 are caught.

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

Requires Docker, Python 3.11 or newer, and a free [FRED API key](https://fredaccount.stlouisfed.org/apikeys).

```bash
cp .env.example .env                  # set LAKE_S3_SECRET_ACCESS_KEY and FRED_API_KEY
docker compose up -d --wait
python -m venv .venv
source .venv/bin/activate             # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
python -m macro_lake.check_stack      # storage, DuckDB and the FRED key
python -m macro_lake.ingest           # every series in config/series.toml
pytest
python tools/mutate.py
```

The last line of the first ingest, then of a rerun straight after:

```
20 series: 20 new, 0 current, 0 unchanged, 0 refused, 0 failed. 80 API requests in 40.2s.
20 series: 0 new, 20 current, 0 unchanged, 0 refused, 0 failed. 20 API requests in 9.9s.
```

## Roadmap

1. Storage, environment and stack check (done)
2. Incremental, idempotent ALFRED ingestion into bronze (done)
3. dbt silver and gold models, with tests on version intervals
4. Dagster assets, schedules and a backfill
5. CI on GitHub Actions against a SeaweedFS service container

## License and data terms

The code is MIT licensed; see [LICENSE](LICENSE).

This product uses the FRED® API but is not endorsed or certified by the Federal Reserve Bank of St. Louis. Some FRED series are owned by third parties, whose permission is needed for anything beyond personal use, so the lake stays local and no raw data is published here.
