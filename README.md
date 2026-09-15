# Macro Vintage Lakehouse

[![CI](https://github.com/ElliottPurdue/macro-vintage-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/ElliottPurdue/macro-vintage-lakehouse/actions/workflows/ci.yml)

Economic data gets revised. A quarter's first GDP estimate is followed by a second and a third, then changed again in annual and comprehensive updates, and payrolls, retail sales and industrial production are revised the same way. A model or backtest built on today's numbers is using figures nobody had at the time.

This project builds a lakehouse that keeps every published version of every observation, using ALFRED, the archive of past releases kept by the Federal Reserve Bank of St. Louis. Any question can then be asked as of a date: what was known, and when it became known.

## Status

All five phases are done: object storage, incremental ingestion of every ALFRED vintage of 20 series, dbt models that turn the raw responses into version histories and revision statistics, Dagster to schedule and backfill the lot, and CI that rebuilds everything on Linux against a real object store.

## How versions are stored

For each observation, the FRED API reports the stretch of real time during which a value was the published one, from `realtime_start` to `realtime_end`. A value that is still current has a `realtime_end` of `9999-12-31`. The lake keeps those intervals instead of overwriting old values, so the value known on date D is the row where `valid_from <= D <= valid_to`.

A version can also be a gap. Real GDP for the first quarter of 1947 (`GDPC1`) read 1239.5 from 1992-12-22 to 1996-01-18, then was not published at all, shown as `.`, from 1996-01-19 to 1997-05-06. It returned on 1997-05-07 as 1402.5, and its 14th and current version, published 2023-09-28, reads 2182.681.

Layers, each written back to object storage as Parquet:

- **Bronze**: raw ALFRED responses, append only, one content-addressed snapshot per series per release.
- **Silver**: `observation_versions`, one row per value per knowledge interval.
- **Gold**: `release_revisions`, one row per observation comparing its first release with today, and `series_revision_summary`, the statistics per series.

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

## Silver and gold

dbt Core reads bronze through DuckDB and writes Parquet back to the lake, so object storage stays the source of truth and DuckDB is only the engine. A full build takes 1.3 s.

`silver/observation_versions.parquet` holds all 158,307 versions, sorted by series and date so readers can skip row groups. Its tests are the interval rules that the rest of the project depends on, and every one of them holds across all 20 series:

- No two versions of an observation overlap, and none leave a gap: each version ends the day before the next begins.
- Every observation has exactly one current version.
- No version repeats the value before it, so every version is a real change.
- Every version starts on one of that series' release dates, and every release date starts at least one version.
- No value is published before the period it describes has begun.

`gold/release_revisions.parquet` compares each observation's first release with the newest one. What counts as the headline number is set per series in [dbt/seeds/series_measures.csv](dbt/seeds/series_measures.csv): the value itself for rates and counts, the monthly change for payrolls, and the percent change for levels and indexes. That last choice matters, because indexes and chained-dollar series get rebased: 2008 Q4 real GDP reads 11,599.4 in the January 2009 vintage and 16,485.35 today, but almost all of that gap is the switch to 2017 dollars, not revision. Percent changes survive rebasing; levels do not.

A change is always computed against the previous period as it was known on the same day, so a first release is never compared with a later revision of its neighbor.

## What the data shows

Revisions are measured over mature observations: those first released at least three years before their series' newest release, so recent figures that have not been through an annual revision yet don't flatter the numbers. That leaves 13,944 of 15,860 observations.

| Series | Headline number | Mature observations | Ever revised | Median revision | Average revision | Largest |
|---|---|---|---|---|---|---|
| Real GDP | quarterly % change | 304 | 100% | 0.26 pp | +0.08 pp | +1.62 pp (2020 Q2) |
| Industrial production | monthly % change | 1,254 | 99.3% | 0.40 pp | +0.07 pp | -9.83 pp (Dec 1934) |
| Nonfarm payrolls | monthly change in jobs | 1,014 | 99.8% | 61,500 jobs | +18,600 jobs | -697,000 (Mar 2020) |
| Retail sales | monthly % change | 377 | 100% | 0.32 pp | +0.04 pp | -2.18 pp (Nov 2008) |
| Core PCE prices | monthly % change | 773 | 100% | 0.034 pp | +0.005 pp | +0.48 pp (Aug 1992) |
| CPI | monthly % change | 917 | 66.8% | 0.037 pp | +0.001 pp | +0.33 pp (Dec 1973) |
| Unemployment rate | the rate | 908 | 57.3% | 0.1 pp | -0.006 pp | -0.4 pp (Dec 1981) |
| Initial jobless claims | the level | 2,957 | 31.0% | 0 claims | -1,548 claims | -702,000 (week of 28 Mar 2020) |

Eleven of the twenty series have revised every mature observation at least once. Industrial production carries the most versions, 31.2 per observation on average; the unemployment rate carries 2.4.

Payrolls show the pattern that matters to anyone trading or modelling on the release: the first estimate of a month's job growth lands 18,600 jobs below its final value on average, and half of all months move by more than 61,500 jobs. March 2020 was first reported as 701,000 jobs lost and now reads 1,398,000.

Asking what was known on a date is a filter on one column pair:

```sql
select value, valid_from
from 's3://lake/silver/observation_versions.parquet'
where series_id = 'GDPC1'
  and observation_date = date '2008-10-01'
  and date '2009-01-30' between valid_from and valid_to
```

On 2009-01-30, the day the first estimate landed, 2008 Q4 real GDP was falling 0.96% for the quarter, about 3.8% at an annual rate, which is the number that was in the news that week. Twelve versions later it stands at 2.19% for the quarter, about 8.5% annualized.

## Scheduling and backfills

Dagster holds the ingestion and the models in one graph. Every series is a partition of the three bronze assets, and the dbt models attach to those assets through their sources, so lineage runs from a FRED request to the revision statistics without being wired by hand.

- **Assets declare when they should run.** The bronze assets carry a cron condition, weekday mornings after the 8:30 Eastern releases, and the dbt models rebuild as soon as the data they read is updated. The daemon evaluates all ten assets on every tick and launches only what the conditions ask for.
- **Checks travel with the assets.** Every dbt test appears as an asset check, next to a Python check that each snapshot's newest release date matches the partition it is filed under.
- **A backfill of all 20 series** ran 20 runs to success in 204 s, two at a time because everything that calls FRED shares one concurrency pool. It made exactly one request per series, wrote nothing, and reported every partition as already current, which is idempotency shown rather than asserted.

```bash
dagster dev                                              # graph, runs and backfills at 127.0.0.1:3000
dagster job backfill --job ingest_all_series --all       # re-check every series
```

## Tests

- **dbt**: 36 data tests and 2 unit tests, covering the interval rules, the seed, and the arithmetic behind the revision numbers.
- **Python**: 31 unit tests for the ingestion client, the bronze writer, settings and the asset graph, running without a network.
- **Mutation check**: [tools/mutate.py](tools/mutate.py) breaks 17 safeguards on purpose, one at a time, 12 in the Python code and 5 in the dbt models, and confirms a test fails each time.
- **CI** runs the unit tests and the Python mutations on Python 3.11 and 3.13, then starts SeaweedFS as a service container, fills bronze with synthetic fixture data, and builds every model with its tests on Linux. No API key is involved: [tools/make_fixture_lake.py](tools/make_fixture_lake.py) writes ALFRED-shaped data, revisions and withdrawn periods included, through the same bronze writer the real ingestion uses.

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

Requires Docker and Python 3.11 or newer. A [FRED API key](https://fredaccount.stlouisfed.org/apikeys) is free, and only the real ingestion needs one.

```bash
cp .env.example .env                  # set LAKE_S3_SECRET_ACCESS_KEY, FRED_API_KEY and DAGSTER_HOME
docker compose up -d --wait
python -m venv .venv
source .venv/bin/activate             # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
python -m macro_lake.check_stack      # storage, DuckDB and the FRED key
python -m macro_lake.ingest           # every series in config/series.toml
dotenv run -- dbt build               # silver and gold, with their tests
pytest
python tools/mutate.py
```

Without a key, `python tools/make_fixture_lake.py` fills bronze with synthetic data and everything downstream works the same.

dbt doesn't read `.env` itself, which is why it runs through `dotenv`; `DBT_PROJECT_DIR` and `DBT_PROFILES_DIR` in `.env` point it at [dbt/](dbt). Dagster reads `.env` on its own, but needs `DAGSTER_HOME` set to an absolute path, for which [dagster_home/](dagster_home) is the natural choice.

The last line of the first ingest, then of a rerun straight after:

```
20 series: 20 new, 0 current, 0 unchanged, 0 refused, 0 failed. 80 API requests in 40.2s.
20 series: 0 new, 20 current, 0 unchanged, 0 refused, 0 failed. 20 API requests in 9.9s.
```

## Roadmap

1. Storage, environment and stack check (done)
2. Incremental, idempotent ALFRED ingestion into bronze (done)
3. dbt silver and gold models, with tests on version intervals (done)
4. Dagster assets, schedules and a backfill (done)
5. CI on GitHub Actions against a SeaweedFS service container (done)

## License and data terms

The code is MIT licensed; see [LICENSE](LICENSE).

This product uses the FRED® API but is not endorsed or certified by the Federal Reserve Bank of St. Louis. Some FRED series are owned by third parties, whose permission is needed for anything beyond personal use, so the lake stays local and no raw data is published here.
