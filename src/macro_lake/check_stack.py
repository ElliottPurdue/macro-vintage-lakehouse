"""End-to-end check of the local lake stack.

Start the object store, then run the check from the repository root:

    docker compose up -d
    python -m macro_lake.check_stack

It confirms the bucket answers S3 calls, writes partitioned Parquet through
DuckDB and runs point-in-time lookups against it, and checks FRED_API_KEY
with the FRED API when one is set. Test objects go under _stack_check/ and
are deleted before the check exits.
"""

from __future__ import annotations

import sys
import uuid

import requests

from macro_lake.settings import LakeSettings, fred_api_key, load_lake_settings
from macro_lake.storage import duckdb_connect, s3_client, sql_literal

FRED_SERIES_URL = "https://api.stlouisfed.org/fred/series"

# One observation first published on 2024-01-10 and revised on 2024-02-08,
# plus a second series. The values are made up; only the intervals matter.
SAMPLE = """
    SELECT series_id, observation_date, CAST(value AS DOUBLE) AS value, realtime_start, realtime_end
    FROM (VALUES
        ('TEST_A', DATE '2023-12-01', 100.0, DATE '2024-01-10', DATE '2024-02-07'),
        ('TEST_A', DATE '2023-12-01', 101.5, DATE '2024-02-08', DATE '9999-12-31'),
        ('TEST_B', DATE '2023-12-01', 42.0, DATE '2024-01-12', DATE '9999-12-31')
    ) AS t(series_id, observation_date, value, realtime_start, realtime_end)
"""

# (as-of date, TEST_A value known on that date, None before first release)
AS_OF_CASES = [("2024-01-05", None), ("2024-01-20", 100.0), ("2024-02-08", 101.5)]


def report(check: str, ok: bool | None, detail: str) -> None:
    status = {True: "ok", False: "FAIL", None: "skip"}[ok]
    print(f"{check:<13} {status:<5} {detail}")


def delete_prefix(s3, bucket: str, prefix: str) -> int:
    deleted = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
            deleted += 1
    return deleted


def check_duckdb(settings: LakeSettings, s3) -> str:
    prefix = f"_stack_check/{uuid.uuid4().hex[:12]}/"
    target = settings.uri(prefix + "observations")
    files = sql_literal(target + "/*/*.parquet")
    con = duckdb_connect(settings)
    try:
        con.execute(f"COPY ({SAMPLE}) TO {sql_literal(target)} (FORMAT parquet, PARTITION_BY (series_id))")
        rows, series = con.execute(
            f"SELECT count(*), count(DISTINCT series_id) FROM read_parquet({files}, hive_partitioning = true)"
        ).fetchone()
        if (rows, series) != (3, 2):
            raise AssertionError(f"read back {rows} rows in {series} partitions, expected 3 in 2")
        for as_of, expected in AS_OF_CASES:
            found = con.execute(
                f"""
                SELECT value FROM read_parquet({files}, hive_partitioning = true)
                WHERE series_id = 'TEST_A' AND CAST(? AS DATE) BETWEEN realtime_start AND realtime_end
                """,
                [as_of],
            ).fetchall()
            if len(found) > 1 or (found[0][0] if found else None) != expected:
                raise AssertionError(f"as of {as_of}: got {found}, expected {expected}")
    finally:
        con.close()
        objects = delete_prefix(s3, settings.bucket, prefix)
    return f"wrote {objects} Parquet objects, read 3 rows back, {len(AS_OF_CASES)} as-of lookups correct"


def check_fred(api_key: str) -> str:
    response = requests.get(
        FRED_SERIES_URL,
        params={"series_id": "GDPC1", "api_key": api_key, "file_type": "json"},
        timeout=20,
    )
    try:
        body = response.json()
    except ValueError:
        body = {}
    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code}: {body.get('error_message', response.reason)}")
    return f"key accepted, GDPC1 is {body['seriess'][0]['title']!r}"


def main() -> int:
    try:
        settings = load_lake_settings()
    except RuntimeError as exc:
        print(exc, file=sys.stderr)
        return 2

    s3 = s3_client(settings)
    try:
        s3.head_bucket(Bucket=settings.bucket)
    except Exception as exc:
        report("object store", False, f"{settings.endpoint_url}: {exc} (is `docker compose up -d` running?)")
        return 1
    report("object store", True, f"bucket {settings.bucket!r} at {settings.endpoint_url}")

    failed = False
    try:
        report("duckdb", True, check_duckdb(settings, s3))
    except Exception as exc:
        report("duckdb", False, str(exc))
        failed = True

    api_key = fred_api_key()
    if api_key is None:
        report("fred api", None, "FRED_API_KEY is not set")
    else:
        try:
            report("fred api", True, check_fred(api_key))
        except Exception as exc:
            # Request errors can include the full URL, key and all.
            report("fred api", False, str(exc).replace(api_key, "<FRED_API_KEY>"))
            failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
