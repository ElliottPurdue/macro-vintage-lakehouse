"""Bronze layer: raw API responses kept as immutable, content-addressed Parquet snapshots.

Objects use Hive-style partitions:

    bronze/alfred/observations/series_id=GDPC1/vintage_through=2026-08-26/<digest>.parquet
    bronze/alfred/vintage_dates/series_id=GDPC1/vintage_through=2026-08-26/<digest>.parquet
    bronze/fred/series/series_id=GDPC1/vintage_through=2026-08-26/<digest>.parquet

vintage_through is the newest vintage date a snapshot contains. The file name
comes from a hash of the content, so writing the same data twice gives the
same key and the second write is skipped. Values stay exactly as the API sent
them, as strings, including "." for a missing value; typing is left to silver.
"""

from __future__ import annotations

import hashlib
import io
import json
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any

import polars as pl
from botocore.exceptions import ClientError

OBSERVATIONS = "bronze/alfred/observations"
VINTAGE_DATES = "bronze/alfred/vintage_dates"
SERIES = "bronze/fred/series"

DIGEST_CHARS = 16


def content_digest(rows: Iterable[Mapping[str, Any]]) -> str:
    """SHA-256 of the rows, independent of row order and key order."""
    lines = sorted(json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False) for row in rows)
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def snapshot_key(dataset: str, series_id: str, vintage_through: str, digest: str) -> str:
    return f"{dataset}/series_id={series_id}/vintage_through={vintage_through}/{digest[:DIGEST_CHARS]}.parquet"


def latest_vintage_through(s3, bucket: str, dataset: str, series_id: str) -> str | None:
    """Newest vintage_through partition stored for a series, or None if there is none."""
    prefix = f"{dataset}/series_id={series_id}/vintage_through="
    latest = None
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            value = obj["Key"][len(prefix) :].split("/", 1)[0]
            if latest is None or value > latest:
                latest = value
    return latest


def object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return True


def write_snapshot(
    s3,
    bucket: str,
    dataset: str,
    series_id: str,
    vintage_through: str,
    rows: list[dict[str, str | None]],
    *,
    run_id: str,
    ingested_at: datetime,
    source: str,
) -> tuple[str, bool]:
    """Store rows as a snapshot unless identical content is already there.

    Returns the object key and whether this call wrote it.
    """
    if not rows:
        raise ValueError(f"refusing to write an empty {dataset} snapshot for {series_id}")
    digest = content_digest(rows)
    key = snapshot_key(dataset, series_id, vintage_through, digest)
    if object_exists(s3, bucket, key):
        return key, False
    columns = sorted({name for row in rows for name in row})
    frame = pl.DataFrame(
        {name: [row.get(name) for row in rows] for name in columns},
        schema={name: pl.String for name in columns},
    ).with_columns(
        pl.lit(run_id).alias("_run_id"),
        pl.lit(ingested_at).alias("_ingested_at"),
        pl.lit(digest).alias("_content_sha256"),
        pl.lit(source).alias("_source"),
    )
    buffer = io.BytesIO()
    frame.write_parquet(buffer, compression="zstd")
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    return key, True
