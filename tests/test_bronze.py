import io
from datetime import datetime, timezone

import polars as pl

from fakes import FakeS3
from macro_lake.bronze import OBSERVATIONS, content_digest, latest_vintage_through, snapshot_key, write_snapshot

NOW = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)

ROWS = [
    {"date": "2023-12-01", "value": "100.0", "realtime_start": "2024-01-10", "realtime_end": "2024-02-07"},
    {"date": "2023-12-01", "value": ".", "realtime_start": "2024-02-08", "realtime_end": "9999-12-31"},
]


def write(s3, rows=ROWS, run_id="run-1"):
    return write_snapshot(
        s3, "lake", OBSERVATIONS, "TEST", "2024-02-08", rows, run_id=run_id, ingested_at=NOW, source="test"
    )


def test_digest_ignores_row_and_key_order():
    shuffled = [dict(reversed(list(row.items()))) for row in reversed(ROWS)]
    assert content_digest(shuffled) == content_digest(ROWS)
    assert content_digest([ROWS[0], {**ROWS[1], "value": "101.5"}]) != content_digest(ROWS)


def test_key_uses_hive_partitions_and_the_digest():
    assert snapshot_key(OBSERVATIONS, "GDPC1", "2026-08-26", "ab" * 32) == (
        "bronze/alfred/observations/series_id=GDPC1/vintage_through=2026-08-26/abababababababab.parquet"
    )


def test_writing_identical_content_twice_is_a_no_op():
    s3 = FakeS3()
    first_key, first_created = write(s3)
    second_key, second_created = write(s3, run_id="run-2")
    assert first_key == second_key
    assert (first_created, second_created) == (True, False)
    assert s3.puts == 1


def test_snapshot_keeps_raw_strings_and_lineage():
    s3 = FakeS3()
    key, _ = write(s3)
    frame = pl.read_parquet(io.BytesIO(s3.objects[("lake", key)]))
    assert frame.columns == [
        "date", "realtime_end", "realtime_start", "value", "_run_id", "_ingested_at", "_content_sha256", "_source",
    ]
    assert frame.schema["value"] == pl.String
    assert frame["value"].to_list() == ["100.0", "."]
    assert frame["_content_sha256"][0] == content_digest(ROWS)


def test_latest_vintage_through_reads_partition_names():
    s3 = FakeS3()
    stored = [("TEST", "2024-01-10"), ("TEST", "2024-02-08"), ("TEST", "2023-12-15"), ("TEST2", "2030-01-01")]
    for series_id, vintage in stored:
        s3.put_object(Bucket="lake", Key=f"{OBSERVATIONS}/series_id={series_id}/vintage_through={vintage}/x.parquet", Body=b"")
    assert latest_vintage_through(s3, "lake", OBSERVATIONS, "TEST") == "2024-02-08"
    assert latest_vintage_through(s3, "lake", OBSERVATIONS, "MISSING") is None
