from datetime import datetime, timezone

import pytest

from fakes import FakeS3
from macro_lake import bronze
from macro_lake.ingest import IngestError, ingest_series

NOW = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)

FIRST_RELEASE = [
    {"date": "2023-12-01", "value": "100.0", "realtime_start": "2024-01-10", "realtime_end": "9999-12-31"},
]
AFTER_REVISION = [
    {"date": "2023-12-01", "value": "100.0", "realtime_start": "2024-01-10", "realtime_end": "2024-02-07"},
    {"date": "2023-12-01", "value": "101.5", "realtime_start": "2024-02-08", "realtime_end": "9999-12-31"},
    {"date": "2024-01-01", "value": "102.0", "realtime_start": "2024-02-08", "realtime_end": "9999-12-31"},
]


class FakeAlfred:
    """Serves one series the way the ALFRED endpoints would and records each call."""

    def __init__(self, observations, vintage_dates, notes="Source: a U.S. government agency."):
        self._observations = observations
        self._vintage_dates = vintage_dates
        self._notes = notes
        self.calls = []

    def latest_vintage_date(self, series_id):
        self.calls.append("latest_vintage_date")
        return max(self._vintage_dates)

    def series(self, series_id):
        self.calls.append("series")
        return {"id": series_id, "notes": self._notes, "popularity": 42}

    def observations_all_vintages(self, series_id):
        self.calls.append("observations_all_vintages")
        return list(self._observations)

    def vintage_dates(self, series_id):
        self.calls.append("vintage_dates")
        return list(self._vintage_dates)


def ingest(alfred, s3, **options):
    return ingest_series(alfred, s3, "lake", "TEST", run_id="run", ingested_at=NOW, **options)


def test_first_pull_writes_metadata_vintage_dates_and_observations():
    s3 = FakeS3()
    result = ingest(FakeAlfred(FIRST_RELEASE, ["2024-01-10"]), s3)
    assert (result.status, result.vintage_through, result.rows, result.vintages) == ("new", "2024-01-10", 1, 1)
    assert result.objects_written == 3
    assert [key.split("/series_id=")[0] for key in s3.keys()] == [bronze.OBSERVATIONS, bronze.VINTAGE_DATES, bronze.SERIES]


def test_rerun_with_no_new_vintage_downloads_nothing():
    s3, alfred = FakeS3(), FakeAlfred(FIRST_RELEASE, ["2024-01-10"])
    ingest(alfred, s3)
    alfred.calls.clear()
    result = ingest(alfred, s3)
    assert (result.status, result.vintage_through) == ("current", "2024-01-10")
    assert alfred.calls == ["latest_vintage_date"]
    assert s3.puts == 3


def test_new_vintage_adds_a_snapshot_beside_the_old_one():
    s3 = FakeS3()
    ingest(FakeAlfred(FIRST_RELEASE, ["2024-01-10"]), s3)
    result = ingest(FakeAlfred(AFTER_REVISION, ["2024-01-10", "2024-02-08"]), s3)
    assert (result.status, result.vintage_through, result.rows, result.objects_written) == ("new", "2024-02-08", 3, 3)
    partitions = {key.split("vintage_through=")[1].split("/")[0] for key in s3.keys() if key.startswith(bronze.OBSERVATIONS)}
    assert partitions == {"2024-01-10", "2024-02-08"}


def test_forced_download_of_unchanged_data_writes_nothing():
    s3, alfred = FakeS3(), FakeAlfred(FIRST_RELEASE, ["2024-01-10"])
    ingest(alfred, s3)
    result = ingest(alfred, s3, force=True)
    assert (result.status, result.objects_written) == ("unchanged", 0)
    assert alfred.calls.count("observations_all_vintages") == 2
    assert s3.puts == 3


def test_series_with_a_copyright_notice_is_refused():
    s3 = FakeS3()
    result = ingest(FakeAlfred(FIRST_RELEASE, ["2024-01-10"], notes="Copyright, 2026, Example Data Vendor."), s3)
    assert result.status == "refused"
    assert s3.puts == 0


def test_lists_ending_on_different_releases_fail_before_writing():
    s3 = FakeS3()
    # The observations already include the 2024-02-08 release; the vintage dates do not.
    with pytest.raises(IngestError, match="does not match"):
        ingest(FakeAlfred(AFTER_REVISION, ["2024-01-10"]), s3)
    assert s3.puts == 0
