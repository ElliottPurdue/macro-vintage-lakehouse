"""Pull every vintage of the configured series from ALFRED into the bronze layer.

    python -m macro_lake.ingest                 # every series in config/series.toml
    python -m macro_lake.ingest GDPC1 PAYEMS    # only these
    python -m macro_lake.ingest --force GDPC1   # download even if bronze is current

A series is downloaded only when ALFRED lists a vintage newer than the newest
bronze snapshot, so a rerun with nothing new costs one request per series and
writes nothing. Snapshots are content-addressed, so a forced download of
unchanged data writes nothing either.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import tomllib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from macro_lake import bronze
from macro_lake.fred import ALL_VINTAGES, FredClient
from macro_lake.settings import fred_api_key, load_lake_settings
from macro_lake.storage import s3_client

DEFAULT_CONFIG = Path("config/series.toml")
STATUSES = ("new", "current", "unchanged", "refused", "failed")

log = logging.getLogger("macro_lake.ingest")


class IngestError(RuntimeError):
    pass


@dataclass(frozen=True)
class SeriesResult:
    series_id: str
    status: str  # one of STATUSES
    vintage_through: str | None = None
    rows: int = 0
    vintages: int = 0
    objects_written: int = 0
    detail: str = ""


def ingest_series(
    client,
    s3,
    bucket: str,
    series_id: str,
    *,
    run_id: str,
    ingested_at: datetime,
    force: bool = False,
) -> SeriesResult:
    latest = client.latest_vintage_date(series_id)
    stored = bronze.latest_vintage_through(s3, bucket, bronze.OBSERVATIONS, series_id)
    if stored is not None and stored >= latest and not force:
        detail = "" if stored == latest else f"bronze holds {stored}, newer than ALFRED's latest {latest}"
        return SeriesResult(series_id, "current", stored, detail=detail)

    info = {name: None if value is None else str(value) for name, value in client.series(series_id).items()}
    if "copyright" in (info.get("notes") or "").lower():
        return SeriesResult(series_id, "refused", detail="FRED notes carry a copyright notice")

    observations = client.observations_all_vintages(series_id)
    vintage_dates = client.vintage_dates(series_id)
    if not observations or not vintage_dates:
        raise IngestError(f"got {len(observations)} observations and {len(vintage_dates)} vintage dates")
    newest_vintage = max(vintage_dates)
    newest_start = max(row["realtime_start"] for row in observations)
    if newest_start != newest_vintage:
        # Both lists should end on the same release. If they don't, one was
        # fetched before a new release and the other after it; rerun later.
        raise IngestError(f"newest realtime_start {newest_start} does not match newest vintage date {newest_vintage}")

    window = f"realtime_start={ALL_VINTAGES['realtime_start']}&realtime_end={ALL_VINTAGES['realtime_end']}"
    snapshots = [
        (bronze.SERIES, [info], f"fred/series?series_id={series_id}"),
        (
            bronze.VINTAGE_DATES,
            [{"vintage_date": vintage} for vintage in vintage_dates],
            f"fred/series/vintagedates?series_id={series_id}&{window}",
        ),
        # Observations go last: their partition is what marks a series as current.
        (bronze.OBSERVATIONS, observations, f"fred/series/observations?series_id={series_id}&{window}"),
    ]
    created = {}
    for dataset, rows, source in snapshots:
        _, created[dataset] = bronze.write_snapshot(
            s3, bucket, dataset, series_id, newest_vintage, rows, run_id=run_id, ingested_at=ingested_at, source=source
        )
    return SeriesResult(
        series_id,
        "new" if created[bronze.OBSERVATIONS] else "unchanged",
        newest_vintage,
        rows=len(observations),
        vintages=len(vintage_dates),
        objects_written=sum(created.values()),
    )


def load_series_ids(path: Path) -> list[str]:
    with path.open("rb") as handle:
        series_ids = tomllib.load(handle)["series"]
    duplicates = sorted({series_id for series_id in series_ids if series_ids.count(series_id) > 1})
    if duplicates:
        raise ValueError(f"{path}: duplicate series ids {duplicates}")
    return series_ids


def print_summary(results: list[SeriesResult], requests_made: int, seconds: float) -> None:
    print(f"\n{'series':<16} {'status':<10} {'vintage_through':<16} {'rows':>7} {'vintages':>9} {'objects':>8}")
    for r in results:
        downloaded = r.status in ("new", "unchanged")
        rows = str(r.rows) if downloaded else "-"
        vintages = str(r.vintages) if downloaded else "-"
        line = f"{r.series_id:<16} {r.status:<10} {r.vintage_through or '-':<16} {rows:>7} {vintages:>9} {r.objects_written:>8}  {r.detail}"
        print(line.rstrip())
    counts = ", ".join(f"{sum(r.status == status for r in results)} {status}" for status in STATUSES)
    print(f"\n{len(results)} series: {counts}. {requests_made} API requests in {seconds:.1f}s.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pull every ALFRED vintage of the configured series into bronze.")
    parser.add_argument("series", nargs="*", help="series ids to pull (default: every series in the config)")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="TOML file listing series ids")
    parser.add_argument("--force", action="store_true", help="download even when bronze already has the latest vintage")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    api_key = fred_api_key()
    if api_key is None:
        print("FRED_API_KEY is not set. Add it to .env.", file=sys.stderr)
        return 2
    settings = load_lake_settings()
    series_ids = args.series or load_series_ids(args.config)
    client = FredClient(api_key)
    s3 = s3_client(settings)
    ingested_at = datetime.now(timezone.utc)
    run_id = f"{ingested_at:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
    log.info("run %s: %d series into s3://%s", run_id, len(series_ids), settings.bucket)

    started = time.monotonic()
    results = []
    for series_id in series_ids:
        try:
            result = ingest_series(
                client, s3, settings.bucket, series_id, run_id=run_id, ingested_at=ingested_at, force=args.force
            )
        except Exception as exc:  # one failing series should not stop the rest
            result = SeriesResult(series_id, "failed", detail=str(exc))
        log.info("%s: %s %s", series_id, result.status, result.detail)
        results.append(result)

    print_summary(results, client.requests_made, time.monotonic() - started)
    return 1 if any(r.status == "failed" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
