"""Write a small synthetic bronze lake, so the models can run without a FRED key.

    python tools/make_fixture_lake.py

The values are invented. Their shape is what matters: real series ids and
frequencies, several vintages per series, revisions, and periods published as
'.'. CI builds and tests the dbt models against this, and it is the quickest
way to try the project without an API key.

Everything goes through the normal bronze writer, using whatever LAKE_S3_*
settings are in the environment, so point LAKE_S3_BUCKET at a scratch bucket
if you don't want fixture data sitting next to real data.
"""

import sys
from datetime import date, datetime, timedelta, timezone

from botocore.exceptions import ClientError

from macro_lake import bronze
from macro_lake.settings import load_lake_settings
from macro_lake.storage import s3_client

OPEN_ENDED = "9999-12-31"
RUN_ID = "fixture"

# Each series is written three times, in the shapes a real lake accumulates: the
# complete pull, a full pull taken the day before its newest release, and a pull
# as of this date. The history check has to accept the newest release revising
# what the earlier full pull showed as current, which it once did not.
FIXTURE_AS_OF = "2024-06-28"


def add_months(day: date, months: int) -> date:
    month = day.month - 1 + months
    return date(day.year + month // 12, month % 12 + 1, 1)


def quarterly(start: date, count: int) -> list[date]:
    return [add_months(start, 3 * index) for index in range(count)]


def monthly(start: date, count: int) -> list[date]:
    return [add_months(start, index) for index in range(count)]


def weekly(start: date, count: int) -> list[date]:
    return [start + timedelta(days=7 * index) for index in range(count)]


SERIES = [
    {
        "series_id": "GDPC1",
        "title": "Real Gross Domestic Product",
        "frequency": "Quarterly",
        "frequency_short": "Q",
        "units": "Billions of Chained 2017 Dollars",
        "units_short": "Bil. of Chn. 2017 $",
        "seasonal_adjustment": "Seasonally Adjusted Annual Rate",
        "seasonal_adjustment_short": "SAAR",
        "periods": quarterly(date(2010, 1, 1), 60),
        # A quarter is first published about a month after it ends.
        "first_release": lambda period: add_months(period, 3) + timedelta(days=29),
        "base": 16000.0,
        "step": 120.0,
        "wiggle": 8.0,
        "revision": 14.5,
    },
    {
        "series_id": "PAYEMS",
        "title": "All Employees, Total Nonfarm",
        "frequency": "Monthly",
        "frequency_short": "M",
        "units": "Thousands of Persons",
        "units_short": "Thous. of Persons",
        "seasonal_adjustment": "Seasonally Adjusted",
        "seasonal_adjustment_short": "SA",
        "periods": monthly(date(2010, 1, 1), 180),
        "first_release": lambda period: add_months(period, 1) + timedelta(days=5),
        "base": 130000.0,
        "step": 180.0,
        "wiggle": 22.0,
        "revision": 31.0,
    },
    {
        "series_id": "ICSA",
        "title": "Initial Claims",
        "frequency": "Weekly, Ending Saturday",
        "frequency_short": "W",
        "units": "Number",
        "units_short": "Number",
        "seasonal_adjustment": "Seasonally Adjusted",
        "seasonal_adjustment_short": "SA",
        "periods": weekly(date(2019, 1, 5), 313),
        "first_release": lambda period: period + timedelta(days=5),
        "base": 220000.0,
        "step": -90.0,
        "wiggle": 3500.0,
        "revision": 1250.0,
    },
]


def release_dates(spec: dict, period: date, index: int) -> list[date]:
    """When this period was first published, and when it was revised after that."""
    first = spec["first_release"](period)
    dates = [first]
    if index % 3 == 0:
        dates.append(first + timedelta(days=30))
    if index % 5 == 0:
        dates.append(date(first.year + 1, 7, 30))
    return dates


def value_for(spec: dict, index: int, version: int) -> float:
    drift = spec["base"] + spec["step"] * index
    wiggle = ((index * 37) % 11 - 5) * spec["wiggle"]
    return drift + wiggle + version * spec["revision"]


def observation_rows(spec: dict) -> list[dict[str, str]]:
    rows = []
    for index, period in enumerate(spec["periods"]):
        dates = release_dates(spec, period, index)
        for version, start in enumerate(dates):
            last = version == len(dates) - 1
            # Every so often a period is withdrawn for a while and then comes back.
            withdrawn = index % 17 == 0 and len(dates) >= 3 and version == 1
            rows.append(
                {
                    "date": period.isoformat(),
                    "value": "." if withdrawn else f"{value_for(spec, index, version):.1f}",
                    "realtime_start": start.isoformat(),
                    "realtime_end": OPEN_ENDED if last else (dates[version + 1] - timedelta(days=1)).isoformat(),
                }
            )
    return rows


def as_of_rows(rows: list[dict[str, str]], as_of: str) -> list[dict[str, str]]:
    """The rows as ALFRED would have reported them on as_of, with current periods clipped to it."""
    return [
        {**row, "realtime_end": min(row["realtime_end"], as_of)}
        for row in rows
        if row["realtime_start"] <= as_of
    ]


def full_pull_rows(rows: list[dict[str, str]], day: str) -> list[dict[str, str]]:
    """The rows as a full pull taken on day would have returned them, with what was current then open ended."""
    return [
        {**row, "realtime_end": OPEN_ENDED if row["realtime_end"] >= day else row["realtime_end"]}
        for row in rows
        if row["realtime_start"] <= day
    ]


def series_row(spec: dict, vintage_through: str) -> dict[str, str]:
    return {
        "id": spec["series_id"],
        "title": spec["title"],
        "frequency": spec["frequency"],
        "frequency_short": spec["frequency_short"],
        "units": spec["units"],
        "units_short": spec["units_short"],
        "seasonal_adjustment": spec["seasonal_adjustment"],
        "seasonal_adjustment_short": spec["seasonal_adjustment_short"],
        "observation_start": spec["periods"][0].isoformat(),
        "observation_end": spec["periods"][-1].isoformat(),
        "realtime_start": vintage_through,
        "realtime_end": OPEN_ENDED,
        "last_updated": f"{vintage_through} 08:30:00-05",
        "popularity": "50",
        "notes": "Synthetic fixture data. The values are invented and are not from FRED.",
    }


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)


def main() -> int:
    settings = load_lake_settings()
    s3 = s3_client(settings)
    ensure_bucket(s3, settings.bucket)
    ingested_at = datetime.now(timezone.utc)

    written = 0
    for spec in SERIES:
        every_version = observation_rows(spec)
        newest_release = date.fromisoformat(max(row["realtime_start"] for row in every_version))
        day_before = (newest_release - timedelta(days=1)).isoformat()
        pulls = [
            ("current", OPEN_ENDED, every_version),
            (f"full on {day_before}", OPEN_ENDED, full_pull_rows(every_version, day_before)),
            (f"as of {FIXTURE_AS_OF}", FIXTURE_AS_OF, as_of_rows(every_version, FIXTURE_AS_OF)),
        ]
        for label, as_of, rows in pulls:
            vintages = sorted({row["realtime_start"] for row in rows})
            vintage_through = vintages[-1]
            snapshots = [
                (bronze.SERIES, [series_row(spec, vintage_through)]),
                (bronze.VINTAGE_DATES, [{"vintage_date": vintage} for vintage in vintages]),
                (bronze.OBSERVATIONS, rows),
            ]
            for dataset, dataset_rows in snapshots:
                _, created = bronze.write_snapshot(
                    s3,
                    settings.bucket,
                    dataset,
                    spec["series_id"],
                    vintage_through,
                    dataset_rows,
                    run_id=RUN_ID,
                    ingested_at=ingested_at,
                    source="synthetic fixture",
                    as_of=as_of,
                )
                written += created
            print(
                f"{spec['series_id']:<8} {label:<24} {len(rows):>5} versions "
                f"{len(vintages):>4} vintage dates through {vintage_through}"
            )
    print(f"{written} objects written to s3://{settings.bucket}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
