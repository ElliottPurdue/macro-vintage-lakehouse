"""Dagster definitions: the bronze ingestion assets, the dbt models, and when they run.

Start the UI from the repository root with `dagster dev`. Every series is a
partition of the bronze assets, so one series can be rerun or backfilled on its
own, and the dbt models rebuild as soon as a series brings in new data.
"""

import os
import shutil
import sys
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    AutomationCondition,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
    Failure,
    MaterializeResult,
    StaticPartitionsDefinition,
    asset_check,
    define_asset_job,
    multi_asset,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from macro_lake import bronze
from macro_lake.fred import FredClient
from macro_lake.ingest import ingest_series, load_series_ids
from macro_lake.settings import fred_api_key, load_env, load_lake_settings
from macro_lake.storage import duckdb_connect, s3_client

# src/macro_lake/definitions.py -> the repository root
REPO_ROOT = Path(__file__).resolve().parents[2]

load_env()

SERIES_IDS = load_series_ids(REPO_ROOT / "config" / "series.toml")
series_partitions = StaticPartitionsDefinition(SERIES_IDS)

# Weekday mornings, after the 8:30 Eastern releases most of these series follow.
INGEST_CRON = "15 9 * * 1-5"

# Asset keys that match the dbt sources in dbt/models/sources.yml, which is
# what joins the ingestion to the models in one graph.
BRONZE_KEYS = {
    bronze.OBSERVATIONS: AssetKey(["bronze", "alfred_observations"]),
    bronze.VINTAGE_DATES: AssetKey(["bronze", "alfred_vintage_dates"]),
    bronze.SERIES: AssetKey(["bronze", "fred_series"]),
}

BRONZE_DESCRIPTIONS = {
    bronze.OBSERVATIONS: "Every value the series has published, with the real-time period it was current.",
    bronze.VINTAGE_DATES: "Every date on which the series was revised or new values released.",
    bronze.SERIES: "FRED metadata for the series as of the snapshot.",
}


@multi_asset(
    name="alfred_snapshots",
    specs=[
        AssetSpec(
            key=key,
            description=BRONZE_DESCRIPTIONS[dataset],
            automation_condition=AutomationCondition.on_cron(INGEST_CRON, cron_timezone="America/New_York"),
            kinds={"python", "parquet"},
        )
        for dataset, key in BRONZE_KEYS.items()
    ],
    partitions_def=series_partitions,
    # One pool for everything that calls FRED, so a backfill of every series
    # doesn't turn into twenty clients hitting a free API at once.
    pool="fred_api",
)
def alfred_snapshots(context: AssetExecutionContext) -> Iterator[MaterializeResult]:
    """Pull one series into bronze, unless bronze already holds its newest vintage."""
    series_id = context.partition_key
    api_key = fred_api_key()
    if api_key is None:
        raise Failure("FRED_API_KEY is not set. Add it to .env.")

    settings = load_lake_settings()
    client = FredClient(api_key)
    result = ingest_series(
        client,
        s3_client(settings),
        settings.bucket,
        series_id,
        run_id=context.run_id,
        ingested_at=datetime.now(timezone.utc),
    )
    context.log.info("%s: %s %s", series_id, result.status, result.detail)

    common = {
        "status": result.status,
        "vintage_through": result.vintage_through or "",
        "objects_written": result.objects_written,
        "api_requests": client.requests_made,
    }
    yield MaterializeResult(asset_key=BRONZE_KEYS[bronze.OBSERVATIONS], metadata={**common, "rows": result.rows})
    yield MaterializeResult(
        asset_key=BRONZE_KEYS[bronze.VINTAGE_DATES], metadata={**common, "vintage_dates": result.vintages}
    )
    yield MaterializeResult(asset_key=BRONZE_KEYS[bronze.SERIES], metadata=common)


@asset_check(
    asset=BRONZE_KEYS[bronze.OBSERVATIONS],
    name="snapshots_match_their_partition",
    description="The newest release date inside a snapshot must equal the vintage_through it is filed under.",
)
def snapshots_match_their_partition() -> AssetCheckResult:
    settings = load_lake_settings()
    observations = f"read_parquet('{settings.uri('bronze/alfred/observations/*/*/*.parquet')}', hive_partitioning = true)"
    connection = duckdb_connect(settings)
    try:
        snapshots = connection.execute(
            f"select count(*) from (select distinct series_id, vintage_through from {observations})"
        ).fetchone()[0]
        mismatched = connection.execute(
            f"""
            select series_id, vintage_through, max(realtime_start) as newest_release
            from {observations}
            group by all
            having cast(max(realtime_start) as date) <> vintage_through
            """
        ).fetchall()
    finally:
        connection.close()
    return AssetCheckResult(
        passed=not mismatched,
        metadata={
            "snapshots": snapshots,
            "mismatched": len(mismatched),
            "examples": str(mismatched[:3]),
        },
    )


def dbt_executable() -> str:
    """dbt from this interpreter's own environment, so PATH doesn't have to be set up."""
    alongside = Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt")
    return str(alongside) if alongside.exists() else (shutil.which("dbt") or "dbt")


dbt_project = DbtProject(project_dir=REPO_ROOT / "dbt", profiles_dir=REPO_ROOT / "dbt")
dbt_project.prepare_if_dev()


class LakeDbtTranslator(DagsterDbtTranslator):
    def get_automation_condition(self, dbt_resource_props: dict) -> AutomationCondition:
        # Rebuild a model as soon as the data it reads has been updated.
        return AutomationCondition.eager()


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=LakeDbtTranslator())
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource) -> Iterator:
    """The silver and gold models, with every dbt test reported as an asset check."""
    yield from dbt.cli(["build"], context=context).stream()


# Every series in one job, so the whole set can be backfilled from the UI or
# with `dagster job backfill --job ingest_all_series --all`.
ingest_all_series = define_asset_job(
    "ingest_all_series",
    selection=AssetSelection.assets(alfred_snapshots),
    description="Pull every configured series into bronze, one run per series.",
)


defs = Definitions(
    assets=[alfred_snapshots, dbt_models],
    asset_checks=[snapshots_match_their_partition],
    jobs=[ingest_all_series],
    resources={"dbt": DbtCliResource(project_dir=dbt_project, dbt_executable=dbt_executable())},
    sensors=[
        AutomationConditionSensorDefinition(
            "automation",
            target=AssetSelection.all(),
            default_status=DefaultSensorStatus.RUNNING,
        )
    ],
)
