import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dagster import AssetKey, AssetMaterialization, DagsterInstance, Definitions, evaluate_automation_conditions

from macro_lake.definitions import alfred_snapshots, defs, series_partitions
from macro_lake.ingest import load_series_ids

ROOT = Path(__file__).resolve().parents[1]


def parents_of(name: str) -> set[AssetKey]:
    return defs.resolve_asset_graph().get(AssetKey(name)).parent_keys


def test_definitions_are_loadable():
    Definitions.validate_loadable(defs)


def test_the_duckdb_path_cannot_depend_on_the_working_directory():
    # A relative path gives a shell and a Dagster run two different databases,
    # which showed up as a scheduled run whose models could not see the seed.
    assert Path(os.environ["LAKE_DUCKDB_PATH"]).is_absolute()


def test_every_configured_series_is_a_partition():
    assert list(series_partitions.get_partition_keys()) == load_series_ids(ROOT / "config" / "series.toml")


def test_staging_models_read_the_bronze_assets():
    assert AssetKey(["bronze", "alfred_observations"]) in parents_of("stg_alfred__observation_snapshots")
    assert AssetKey(["bronze", "alfred_vintage_dates"]) in parents_of("stg_alfred__vintage_dates")
    assert AssetKey(["bronze", "fred_series"]) in parents_of("stg_fred__series")


def test_ingestion_writes_exactly_the_assets_the_dbt_sources_read():
    # The parent keys come from the dbt manifest and the produced keys from the
    # Python asset, so this is what keeps the two halves of the graph joined.
    consumed = set()
    for model in ("stg_alfred__observation_snapshots", "stg_alfred__vintage_dates", "stg_fred__series"):
        consumed |= set(parents_of(model))
    assert set(alfred_snapshots.keys) == consumed


def test_gold_is_built_from_silver():
    assert AssetKey("observation_versions") in parents_of("release_revisions")
    assert AssetKey("release_revisions") in parents_of("series_revision_summary")


def test_the_automation_builds_a_new_instance_all_the_way_to_gold():
    # Walk an empty instance through a weekday morning, recording whatever the
    # automation asks for as materialized, the way a successful run would.
    # eager() never builds an asset that was already missing when it was first
    # evaluated, and holds back everything downstream of a missing one, which
    # once left the gold models waiting on a seed nothing was going to build.
    instance = DagsterInstance.ephemeral()
    every_asset = defs.resolve_asset_graph().get_all_asset_keys()
    requested, cursor = set(), None
    tick = datetime(2026, 9, 21, 13, 0, tzinfo=timezone.utc)  # 9:00 in New York
    for _ in range(6):
        result = evaluate_automation_conditions(defs, instance, evaluation_time=tick, cursor=cursor)
        cursor = result.cursor
        for key in every_asset:
            for partition in result.get_requested_partitions(key):
                instance.report_runless_asset_event(AssetMaterialization(key, partition=partition))
                requested.add(key)
        tick += timedelta(minutes=10)
    assert requested == every_asset
