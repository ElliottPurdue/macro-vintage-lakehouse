import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterInstance,
    DataVersion,
    Definitions,
    build_asset_context,
    evaluate_automation_conditions,
)

from macro_lake import definitions
from macro_lake.definitions import alfred_snapshots, defs, series_partitions
from macro_lake.ingest import SeriesResult, load_series_ids

ROOT = Path(__file__).resolve().parents[1]
BRONZE = set(alfred_snapshots.keys)
# The tag MaterializeResult(data_version=...) writes.
DATA_VERSION_TAG = "dagster/data_version"


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


def test_every_check_is_versioned_by_the_newest_release_it_holds(monkeypatch):
    monkeypatch.setattr(definitions, "fred_api_key", lambda: "key")
    monkeypatch.setattr(definitions, "load_lake_settings", lambda: SimpleNamespace(bucket="lake"))
    monkeypatch.setattr(definitions, "s3_client", lambda settings: None)
    monkeypatch.setattr(definitions, "FredClient", lambda api_key: SimpleNamespace(requests_made=1))
    current = SeriesResult("GDPC1", "current", "2026-08-26")
    monkeypatch.setattr(definitions, "ingest_series", lambda *args, **kwargs: current)

    results = list(alfred_snapshots(build_asset_context(partition_key="GDPC1")))

    assert {result.asset_key for result in results} == BRONZE
    assert {result.data_version for result in results} == {DataVersion("2026-08-26")}


class SimulatedInstance:
    """An empty instance where every run the automation asks for succeeds at once."""

    def __init__(self):
        self.instance = DagsterInstance.ephemeral()
        self.cursor = None
        self.every_asset = defs.resolve_asset_graph().get_all_asset_keys()

    def morning(self, day: int, newest_release: dict[str, str]) -> set[AssetKey]:
        """Ticks from 9:00 to 9:50 in New York on 2026-09-<day>, returning every asset requested."""
        requested = set()
        start = datetime(2026, 9, day, 13, 0, tzinfo=timezone.utc)
        for minutes in range(0, 60, 10):
            result = evaluate_automation_conditions(
                defs, self.instance, evaluation_time=start + timedelta(minutes=minutes), cursor=self.cursor
            )
            self.cursor = result.cursor
            for key in self.every_asset:
                for partition in result.get_requested_partitions(key):
                    # Bronze carries the data version the real asset reports.
                    tags = {DATA_VERSION_TAG: newest_release[partition]} if key in BRONZE else None
                    self.instance.report_runless_asset_event(AssetMaterialization(key, partition=partition, tags=tags))
                    requested.add(key)
        return requested


def test_the_automation_builds_a_new_instance_all_the_way_to_gold():
    # eager() never builds an asset that was already missing when it was first
    # evaluated, and holds back everything downstream of a missing one, which
    # once left the gold models waiting on a seed nothing was going to build.
    lake = SimulatedInstance()
    releases = dict.fromkeys(series_partitions.get_partition_keys(), "2026-09-18")
    assert lake.morning(21, releases) == lake.every_asset


def test_the_models_rebuild_only_when_a_release_lands():
    lake = SimulatedInstance()
    releases = dict.fromkeys(series_partitions.get_partition_keys(), "2026-09-18")
    lake.morning(21, releases)

    # Tuesday: every series is checked and found current, which rebuilds nothing.
    assert lake.morning(22, releases) == BRONZE

    # Wednesday: one series has a new release, which rebuilds every model.
    releases["ICSA"] = "2026-09-23"
    models = lake.every_asset - BRONZE - {AssetKey("series_measures")}
    assert lake.morning(23, releases) == BRONZE | models
