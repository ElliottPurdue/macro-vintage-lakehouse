from pathlib import Path

from dagster import AssetKey, Definitions

from macro_lake.definitions import alfred_snapshots, defs, series_partitions
from macro_lake.ingest import load_series_ids

ROOT = Path(__file__).resolve().parents[1]


def parents_of(name: str) -> set[AssetKey]:
    return defs.resolve_asset_graph().get(AssetKey(name)).parent_keys


def test_definitions_are_loadable():
    Definitions.validate_loadable(defs)


def test_every_configured_series_is_a_partition():
    assert list(series_partitions.get_partition_keys()) == load_series_ids(ROOT / "config" / "series.toml")


def test_staging_models_read_the_bronze_assets():
    assert AssetKey(["bronze", "alfred_observations"]) in parents_of("stg_alfred__observations")
    assert AssetKey(["bronze", "alfred_vintage_dates"]) in parents_of("stg_alfred__vintage_dates")
    assert AssetKey(["bronze", "fred_series"]) in parents_of("stg_fred__series")


def test_ingestion_writes_exactly_the_assets_the_dbt_sources_read():
    # The parent keys come from the dbt manifest and the produced keys from the
    # Python asset, so this is what keeps the two halves of the graph joined.
    consumed = set()
    for model in ("stg_alfred__observations", "stg_alfred__vintage_dates", "stg_fred__series"):
        consumed |= set(parents_of(model))
    assert set(alfred_snapshots.keys) == consumed


def test_gold_is_built_from_silver():
    assert AssetKey("observation_versions") in parents_of("release_revisions")
    assert AssetKey("release_revisions") in parents_of("series_revision_summary")
