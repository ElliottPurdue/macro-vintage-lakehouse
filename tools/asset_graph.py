"""Print the asset graph as a Mermaid diagram, so the README can show the lineage.

    python tools/asset_graph.py

The diagram is generated from the Dagster definitions, which take the model side
from the dbt manifest, so it cannot drift from what actually runs.
"""

import re
import sys
from collections import defaultdict

from macro_lake.definitions import defs

LAYERS = [
    ("bronze", "bronze, written by the ingestion"),
    ("staging", "staging"),
    ("silver", "silver"),
    ("gold", "gold"),
    ("checks", "checks"),
    ("seeds", "seeds"),
]


def node_id(key) -> str:
    return re.sub(r"[^0-9a-zA-Z]", "_", "/".join(key.path))


def layer_of(key, group_name: str | None) -> str:
    if key.path[0] == "bronze":
        return "bronze"
    name = key.path[-1]
    if name.startswith("stg_"):
        return "staging"
    if name == "observation_versions":
        return "silver"
    if name == "history_rewrites":
        return "checks"
    if name == "series_measures":
        return "seeds"
    return "gold"


def main() -> int:
    graph = defs.resolve_asset_graph()
    keys = sorted(graph.get_all_asset_keys(), key=lambda key: key.path)
    grouped = defaultdict(list)
    for key in keys:
        grouped[layer_of(key, graph.get(key).group_name)].append(key)

    print("flowchart LR")
    for layer, label in LAYERS:
        if not grouped[layer]:
            continue
        print(f"    subgraph {layer}[{label}]")
        for key in grouped[layer]:
            print(f'        {node_id(key)}["{"/".join(key.path)}"]')
        print("    end")
    for key in keys:
        for parent in sorted(graph.get(key).parent_keys, key=lambda parent: parent.path):
            print(f"    {node_id(parent)} --> {node_id(key)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
