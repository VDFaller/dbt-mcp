from typing import Any
from dbt_mcp.dbt_cli.manifest.lineage_types import ModelLineage


def get_parent_lineage(
    manifest: dict[str, Any], model_id: str, recursive: bool = False
) -> str:
    """Return a JSON dump of parent lineage for model_id using ModelLineage factory."""
    ml = ModelLineage.from_manifest(
        manifest, model_id, recursive=recursive, direction="parents"
    )
    return ml.model_dump_json()


def get_child_lineage(
    manifest: dict[str, Any], model_id: str, recursive: bool = False
) -> str:
    """Return a JSON dump of child lineage for model_id using ModelLineage factory."""
    ml = ModelLineage.from_manifest(
        manifest, model_id, recursive=recursive, direction="children"
    )
    return ml.model_dump_json()


# %%
if __name__ == "__main__":
    import json

    with open("/home/faller/repos/dbt-mcp/src/dbt_mcp/dbt_cli/manifest.json") as f:
        manifest = json.load(f)
    model_id = "model.jaffle_shop.stg_customers"
    print("Model ID:", model_id)
    print(get_parent_lineage(manifest, model_id, recursive=True))
    print(get_child_lineage(manifest, model_id, recursive=True))

    model_id = "model.jaffle_shop.order_items"
    print("Model ID:", model_id)
    print(get_parent_lineage(manifest, model_id, recursive=False))
    print(get_child_lineage(manifest, model_id, recursive=False))

# %%
