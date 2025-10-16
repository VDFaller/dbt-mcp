from typing import Any
from dbt_mcp.dbt_cli.manifest.lineage_types import Ancestor, Descendant, ModelLineage


# TODO: I think these could just be classmethods of the validators
def get_parent_lineage(
    manifest: dict[str, Any], model_id: str, recursive: bool = False
) -> str:
    parent_map = manifest.get("parent_map", {})
    if not recursive:
        return ModelLineage.model_validate(
            {
                "model_id": model_id,
                "ancestors": [
                    Ancestor.model_validate({"model_id": pid})
                    for pid in parent_map.get(model_id, [])
                ],
            }
        ).model_dump_json()
    # If recursive is True, we need to get all ancestors
    ancestors: list[Ancestor] = []
    visited = set()
    to_visit = parent_map.get(model_id, []).copy()
    while to_visit:
        current = to_visit.pop(0)
        if current not in visited:
            visited.add(current)
            ancestors.append(Ancestor.model_validate({"model_id": current}))
            to_visit.extend(parent_map.get(current, []))
    return ModelLineage.model_validate(
        {"model_id": model_id, "ancestors": ancestors}
    ).model_dump_json()


def get_child_lineage(
    manifest: dict[str, Any], model_id: str, recursive: bool = False
) -> str:
    child_map = manifest.get("child_map", {})
    if not recursive:
        return ModelLineage.model_validate(
            {
                "model_id": model_id,
                "descendants": [
                    Descendant.model_validate({"model_id": cid})
                    for cid in child_map.get(model_id, [])
                ],
            }
        ).model_dump_json()

    # If recursive is True, we need to get all descendants
    descendants: list[Descendant] = []
    visited = set()
    to_visit = child_map.get(model_id, []).copy()
    while to_visit:
        current = to_visit.pop(0)
        if current not in visited:
            visited.add(current)
            descendants.append(Descendant.model_validate({"model_id": current}))
            to_visit.extend(child_map.get(current, []))

    return ModelLineage.model_validate(
        {"model_id": model_id, "descendants": descendants}
    ).model_dump_json()


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
