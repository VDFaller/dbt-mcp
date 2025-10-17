# %%

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator
from typing import Any


class Descendant(BaseModel):
    model_id: str
    children: list[Descendant] = Field(default_factory=list)


class Ancestor(BaseModel):
    model_id: str
    parents: list[Ancestor] = Field(default_factory=list)


class ModelLineage(BaseModel):
    model_id: str
    ancestors: list[Ancestor] | None = Field(default_factory=list)
    descendants: list[Descendant] | None = Field(default_factory=list)

    @field_validator("descendants", mode="after")
    @classmethod
    def _sanitize_descendants(cls, value: list[Descendant] | None) -> list[Descendant]:  # noqa: D401 - simple filter
        def _filter(nodes: list[Descendant]) -> list[Descendant]:
            filtered: list[Descendant] = []
            for node in nodes:
                if node.model_id.startswith(("test.", "unit_test.")):
                    continue
                filtered_children = _filter(node.children)
                filtered.append(node.model_copy(update={"children": filtered_children}))
            return filtered

        if not value:
            return []

        return _filter(value)

    @classmethod
    def from_manifest(
        cls,
        manifest: dict[str, Any],
        model_id: str,
        recursive: bool = False,
        direction: str = "both",
    ) -> ModelLineage:
        """
        Build a ModelLineage instance from a dbt manifest mapping.

        - manifest: dict containing at least 'parent_map' and/or 'child_map'
        - model_id: the model id to start from
        - recursive: whether to traverse recursively
        - direction: one of 'parents', 'children', or 'both'

        The returned ModelLineage contains lists of Ancestor and/or Descendant
        objects. For compatibility with the previous implementation, recursive
        traversal returns a flat list of Ancestor/Descendant nodes (no nested
        parents/children relationships are constructed). Filtering for test
        nodes is left to the validators.
        """
        parent_map = manifest.get("parent_map", {})
        child_map = manifest.get("child_map", {})

        ancestors: list[Ancestor] = []
        descendants: list[Descendant] = []

        if direction in ("both", "parents"):
            if not recursive:
                # direct parents only
                for pid in parent_map.get(model_id, []):
                    ancestors.append(Ancestor.model_validate({"model_id": pid}))
            else:
                # Build nested ancestor trees. We prevent cycles using path tracking.
                def _build_ancestor(node_id: str, path: set[str]) -> Ancestor:
                    if node_id in path:
                        # cycle detected, return node without parents
                        return Ancestor.model_validate({"model_id": node_id})
                    new_path = set(path)
                    new_path.add(node_id)
                    parents = [
                        _build_ancestor(pid, new_path)
                        for pid in parent_map.get(node_id, [])
                    ]
                    return Ancestor.model_validate(
                        {"model_id": node_id, "parents": parents}
                    )

                for pid in parent_map.get(model_id, []):
                    ancestors.append(_build_ancestor(pid, {model_id}))

        if direction in ("both", "children"):
            if not recursive:
                descendants = [
                    Descendant.model_validate({"model_id": cid})
                    for cid in child_map.get(model_id, [])
                ]
            else:
                # Build nested descendant trees. Prevent cycles using path tracking.
                def _build_descendant(node_id: str, path: set[str]) -> Descendant:
                    if node_id in path:
                        return Descendant.model_validate({"model_id": node_id})
                    new_path = set(path)
                    new_path.add(node_id)
                    children = [
                        _build_descendant(cid, new_path)
                        for cid in child_map.get(node_id, [])
                    ]
                    return Descendant.model_validate(
                        {"model_id": node_id, "children": children}
                    )

                for cid in child_map.get(model_id, []):
                    descendants.append(_build_descendant(cid, {model_id}))

        return cls(model_id=model_id, ancestors=ancestors, descendants=descendants)


# %%
if __name__ == "__main__":
    import json

    with open("/home/faller/repos/dbt-mcp/src/dbt_mcp/dbt_cli/manifest.json") as f:
        manifest = json.load(f)
    model_id = "model.jaffle_shop.order_items"
    print("Model ID:", model_id)
    print(
        ModelLineage.from_manifest(
            manifest, model_id, recursive=True, direction="parents"
        ).model_dump()
    )
