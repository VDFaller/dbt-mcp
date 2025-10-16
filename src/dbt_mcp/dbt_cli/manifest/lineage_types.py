# %%

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


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


# %%
