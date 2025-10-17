from dbt_mcp.dbt_cli.manifest.lineage_types import ModelLineage


def test_model_lineage__from_manifest():
    manifest = {
        "child_map": {
            "model.a": ["model.b", "model.c"],
            "model.b": ["model.d"],
            "model.c": [],
            "model.d": [],
        },
        "parent_map": {
            "model.b": ["model.a"],
            "model.c": ["model.a"],
            "model.d": ["model.b"],
            "model.a": [],
        },
    }
    lineage = ModelLineage.from_manifest(
        manifest, "model.a", direction="both", recursive=True
    )
    assert lineage.model_id == "model.a"
    assert len(lineage.ancestors) == 0
    assert len(lineage.descendants) == 2
    assert lineage.descendants[0].model_id == "model.b"
    assert lineage.descendants[0].children[0].model_id == "model.d"

    lineage_b = ModelLineage.from_manifest(
        manifest, "model.b", direction="both", recursive=True
    )
    assert lineage_b.model_id == "model.b"
    assert len(lineage_b.ancestors) == 1
