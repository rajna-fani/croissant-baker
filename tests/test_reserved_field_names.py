"""Regression test for JSON-LD node identifier collisions.

This test exercises nested (struct) columns whose sub-fields use names that can
be confused with Croissant/JSON-LD terms (e.g., ``source``). The generator must
produce Croissant metadata that validates and does not contain duplicate node
``@id`` values within the same graph.
"""

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from croissant_baker.__main__ import app

runner = CliRunner()


@pytest.fixture
def output_dir() -> Path:
    output_path = Path(__file__).parent / "data" / "output"
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path


@pytest.fixture
def reserved_name_parquet_path(tmp_path: Path) -> Path:
    """Parquet dataset with struct sub-fields whose names collide with
    Croissant JSON-LD context terms: source, data, field, references, column.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema(
        [
            ("id", pa.string()),
            (
                "metadata",
                pa.struct(
                    [
                        pa.field("source", pa.string()),
                        pa.field("data", pa.string()),
                        pa.field("field", pa.string()),
                        pa.field("references", pa.string()),
                        pa.field("column", pa.string()),
                    ]
                ),
            ),
        ]
    )

    table = pa.table(
        {
            "id": ["row1", "row2"],
            "metadata": [
                {
                    "source": "src_a",
                    "data": "dat_a",
                    "field": "fld_a",
                    "references": "ref_a",
                    "column": "col_a",
                },
                {
                    "source": "src_b",
                    "data": "dat_b",
                    "field": "fld_b",
                    "references": "ref_b",
                    "column": "col_b",
                },
            ],
        },
        schema=schema,
    )

    out = tmp_path / "reserved_names"
    out.mkdir()
    pq.write_table(table, out / "part-00000.parquet")
    return out


def test_reserved_name_struct_fields_validate(
    reserved_name_parquet_path: Path, output_dir: Path
) -> None:
    """Generation + mlcroissant validation must succeed when struct sub-fields
    are named 'source', 'data', 'field', 'references', or 'column'."""
    output_file = output_dir / "reserved_names_croissant.jsonld"

    result = runner.invoke(
        app,
        [
            "-i",
            str(reserved_name_parquet_path),
            "-o",
            str(output_file),
            "--name",
            "Reserved-name regression test",
            "--creator",
            "Test Author",
            "--url",
            "https://example.com/reserved-name-test",
            # Fixed metadata so committed tests/data/output/reserved_names_croissant.jsonld
            # does not change on every run (defaults use datetime.now() and current year).
            "--date-published",
            "2025-01-01",
            "--citation",
            "Test Author. (2025). Reserved-name regression test.",
        ],
    )

    assert result.exit_code == 0, f"Generation failed:\n{result.stdout}"
    assert output_file.exists()

    with open(output_file) as f:
        metadata = json.load(f)

    rs = metadata["recordSet"]
    assert len(rs) == 1

    top_fields = {f["name"] for f in rs[0]["field"]}
    assert "metadata" in top_fields

    meta_field = next(f for f in rs[0]["field"] if f["name"] == "metadata")
    sub_names = {sf["name"] for sf in meta_field["subField"]}
    assert sub_names == {"source", "data", "field", "references", "column"}

    # The actual bug: verify all node @id values in the RecordSet are unique.
    # Only collect from objects that define a node (@type present), not bare
    # JSON-LD references like {"@id": "file_0"}.
    node_ids: list[str] = []

    def _collect_node_ids(obj: object) -> None:
        if isinstance(obj, dict):
            if "@id" in obj and "@type" in obj:
                node_ids.append(obj["@id"])
            for v in obj.values():
                _collect_node_ids(v)
        elif isinstance(obj, list):
            for item in obj:
                _collect_node_ids(item)

    _collect_node_ids(rs[0])
    assert len(node_ids) == len(set(node_ids)), (
        f"Duplicate node @id values found: "
        f"{[x for x in node_ids if node_ids.count(x) > 1]}"
    )
