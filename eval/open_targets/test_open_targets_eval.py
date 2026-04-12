"""External validation against the Open Targets human-authored Croissant metadata.

This test generates Croissant metadata for the Open Targets Platform dataset
using Croissant Baker, validates it with mlcroissant, and compares the output
against the human-authored ground truth published by Open Targets.

Requires a one-time download (~20-30 GB):
    bash eval/open_targets/download.sh

The evaluation is automatically skipped when the downloaded data is not present.
"""

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from croissant_baker.__main__ import app

runner = CliRunner()

EVAL_DIR = Path(__file__).resolve().parent
INPUT_DIR = EVAL_DIR
OUTPUT_DIR = EVAL_DIR / "output"
GROUND_TRUTH = EVAL_DIR / "croissant_ground_truth.json"

SEMANTIC_FLOAT = {"Float", "Float32", "Float64"}
SEMANTIC_INT = {"Integer", "Int8", "Int16", "Int32", "Int64"}
SEMANTIC_GROUPS = [SEMANTIC_FLOAT, SEMANTIC_INT]


def _has_parquet_data() -> bool:
    """Check whether at least one dataset subdirectory with Parquet exists."""
    if not INPUT_DIR.exists():
        return False
    for child in INPUT_DIR.iterdir():
        if child.is_dir() and any(child.glob("*.parquet")):
            return True
    return False


def _load_recordsets(path: Path) -> dict[str, dict[str, str]]:
    """Parse Croissant JSON-LD into {recordset_name: {field_name: dataType}}."""
    with open(path) as f:
        data = json.load(f)
    result: dict[str, dict[str, str]] = {}
    for rs in data.get("recordSet", []):
        name = rs.get("name", rs.get("@id", "unknown"))
        fields: dict[str, str] = {}
        for field in rs.get("field", []):
            fname = field.get("name", field.get("@id", "unknown"))
            ftype = field.get("dataType", "unknown")
            fields[fname] = ftype
        result[name] = fields
    return result


def _normalize_type(t: str) -> str:
    if ":" in t:
        return t.split(":", 1)[1]
    return t


def _types_semantically_equal(a: str, b: str) -> bool:
    na, nb = _normalize_type(a), _normalize_type(b)
    if na == nb:
        return True
    for group in SEMANTIC_GROUPS:
        if na in group and nb in group:
            return True
    return False


def _distribution_stats(metadata: dict) -> dict:
    """Count FileObjects, FileSets, and total Parquet files from generated metadata."""
    dist = metadata.get("distribution", [])
    file_objects = [d for d in dist if d.get("@type") == "cr:FileObject"]
    file_sets = [d for d in dist if d.get("@type") == "cr:FileSet"]
    parquet_files = [d for d in file_objects if d.get("name", "").endswith(".parquet")]
    return {
        "file_objects": len(file_objects),
        "file_sets": len(file_sets),
        "parquet_files": len(parquet_files),
    }


def _compare(gt_path: Path, gen_path: Path) -> dict:
    """Compare generated Croissant against ground truth. Returns summary dict."""
    gt = _load_recordsets(gt_path)
    gen = _load_recordsets(gen_path)

    with open(gen_path) as f:
        gen_metadata = json.load(f)
    dist = _distribution_stats(gen_metadata)

    gt_names = set(gt.keys())
    gen_names = set(gen.keys())
    matched = gt_names & gen_names

    total_gt_fields = 0
    total_gen_fields = 0
    total_matched_fields = 0
    total_type_agree = 0
    total_sem_agree = 0

    for rs_name in sorted(matched):
        gt_fields = gt[rs_name]
        gen_fields = gen[rs_name]
        common = set(gt_fields) & set(gen_fields)

        for fname in common:
            gt_type = _normalize_type(gt_fields[fname])
            gen_type = _normalize_type(gen_fields[fname])
            if gt_type == gen_type:
                total_type_agree += 1
                total_sem_agree += 1
            elif _types_semantically_equal(gt_fields[fname], gen_fields[fname]):
                total_sem_agree += 1

        total_gt_fields += len(gt_fields)
        total_gen_fields += len(gen_fields)
        total_matched_fields += len(common)

    return {
        "file_objects": dist["file_objects"],
        "file_sets": dist["file_sets"],
        "parquet_files": dist["parquet_files"],
        "recordsets_gt": len(gt_names),
        "recordsets_gen": len(gen_names),
        "recordsets_matched": len(matched),
        "fields_gt": total_gt_fields,
        "fields_gen": total_gen_fields,
        "fields_matched": total_matched_fields,
        "strict_type_agree": total_type_agree,
        "strict_type_agree_pct": (
            round(100 * total_type_agree / total_matched_fields, 1)
            if total_matched_fields
            else 0
        ),
        "semantic_type_agree": total_sem_agree,
        "semantic_type_agree_pct": (
            round(100 * total_sem_agree / total_matched_fields, 1)
            if total_matched_fields
            else 0
        ),
        "true_mismatches": total_matched_fields - total_sem_agree,
    }


@pytest.mark.skipif(
    not _has_parquet_data(),
    reason="Open Targets Parquet data not downloaded. Run: bash eval/open_targets/download.sh",
)
def test_open_targets_evaluation() -> None:
    """Generate Croissant for Open Targets, validate, and compare to ground truth."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_DIR / "open_targets_croissant.jsonld"

    result = runner.invoke(
        app,
        [
            "-i",
            str(INPUT_DIR),
            "-o",
            str(output_file),
            "--name",
            "Open Targets Platform",
            "--creator",
            "Open Targets",
            "--description",
            "Open Targets Platform dataset",
            "--license",
            "https://platform-docs.opentargets.org/licence",
            "--url",
            "https://platform.opentargets.org",
        ],
    )

    assert result.exit_code == 0, f"Croissant Baker failed:\n{result.stdout}"
    assert output_file.exists(), "Output file was not created"

    with open(output_file) as f:
        metadata = json.load(f)
    assert "recordSet" in metadata, "Generated metadata has no recordSet"
    assert len(metadata["recordSet"]) > 0, "Generated metadata has empty recordSet"

    assert GROUND_TRUTH.exists(), f"Ground truth not found: {GROUND_TRUTH}"
    summary = _compare(GROUND_TRUTH, output_file)

    comparison_file = OUTPUT_DIR / "open_targets_comparison.json"
    with open(comparison_file, "w") as f:
        json.dump(summary, f, indent=2)
        f.write("\n")

    assert summary["recordsets_matched"] == summary["recordsets_gt"], (
        f"RecordSet mismatch: matched {summary['recordsets_matched']} "
        f"of {summary['recordsets_gt']} ground truth RecordSets"
    )

    assert summary["semantic_type_agree_pct"] >= 90.0, (
        f"Semantic type agreement {summary['semantic_type_agree_pct']}% "
        f"is below the 90% threshold"
    )

    assert summary["fields_matched"] == summary["fields_gt"], (
        f"Field coverage: matched {summary['fields_matched']} "
        f"of {summary['fields_gt']} ground truth fields"
    )
