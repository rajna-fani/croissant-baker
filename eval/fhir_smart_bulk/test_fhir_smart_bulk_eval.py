"""Out-of-distribution evaluation: FHIR NDJSON bulk format (SMART Health IT).

Croissant Baker is run against 10-patient synthetic FHIR NDJSON bulk export data
from the SMART Health IT project (Boston Children's Hospital / Harvard), exercising
the NDJSON bulk code path against non-MIMIC FHIR schemas with standard resource
naming (no Mimic-prefixed types).

Source: https://github.com/smart-on-fhir/sample-bulk-fhir-datasets (10-patients branch)
Format: NDJSON bulk (one resource type per .ndjson file, FHIR Bulk Data spec)
License: CC0

Requires a one-time download (~8.7 MB):
    bash eval/fhir_smart_bulk/download.sh

Evaluation is automatically skipped when the downloaded data is not present.
"""

import json
import time
from pathlib import Path

import pytest
from typer.testing import CliRunner

from croissant_baker.__main__ import app
from eval.fhir_smart_bulk.standards_eval import (
    build_standards_ground_truth,
    compare_against_generated,
    flatten_generated_recordsets,
)

runner = CliRunner()

EVAL_DIR = Path(__file__).resolve().parent
INPUT_DIR = EVAL_DIR / "ndjson"
OUTPUT_DIR = EVAL_DIR / "output"
OUTPUT_FILE = OUTPUT_DIR / "smart_bulk_croissant.jsonld"
COMPARISON_FILE = OUTPUT_DIR / "smart_bulk_standards_comparison.json"
GROUND_TRUTH_FILE = OUTPUT_DIR / "smart_bulk_standards_ground_truth.json"
STANDARDS_DIR = EVAL_DIR / "standards"

EXPECTED_RESOURCE_TYPES = {
    "AllergyIntolerance",
    "Condition",
    "Device",
    "DiagnosticReport",
    "DocumentReference",
    "EpisodeOfCare",
    "Encounter",
    "Immunization",
    "Location",
    "MedicationRequest",
    "Observation",
    "Organization",
    "Patient",
    "Practitioner",
    "PractitionerRole",
    "Procedure",
    "ServiceRequest",
    "Specimen",
}


def _has_ndjson_data() -> bool:
    return INPUT_DIR.exists() and any(INPUT_DIR.glob("*.ndjson"))


def _has_standards_packages() -> bool:
    return (
        (STANDARDS_DIR / "us_core" / "package").exists()
        and (STANDARDS_DIR / "fhir_r4_core" / "package").exists()
    )


@pytest.mark.skipif(
    not (_has_ndjson_data() and _has_standards_packages()),
    reason=(
        "SMART bulk FHIR NDJSON data or HL7 standards packages not downloaded. "
        "Run: bash eval/fhir_smart_bulk/download.sh"
    ),
)
def test_fhir_smart_bulk_evaluation() -> None:
    """Generate Croissant and compare it to official HL7 standards ground truth."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    start = time.perf_counter()
    result = runner.invoke(
        app,
        [
            "-i", str(INPUT_DIR),
            "-o", str(OUTPUT_FILE),
            "--name", "SMART on FHIR Bulk Sample (10 patients)",
            "--creator", "SMART Health IT",
            "--description", "Synthetic FHIR NDJSON bulk export sample data",
            "--license", "https://creativecommons.org/publicdomain/zero/1.0/",
        ],
    )
    elapsed = time.perf_counter() - start

    assert result.exit_code == 0, f"Croissant Baker failed:\n{result.stdout}"
    assert OUTPUT_FILE.exists(), "Output file was not created"

    with open(OUTPUT_FILE) as f:
        metadata = json.load(f)

    record_sets = metadata.get("recordSet", [])
    assert len(record_sets) > 0, "No RecordSets generated"

    # Each NDJSON file maps to one RecordSet (one per resource type)
    generated_names = {rs.get("name", rs.get("@id", "")) for rs in record_sets}
    assert EXPECTED_RESOURCE_TYPES == generated_names, (
        f"Resource type mismatch.\n"
        f"  Expected: {sorted(EXPECTED_RESOURCE_TYPES)}\n"
        f"  Got:      {sorted(generated_names)}"
    )

    # Verify field counts are non-trivial for key resource types
    rs_by_name = {rs.get("name", rs.get("@id", "")): rs for rs in record_sets}
    for rtype in ("Patient", "Observation", "Encounter"):
        fields = rs_by_name[rtype].get("field", [])
        assert len(fields) >= 4, f"{rtype} has only {len(fields)} fields — unexpectedly sparse"

    # Verify NDJSON chunk merging: Observation may have multiple chunk files
    # (Observation.000.ndjson, Observation.001.ndjson) → single RecordSet
    obs_rs = rs_by_name.get("Observation")
    assert obs_rs is not None, "Observation RecordSet not found"

    # Summary for paper
    total_fields = sum(len(rs.get("field", [])) for rs in record_sets)
    print(f"\n=== SMART Bulk NDJSON Evaluation ===")
    print(f"  RecordSets: {len(record_sets)} ({', '.join(sorted(generated_names))})")
    print(f"  Total fields: {total_fields}")
    print(f"  Generation time: {elapsed:.2f}s")
    for rs in sorted(record_sets, key=lambda r: r.get("name", "")):
        n = len(rs.get("field", []))
        print(f"    {rs.get('name','?'):30s} {n:3d} fields")

    ground_truth, unresolved, profiles_by_type = build_standards_ground_truth(
        INPUT_DIR,
        EVAL_DIR,
    )
    generated = flatten_generated_recordsets(OUTPUT_FILE)
    comparison = compare_against_generated(
        ground_truth,
        generated,
        unresolved,
        profiles_by_type,
    )

    with open(GROUND_TRUTH_FILE, "w") as f:
        json.dump(ground_truth, f, indent=2, sort_keys=True)
        f.write("\n")

    with open(COMPARISON_FILE, "w") as f:
        json.dump(comparison, f, indent=2, sort_keys=True)
        f.write("\n")

    print("\n=== Standards-Grounded Comparison ===")
    print(
        "  RecordSets matched:"
        f" {comparison['recordsets_matched']} / {comparison['recordsets_gt']}"
    )
    print(
        "  Leaf fields matched:"
        f" {comparison['fields_matched']} / {comparison['fields_gt']}"
    )
    print(
        "  Strict type agreement:"
        f" {comparison['strict_type_agree']} / {comparison['fields_matched']}"
        f" ({comparison['strict_type_agree_pct']:.1f}%)"
    )
    print(
        "  Semantic type agreement:"
        f" {comparison['semantic_type_agree']} / {comparison['fields_matched']}"
        f" ({comparison['semantic_type_agree_pct']:.1f}%)"
    )
    print(f"  Unresolved standards paths: {comparison['unresolved_path_count']}")
    if comparison["missing_fields"]:
        print(f"  Missing generated fields: {len(comparison['missing_fields'])}")
    if comparison["mismatches"]:
        print(f"  Type mismatches: {len(comparison['mismatches'])}")

    # Structural paper numbers from the current SMART bulk sample.
    assert len(record_sets) == 18
    assert total_fields == 186
    assert comparison["recordsets_matched"] == comparison["recordsets_gt"]
    assert comparison["fields_matched"] > 0
    assert comparison["semantic_type_agree_pct"] >= 90.0
