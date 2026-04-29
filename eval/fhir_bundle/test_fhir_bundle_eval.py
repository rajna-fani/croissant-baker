"""Out-of-distribution evaluation: FHIR JSON Bundle format (SMART custom-sample-data).

Croissant Baker is run against 5 FHIR transaction Bundle JSON files from the SMART
Health IT custom-sample-data repository, exercising the JSON Bundle code path of
the FHIR handler against non-MIMIC FHIR data. MIMIC-IV FHIR uses NDJSON bulk format;
these are single-file JSON Bundles — a structurally distinct ingestion path.

Source: https://github.com/smart-on-fhir/custom-sample-data (fhir-resources/)
Format: JSON Bundle (resourceType = "Bundle", type = "transaction")
License: CC0

Requires a one-time download (~150 KB):
    bash eval/fhir_bundle/download.sh

Evaluation is automatically skipped when the downloaded data is not present.
"""

import json
import time
from pathlib import Path

import pytest
from typer.testing import CliRunner

from croissant_baker.__main__ import app

runner = CliRunner()

EVAL_DIR = Path(__file__).resolve().parent
INPUT_DIR = EVAL_DIR / "fhir"
OUTPUT_DIR = EVAL_DIR / "output"
OUTPUT_FILE = OUTPUT_DIR / "bundle_croissant.jsonld"

EXPECTED_RESOURCE_TYPES = {
    "AllergyIntolerance",
    "Condition",
    "Coverage",
    "Encounter",
    "FamilyMemberHistory",
    "MedicationOrder",
    "MedicationStatement",
    "NutritionOrder",
    "Observation",
    "Patient",
    "Procedure",
}


def _has_bundle_data() -> bool:
    return INPUT_DIR.exists() and any(INPUT_DIR.glob("*.json"))


@pytest.mark.skipif(
    not _has_bundle_data(),
    reason=("FHIR Bundle data not downloaded. Run: bash eval/fhir_bundle/download.sh"),
)
def test_fhir_bundle_evaluation() -> None:
    """Generate Croissant for FHIR JSON Bundles and validate structural fidelity."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    start = time.perf_counter()
    result = runner.invoke(
        app,
        [
            "-i",
            str(INPUT_DIR),
            "-o",
            str(OUTPUT_FILE),
            "--name",
            "SMART FHIR Bundle Sample",
            "--creator",
            "SMART Health IT",
            "--description",
            "Sample FHIR transaction bundles from SMART Health IT",
            "--license",
            "https://creativecommons.org/publicdomain/zero/1.0/",
        ],
    )
    elapsed = time.perf_counter() - start

    assert result.exit_code == 0, f"Croissant Baker failed:\n{result.stdout}"
    assert OUTPUT_FILE.exists(), "Output file was not created"

    with open(OUTPUT_FILE) as f:
        metadata = json.load(f)

    record_sets = metadata.get("recordSet", [])
    distribution = metadata.get("distribution", [])
    assert len(record_sets) > 0, "No RecordSets generated"

    # The Bundle handler groups resource types across all Bundle files into one
    # RecordSet per type, backed by a single FileSet.
    generated_names = {rs.get("name", rs.get("@id", "")) for rs in record_sets}
    assert EXPECTED_RESOURCE_TYPES == generated_names, (
        f"Resource type mismatch.\n"
        f"  Expected: {sorted(EXPECTED_RESOURCE_TYPES)}\n"
        f"  Got:      {sorted(generated_names)}"
    )

    # 5 FileObjects (one per Bundle file) + 1 FileSet = 6 distribution entries
    assert len(distribution) == 6, (
        f"Expected 6 distribution items, got {len(distribution)}"
    )

    # Verify key resource types have non-trivial field counts
    rs_by_name = {rs.get("name", rs.get("@id", "")): rs for rs in record_sets}
    for rtype in ("Patient", "Observation", "Condition"):
        fields = rs_by_name[rtype].get("field", [])
        assert len(fields) >= 4, (
            f"{rtype} has only {len(fields)} fields — unexpectedly sparse"
        )

    # Summary for paper
    total_fields = sum(len(rs.get("field", [])) for rs in record_sets)
    print("\n=== FHIR Bundle Evaluation ===")
    print("  Input files: 5 JSON Bundles")
    print(f"  RecordSets: {len(record_sets)} ({', '.join(sorted(generated_names))})")
    print(f"  Total fields: {total_fields}")
    print(f"  Generation time: {elapsed:.2f}s")
    for rs in sorted(record_sets, key=lambda r: r.get("name", "")):
        n = len(rs.get("field", []))
        print(f"    {rs.get('name', '?'):30s} {n:3d} fields")

    # Paper numbers: 11 RecordSets, 94 total fields
    assert len(record_sets) == 11
    assert total_fields == 94
