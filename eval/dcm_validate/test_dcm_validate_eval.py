"""Paired DICOM + NIfTI evaluation against the dcm_validate corpus.

Six modules from the Rorden et al. (Sci Data 2025) dcm_validate corpus
[https://doi.org/10.5281/zenodo.15310934] are baked twice each:

  - In/  -> exercises the DICOM handler
  - Ref/ -> exercises the NIfTI handler and (via paired BIDS .json
            sidecars) the JSON handler in a single bake

For each module the evaluation asserts:
  * DICOM RecordSet emits the standard tag fields with descriptions that
    reference real DICOM tag IDs validated against pydicom.datadict
    (DICOM PS3.6 dictionary as ground truth).
  * NIfTI RecordSet emits the NIfTI-1 core fields (dim_x/y/z,
    voxel_spacing, data_dtype, nifti_version, tr_seconds).
  * One per-volume RecordSet is lifted per .nii reference volume from
    its paired BIDS JSON sidecar (multi-handler cooperation in one bake).

Requires a one-time download (~1.1 GB across 6 module repos):
    bash eval/dcm_validate/download.sh

The evaluation is automatically skipped when the downloaded data is not
present.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from pydicom.datadict import keyword_for_tag
from typer.testing import CliRunner

from croissant_baker.__main__ import app

runner = CliRunner()

EVAL_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = EVAL_DIR / "output"
SUMMARY_FILE = OUTPUT_DIR / "dcm_validate_summary.json"

MODULES = (
    "dcm_qa_ge",
    "dcm_qa_nih",
    "dcm_qa_polar",
    "dcm_qa_stc",
    "dcm_qa_uih",
    "dcm_qa_enh",
)

NIFTI_CORE_FIELDS = {
    "dim_x",
    "dim_y",
    "dim_z",
    "voxel_spacing",
    "data_dtype",
    "nifti_version",
    "tr_seconds",
}

DICOM_TAG_PATTERN = re.compile(r"\(([0-9A-Fa-f]{4}),([0-9A-Fa-f]{4})\)")


def _has_modules() -> bool:
    return all((EVAL_DIR / m / "In").exists() for m in MODULES)


def _bake(input_dir: Path, output_file: Path, name: str) -> dict:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    result = runner.invoke(
        app,
        [
            "-i",
            str(input_dir),
            "-o",
            str(output_file),
            "--no-validate",
            "--name",
            name,
            "--creator",
            "Chris Rorden",
            "--license",
            "BSD-2-Clause",
            "--url",
            f"https://github.com/neurolabusc/{input_dir.parent.name}",
        ],
    )
    assert result.exit_code == 0, (
        f"Croissant Baker failed on {input_dir}:\n{result.stdout}"
    )
    with open(output_file) as f:
        return json.load(f)


def _check_dicom_tag_validity(metadata: dict) -> tuple[int, int]:
    """Count how many DICOM tag IDs in field descriptions are real
    PS3.6 dictionary entries. Returns (matched, total)."""
    matched = total = 0
    for rs in metadata.get("recordSet", []):
        for field in rs.get("field", []):
            for m in DICOM_TAG_PATTERN.finditer(field.get("description", "")):
                total += 1
                tag = (int(m.group(1), 16) << 16) | int(m.group(2), 16)
                if keyword_for_tag(tag):
                    matched += 1
    return matched, total


def _summarize_dicom(metadata: dict) -> dict:
    rs_dicom = next(
        (rs for rs in metadata.get("recordSet", []) if rs.get("name") == "dicom"),
        None,
    )
    n_dcm = sum(
        1
        for d in metadata.get("distribution", [])
        if d.get("@type") == "cr:FileObject"
        and d.get("name", "").lower().endswith((".dcm", ".dicom"))
    )
    matched, total = _check_dicom_tag_validity(metadata)
    return {
        "dicom_files": n_dcm,
        "dicom_record_sets": 1 if rs_dicom else 0,
        "dicom_fields": len(rs_dicom.get("field", [])) if rs_dicom else 0,
        "tag_ids_seen": total,
        "tag_ids_valid": matched,
    }


def _summarize_nifti(metadata: dict) -> dict:
    rs_list = metadata.get("recordSet", [])
    rs_nifti = next((rs for rs in rs_list if rs.get("name") == "nifti"), None)
    n_nii = sum(
        1
        for d in metadata.get("distribution", [])
        if d.get("@type") == "cr:FileObject"
        and d.get("name", "").lower().endswith((".nii", ".nii.gz"))
    )
    per_volume = [
        rs
        for rs in rs_list
        if rs.get("name") != "nifti" and rs.get("@id", "") not in {"nifti"}
    ]
    bids_fields = sum(len(rs.get("field", [])) for rs in per_volume)
    return {
        "nifti_files": n_nii,
        "nifti_core_fields": (
            len(set(f["name"] for f in rs_nifti.get("field", [])) & NIFTI_CORE_FIELDS)
            if rs_nifti
            else 0
        ),
        "nifti_per_volume_record_sets": len(per_volume),
        "bids_sidecar_fields_lifted": bids_fields,
    }


@pytest.mark.skipif(
    not _has_modules(),
    reason="dcm_validate modules not downloaded. Run: bash eval/dcm_validate/download.sh",
)
def test_dcm_validate_evaluation() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    per_module: dict[str, dict] = {}

    for mod in MODULES:
        mod_dir = EVAL_DIR / mod
        if not (mod_dir / "In").exists():
            continue

        dicom_meta = _bake(
            mod_dir / "In",
            OUTPUT_DIR / f"{mod}_dicom.jsonld",
            f"{mod}_dicom",
        )
        nifti_meta = _bake(
            mod_dir / "Ref",
            OUTPUT_DIR / f"{mod}_nifti.jsonld",
            f"{mod}_nifti",
        )

        d = _summarize_dicom(dicom_meta)
        n = _summarize_nifti(nifti_meta)
        per_module[mod] = {**d, **n}

        # Per-module structural assertions.
        assert d["dicom_files"] > 0, f"{mod}: no DICOM files emitted"
        assert d["dicom_record_sets"] == 1, f"{mod}: expected 1 dicom RecordSet"
        assert d["dicom_fields"] >= 8, (
            f"{mod}: dicom RecordSet has only {d['dicom_fields']} fields"
        )
        assert d["tag_ids_valid"] == d["tag_ids_seen"] > 0, (
            f"{mod}: {d['tag_ids_seen'] - d['tag_ids_valid']} unrecognized "
            f"DICOM tag IDs in field descriptions"
        )

        assert n["nifti_files"] > 0, f"{mod}: no NIfTI files emitted"
        assert n["nifti_core_fields"] == len(NIFTI_CORE_FIELDS), (
            f"{mod}: nifti RecordSet missing core fields "
            f"(got {n['nifti_core_fields']}/{len(NIFTI_CORE_FIELDS)})"
        )
        assert n["nifti_per_volume_record_sets"] >= 1, (
            f"{mod}: no per-volume RecordSets lifted from BIDS sidecars"
        )

    assert per_module, "No modules processed"

    aggregate = {
        "modules_evaluated": len(per_module),
        "total_dicom_files": sum(m["dicom_files"] for m in per_module.values()),
        "total_nifti_volumes": sum(m["nifti_files"] for m in per_module.values()),
        "total_per_volume_record_sets": sum(
            m["nifti_per_volume_record_sets"] for m in per_module.values()
        ),
        "total_bids_fields_lifted": sum(
            m["bids_sidecar_fields_lifted"] for m in per_module.values()
        ),
        "dicom_tag_validity_pct": round(
            100
            * sum(m["tag_ids_valid"] for m in per_module.values())
            / max(1, sum(m["tag_ids_seen"] for m in per_module.values())),
            2,
        ),
    }
    summary = {"aggregate": aggregate, "per_module": per_module}

    with open(SUMMARY_FILE, "w") as f:
        json.dump(summary, f, indent=2)
        f.write("\n")

    print("\n=== dcm_validate Evaluation ===")
    print(f"  Modules evaluated:           {aggregate['modules_evaluated']}")
    print(f"  Total DICOM files baked:     {aggregate['total_dicom_files']}")
    print(f"  Total NIfTI volumes baked:   {aggregate['total_nifti_volumes']}")
    print(f"  Per-volume RecordSets:       {aggregate['total_per_volume_record_sets']}")
    print(f"  BIDS sidecar fields lifted:  {aggregate['total_bids_fields_lifted']}")
    print(f"  DICOM tag ID validity:       {aggregate['dicom_tag_validity_pct']}%")
    print()
    for mod, s in per_module.items():
        print(
            f"  {mod:20s}  DICOM {s['dicom_files']:>5d} files / "
            f"{s['dicom_fields']:>2d} fields    "
            f"NIfTI {s['nifti_files']:>3d} files / "
            f"{s['nifti_per_volume_record_sets']:>2d} per-vol RS / "
            f"{s['bids_sidecar_fields_lifted']:>4d} BIDS fields"
        )

    assert aggregate["dicom_tag_validity_pct"] == 100.0, (
        f"DICOM tag ID validity {aggregate['dicom_tag_validity_pct']}% is below 100%"
    )
