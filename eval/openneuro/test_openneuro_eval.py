"""External validation against a sample of the smallest OpenNeuro datasets.

OpenNeuro [https://openneuro.org] hosts >1000 BIDS-organized neuroimaging
datasets. Each dataset typically contains ``.nii``/``.nii.gz`` volumes,
paired BIDS ``.json`` sidecars, and tabular ``.tsv`` files
(``participants.tsv``, ``*_events.tsv``).

This evaluation bakes each downloaded dataset and asserts that:
  * The NIfTI handler emits a ``nifti`` RecordSet with NIfTI-1 core fields
    when the dataset contains volumes.
  * The JSON handler lifts at least one BIDS sidecar as a per-volume
    RecordSet (cross-handler cooperation in a single bake).

Tabular ``.tsv`` and ``.csv`` content is processed by the TSV handler
and counted in the per-dataset summary alongside NIfTI and sidecar
RecordSets. Datasets without NIfTI volumes still exercise the JSON and
TSV handlers, contributing to the per-volume sidecar and tabular
RecordSet counts.

Requires a one-time download (size depends on LIMIT, default ~5 GB):
    bash eval/openneuro/download.sh

The evaluation is automatically bypassed when no dataset directories are
present.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from croissant_baker.__main__ import app

runner = CliRunner()

EVAL_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = EVAL_DIR / "output"
SUMMARY_FILE = OUTPUT_DIR / "openneuro_summary.json"

NIFTI_STRUCTURAL_FIELDS = {
    "dim_x",
    "dim_y",
    "dim_z",
    "voxel_spacing",
    "data_dtype",
    "nifti_version",
}
# tr_seconds (Repetition Time) is a temporal field that only applies to
# functional/dynamic acquisitions; structural (T1w/T2w) and PET volumes have
# no meaningful TR. Tracked separately as a soft signal rather than required.


def _dataset_dirs() -> list[Path]:
    return sorted(p for p in EVAL_DIR.glob("ds*") if p.is_dir())


def _has_datasets() -> bool:
    return bool(_dataset_dirs())


def _bake(dataset_dir: Path, output_file: Path) -> dict | None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    result = runner.invoke(
        app,
        [
            "-i",
            str(dataset_dir),
            "-o",
            str(output_file),
            "--no-validate",
            "--name",
            dataset_dir.name,
            "--creator",
            "OpenNeuro contributors",
            "--license",
            "CC0-1.0",
            "--url",
            f"https://openneuro.org/datasets/{dataset_dir.name}",
        ],
    )
    if result.exit_code != 0:
        return None
    with open(output_file) as f:
        return json.load(f)


def _summarize(dataset_dir: Path, metadata: dict) -> dict:
    n_nii_on_disk = sum(
        1
        for p in dataset_dir.rglob("*")
        if p.is_file() and (p.name.endswith(".nii") or p.name.endswith(".nii.gz"))
    )
    n_tsv_on_disk = sum(1 for p in dataset_dir.rglob("*.tsv") if p.is_file())
    n_json_on_disk = sum(1 for p in dataset_dir.rglob("*.json") if p.is_file())
    # Per-volume sidecars are .json files that sit next to a .nii(.gz) of the
    # same stem. Top-level metadata files (dataset_description.json, etc.) and
    # sidecars without a matching volume are excluded.
    nii_stems = {
        (p.parent, p.name.removesuffix(".gz").removesuffix(".nii"))
        for p in dataset_dir.rglob("*")
        if p.is_file() and (p.name.endswith(".nii") or p.name.endswith(".nii.gz"))
    }
    n_per_volume_sidecars_on_disk = sum(
        1
        for p in dataset_dir.rglob("*.json")
        if p.is_file() and (p.parent, p.stem) in nii_stems
    )

    rs_list = metadata.get("recordSet", [])
    rs_by_name = {rs.get("name", rs.get("@id", "")): rs for rs in rs_list}

    nifti_rs = rs_by_name.get("nifti")
    nifti_field_names = (
        {f["name"] for f in nifti_rs.get("field", [])} if nifti_rs else set()
    )
    structural_present = len(nifti_field_names & NIFTI_STRUCTURAL_FIELDS)
    has_tr_seconds = "tr_seconds" in nifti_field_names

    # Classify each non-trivial RecordSet by the encoding format of the
    # FileObject referenced by its first field's source. The JSON, TSV, and
    # CSV handlers each lift their input as a RecordSet whose fields point at
    # the originating FileObject; we count them separately so paper-grade
    # numbers do not conflate sidecars with tabular records.
    fileobject_format = {
        d.get("@id"): d.get("encodingFormat", "")
        for d in metadata.get("distribution", [])
        if d.get("@type") == "cr:FileObject"
    }
    JSON_FORMATS = {"application/json", "application/jsonl", "application/gzip"}
    TABULAR_FORMATS = {"text/tab-separated-values", "text/tsv", "text/csv"}

    sidecar_record_sets = 0
    tabular_record_sets = 0
    for rs in rs_list:
        if rs.get("name") in {"nifti", "dataset_description"}:
            continue
        fields = rs.get("field", [])
        if not fields:
            continue
        src_id = fields[0].get("source", {}).get("fileObject", {}).get("@id")
        fmt = fileobject_format.get(src_id, "")
        if fmt in JSON_FORMATS:
            sidecar_record_sets += 1
        elif fmt in TABULAR_FORMATS:
            tabular_record_sets += 1

    tsv_files_emitted = sum(
        1
        for d in metadata.get("distribution", [])
        if d.get("@type") == "cr:FileObject" and d.get("name", "").endswith(".tsv")
    )

    return {
        "nii_files_on_disk": n_nii_on_disk,
        "tsv_files_on_disk": n_tsv_on_disk,
        "json_files_on_disk": n_json_on_disk,
        "per_volume_sidecars_on_disk": n_per_volume_sidecars_on_disk,
        "record_sets_total": len(rs_list),
        "nifti_record_set": bool(nifti_rs),
        "nifti_structural_fields": structural_present,
        "has_tr_seconds": has_tr_seconds,
        "sidecar_record_sets": sidecar_record_sets,
        "tabular_record_sets": tabular_record_sets,
        "tsv_files_emitted": tsv_files_emitted,
    }


@pytest.mark.skipif(
    not _has_datasets(),
    reason="OpenNeuro datasets not downloaded. Run: bash eval/openneuro/download.sh",
)
def test_openneuro_evaluation() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    per_dataset: dict[str, dict] = {}
    non_nifti_datasets: list[str] = []
    bake_failures: list[str] = []

    for ds_dir in _dataset_dirs():
        ds_id = ds_dir.name
        meta = _bake(ds_dir, OUTPUT_DIR / f"{ds_id}.jsonld")
        if meta is None:
            bake_failures.append(ds_id)
            continue

        s = _summarize(ds_dir, meta)
        per_dataset[ds_id] = s

        if s["nii_files_on_disk"] == 0:
            non_nifti_datasets.append(ds_id)
            continue

        # Per-dataset assertions for datasets that contain NIfTI.
        assert s["nifti_record_set"], (
            f"{ds_id}: NIfTI files present on disk but no nifti RecordSet emitted"
        )
        assert s["nifti_structural_fields"] == len(NIFTI_STRUCTURAL_FIELDS), (
            f"{ds_id}: nifti RecordSet missing structural fields "
            f"({s['nifti_structural_fields']}/{len(NIFTI_STRUCTURAL_FIELDS)})"
        )
        if s["per_volume_sidecars_on_disk"] > 0:
            assert s["sidecar_record_sets"] >= 1, (
                f"{ds_id}: {s['per_volume_sidecars_on_disk']} per-volume "
                f"BIDS sidecars on disk but none lifted as RecordSets"
            )

    assert per_dataset, "No datasets baked"

    nifti_datasets = {
        k: v for k, v in per_dataset.items() if v["nii_files_on_disk"] > 0
    }
    tsv_datasets = {k: v for k, v in per_dataset.items() if v["tsv_files_on_disk"] > 0}

    aggregate = {
        "datasets_downloaded": len(per_dataset) + len(bake_failures),
        "datasets_baked": len(per_dataset),
        "datasets_with_nifti": len(nifti_datasets),
        "datasets_with_tsv": len(tsv_datasets),
        "datasets_with_tr_seconds": sum(
            1 for s in per_dataset.values() if s["has_tr_seconds"]
        ),
        "datasets_without_nifti": len(non_nifti_datasets),
        "bake_failures": len(bake_failures),
        "total_nii_files": sum(s["nii_files_on_disk"] for s in per_dataset.values()),
        "total_tsv_files": sum(s["tsv_files_on_disk"] for s in per_dataset.values()),
        "total_json_files": sum(s["json_files_on_disk"] for s in per_dataset.values()),
        "total_sidecar_record_sets": sum(
            s["sidecar_record_sets"] for s in per_dataset.values()
        ),
        "total_tabular_record_sets": sum(
            s["tabular_record_sets"] for s in per_dataset.values()
        ),
        "total_tsv_files_emitted": sum(
            s["tsv_files_emitted"] for s in per_dataset.values()
        ),
    }
    summary = {
        "aggregate": aggregate,
        "non_nifti_datasets": non_nifti_datasets,
        "bake_failures": bake_failures,
        "per_dataset": per_dataset,
    }
    with open(SUMMARY_FILE, "w") as f:
        json.dump(summary, f, indent=2)
        f.write("\n")

    print("\n=== OpenNeuro Evaluation ===")
    print(f"  Datasets downloaded:         {aggregate['datasets_downloaded']}")
    print(f"  Datasets baked:              {aggregate['datasets_baked']}")
    print(f"  Datasets with NIfTI:         {aggregate['datasets_with_nifti']}")
    print(f"  Datasets with TSV:           {aggregate['datasets_with_tsv']}")
    print(f"  Without NIfTI volumes:       {aggregate['datasets_without_nifti']}")
    print(f"  Bake failures:               {aggregate['bake_failures']}")
    print(f"  Total NIfTI files (on disk): {aggregate['total_nii_files']}")
    print(f"  Total TSV files (on disk):   {aggregate['total_tsv_files']}")
    print(f"  Sidecar RecordSets (.json):  {aggregate['total_sidecar_record_sets']}")
    print(f"  Tabular RecordSets (.tsv/.csv): {aggregate['total_tabular_record_sets']}")
    print(f"  TSV FileObjects emitted:     {aggregate['total_tsv_files_emitted']}")
    print()
    for ds_id, s in per_dataset.items():
        print(
            f"  {ds_id:12s}  NIfTI {s['nii_files_on_disk']:>3d} / "
            f"TSV {s['tsv_files_on_disk']:>3d} / "
            f"JSON {s['json_files_on_disk']:>3d}    "
            f"sidecar RS {s['sidecar_record_sets']:>3d} / "
            f"tabular RS {s['tabular_record_sets']:>3d}"
        )

    assert aggregate["datasets_with_nifti"] >= 1, (
        "No downloaded dataset contained NIfTI volumes; eval has nothing to assert"
    )
    assert aggregate["bake_failures"] == 0, (
        f"{aggregate['bake_failures']} datasets failed to bake: {bake_failures}"
    )
