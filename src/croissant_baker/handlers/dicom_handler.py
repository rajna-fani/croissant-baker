"""DICOM file handler for medical imaging datasets."""

import logging
from pathlib import Path
from typing import Dict, List, Optional

import mlcroissant as mlc

from croissant_baker.handlers.base_handler import FileTypeHandler
from croissant_baker.handlers.utils import compute_file_hash

logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = {".dcm", ".dicom"}

MIME_TYPE = "application/dicom"

_DICOM_MAGIC_OFFSET = 128
_DICOM_MAGIC = b"DICM"


def _has_dicom_magic(file_path: Path) -> bool:
    try:
        with open(file_path, "rb") as f:
            f.seek(_DICOM_MAGIC_OFFSET)
            return f.read(4) == _DICOM_MAGIC
    except (IOError, OSError):
        return False


def _safe_get(ds, keyword: str, default=None):
    try:
        val = getattr(ds, keyword, None)
        if val is None:
            return default
        if hasattr(val, "__iter__") and not isinstance(val, (str, bytes)):
            return [float(v) for v in val]
        return val
    except Exception:
        return default


def _read_dicom_properties(file_path: Path) -> Dict:
    import pydicom

    ds = pydicom.dcmread(str(file_path), stop_before_pixels=True)

    props: Dict = {}

    rows = _safe_get(ds, "Rows")
    columns = _safe_get(ds, "Columns")
    if rows is not None:
        props["rows"] = int(rows)
    if columns is not None:
        props["columns"] = int(columns)

    num_frames = _safe_get(ds, "NumberOfFrames")
    props["num_frames"] = int(num_frames) if num_frames is not None else 1

    bits_allocated = _safe_get(ds, "BitsAllocated")
    if bits_allocated is not None:
        props["bits_allocated"] = int(bits_allocated)

    samples_per_pixel = _safe_get(ds, "SamplesPerPixel")
    if samples_per_pixel is not None:
        props["samples_per_pixel"] = int(samples_per_pixel)

    photometric = _safe_get(ds, "PhotometricInterpretation")
    if photometric is not None:
        props["photometric_interpretation"] = str(photometric).strip()

    pixel_spacing = _safe_get(ds, "PixelSpacing")
    if pixel_spacing is not None:
        props["pixel_spacing"] = pixel_spacing

    slice_thickness = _safe_get(ds, "SliceThickness")
    if slice_thickness is not None:
        try:
            props["slice_thickness"] = float(slice_thickness)
        except (ValueError, TypeError):
            pass

    modality = _safe_get(ds, "Modality")
    if modality is not None:
        props["modality"] = str(modality).strip()

    study_desc = _safe_get(ds, "StudyDescription")
    if study_desc is not None:
        props["study_description"] = str(study_desc).strip()

    series_desc = _safe_get(ds, "SeriesDescription")
    if series_desc is not None:
        props["series_description"] = str(series_desc).strip()

    manufacturer = _safe_get(ds, "Manufacturer")
    if manufacturer is not None:
        props["manufacturer"] = str(manufacturer).strip()

    sop_class = _safe_get(ds, "SOPClassUID")
    if sop_class is not None:
        props["sop_class_uid"] = str(sop_class)

    return props


class DICOMHandler(FileTypeHandler):
    """
    Handler for DICOM medical imaging files (.dcm, .dicom).

    - Detects files by extension or DICOM magic bytes (DICM at offset 128)
    - Extracts image geometry, pixel encoding, modality, and acquisition
      parameters using pydicom (header only — no pixel data loaded)
    - Computes SHA256 for reproducibility
    """

    EXTENSIONS = (".dcm", ".dicom")
    FORMAT_NAME = "DICOM"
    FORMAT_DESCRIPTION = (
        "Image geometry, modality, pixel encoding, acquisition parameters"
    )

    def can_handle(self, file_path: Path) -> bool:
        suffix = file_path.suffix.lower()
        # For all candidates (by extension or extensionless), verify the DICM
        # preamble when the file is present. Files without it are DICOMDIR
        # fragment references, not standalone DICOM — skip them rather than
        # letting extract_metadata raise later.
        if suffix in SUPPORTED_EXTENSIONS or (not suffix and file_path.is_file()):
            if file_path.is_file():
                return _has_dicom_magic(file_path)
            return True  # path-only check (e.g. registry lookup before file exists)
        return False

    def extract_metadata(self, file_path: Path, **kwargs) -> dict:
        if not file_path.exists():
            raise FileNotFoundError(f"DICOM file not found: {file_path}")

        try:
            props = _read_dicom_properties(file_path)
        except Exception as e:
            raise ValueError(f"Failed to read DICOM file {file_path}: {e}") from e

        return {
            "file_path": str(file_path),
            "file_name": file_path.name,
            "file_size": file_path.stat().st_size,
            "sha256": compute_file_hash(file_path),
            "encoding_format": MIME_TYPE,
            "dicom_properties": props,
        }

    def build_croissant(
        self, file_metas: list[dict], file_ids: list[str]
    ) -> tuple[list, list]:
        summary = collect_dicom_summary(file_metas)

        num_files = summary.get("num_files", len(file_metas))
        modality_counts = summary.get("modality_counts", {})
        modalities_str = (
            ", ".join(f"{m} ({c})" for m, c in modality_counts.items())
            if modality_counts
            else "unknown modality"
        )

        rows_range = summary.get("rows_range")
        cols_range = summary.get("columns_range")
        if rows_range and cols_range:
            if rows_range[0] == rows_range[1] and cols_range[0] == cols_range[1]:
                dims_note = f"{rows_range[0]}x{cols_range[0]}"
            else:
                dims_note = (
                    f"{rows_range[0]}-{rows_range[1]}x{cols_range[0]}-{cols_range[1]}"
                )
        else:
            dims_note = "unknown dimensions"

        fileset_id = "dicom-files"
        dicom_fileset = mlc.FileSet(
            id=fileset_id,
            name="DICOM files",
            description=f"{num_files} DICOM file(s) ({modalities_str})",
            encoding_formats=[MIME_TYPE],
            includes=["**/*.dcm", "**/*.dicom"],
        )

        fields = [
            mlc.Field(
                id="dicom/modality",
                name="modality",
                description="DICOM modality (e.g. CT, MR, PT)",
                data_types=["sc:Text"],
                source=mlc.Source(
                    file_set=fileset_id,
                    extract=mlc.Extract(file_property="content"),
                ),
            ),
            mlc.Field(
                id="dicom/rows",
                name="rows",
                description="Number of pixel rows (image height)",
                data_types=["sc:Integer"],
                source=mlc.Source(
                    file_set=fileset_id,
                    extract=mlc.Extract(file_property="content"),
                ),
            ),
            mlc.Field(
                id="dicom/columns",
                name="columns",
                description="Number of pixel columns (image width)",
                data_types=["sc:Integer"],
                source=mlc.Source(
                    file_set=fileset_id,
                    extract=mlc.Extract(file_property="content"),
                ),
            ),
            mlc.Field(
                id="dicom/num_frames",
                name="num_frames",
                description="Number of frames (>1 for multi-frame / cine DICOM)",
                data_types=["sc:Integer"],
                source=mlc.Source(
                    file_set=fileset_id,
                    extract=mlc.Extract(file_property="content"),
                ),
            ),
            mlc.Field(
                id="dicom/bits_allocated",
                name="bits_allocated",
                description="Bits allocated per pixel sample",
                data_types=["sc:Integer"],
                source=mlc.Source(
                    file_set=fileset_id,
                    extract=mlc.Extract(file_property="content"),
                ),
            ),
        ]

        dicom_record_set = mlc.RecordSet(
            id="dicom",
            name="dicom",
            description=f"{num_files} DICOM files ({dims_note}): {modalities_str}",
            fields=fields,
        )

        return [dicom_fileset], [dicom_record_set]


def collect_dicom_summary(dicom_metadata_list: List[Dict]) -> Dict:
    if not dicom_metadata_list:
        return {}

    rows_list: List[int] = []
    cols_list: List[int] = []
    frames_list: List[int] = []
    modalities: Dict[str, int] = {}
    bits_set: set = set()

    for meta in dicom_metadata_list:
        props = meta.get("dicom_properties", {})

        if "rows" in props:
            rows_list.append(props["rows"])
        if "columns" in props:
            cols_list.append(props["columns"])
        if "num_frames" in props:
            frames_list.append(props["num_frames"])
        if "bits_allocated" in props:
            bits_set.add(props["bits_allocated"])

        modality: Optional[str] = props.get("modality")
        if modality:
            modalities[modality] = modalities.get(modality, 0) + 1

    summary: Dict = {"num_files": len(dicom_metadata_list)}

    if rows_list:
        summary["rows_range"] = (min(rows_list), max(rows_list))
    if cols_list:
        summary["columns_range"] = (min(cols_list), max(cols_list))
    if frames_list:
        summary["frames_range"] = (min(frames_list), max(frames_list))
    if modalities:
        summary["modality_counts"] = modalities
    if bits_set:
        summary["bits_allocated_values"] = sorted(bits_set)

    return summary
