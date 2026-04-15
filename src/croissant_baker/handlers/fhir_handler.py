"""FHIR (Fast Healthcare Interoperability Resources) file handler.

Supports two FHIR serialisation formats found in clinical research datasets:

- NDJSON bulk export (.ndjson, .ndjson.gz): one resource per line, all of the
  same resourceType (FHIR Bulk Data spec §3.1).

- JSON Bundle (.json, .json.gz): a single Bundle resource whose entry[] array
  may contain mixed resourceTypes.
"""

import json
import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Optional

import mlcroissant as mlc

from croissant_baker.handlers.base_handler import FileTypeHandler
from croissant_baker.handlers.utils import (
    SCHEMA_SAMPLE,
    build_fields_from_json_schema,
    compute_file_hash,
    get_clean_record_name,
    infer_json_schema,
    open_text_file,
    sanitize_id,
)

logger = logging.getLogger(__name__)

# Infrastructure resource types skipped in build_croissant — they carry error/warning
# metadata, not clinical data, and have no ML value regardless of format.
# In NDJSON bulk exports, servers may inline these as error rows (Bulk Data spec §4.2).
# In Bundles, they can appear as per-entry error markers in transaction/batch responses.
_SKIP_RESOURCE_TYPES = frozenset({"OperationOutcome"})


def merge_fhir_column_types(all_schemas: list) -> dict:
    """Merge column schemas from multiple FHIR files.

    Three possible type representations are handled:
    - str                          → scalar primitive
    - {"type": str, "is_array": True}  → array of primitives
    - {"fields": {...}, "is_array": bool} → struct (or array-of-struct)

    Priority when the same field appears with different representations across
    files: struct > primitive-array > primitive. FHIR R4 cardinality is fixed
    by the spec, so richer structure always wins.

    Args:
        all_schemas: List of column schema dicts from individual files.

    Returns:
        Merged schema dict.
    """
    all_keys: set = set()
    for schema in all_schemas:
        all_keys.update(schema.keys())
    merged = {}
    for key in sorted(all_keys):
        type_infos = [s[key] for s in all_schemas if key in s]
        if not type_infos:
            merged[key] = "sc:Text"
            continue
        struct_dicts = [t for t in type_infos if isinstance(t, dict) and "fields" in t]
        prim_arrays = [t for t in type_infos if isinstance(t, dict) and "type" in t]
        primitives = [t for t in type_infos if isinstance(t, str)]
        if struct_dicts:
            is_array = any(t.get("is_array", False) for t in struct_dicts)
            sub = merge_fhir_column_types(
                [t["fields"] for t in struct_dicts if t.get("fields")]
            )
            merged[key] = {"fields": sub, "is_array": is_array}
        elif prim_arrays:
            votes: dict = {}
            for t in prim_arrays:
                votes[t["type"]] = votes.get(t["type"], 0) + 1
            merged[key] = {"type": max(votes, key=votes.get), "is_array": True}
        else:
            votes = {}
            for t in primitives:
                votes[t] = votes.get(t, 0) + 1
            merged[key] = max(votes, key=votes.get) if votes else "sc:Text"
    return merged


def _is_bulk_chunk(file_name: str, resource_type: str) -> bool:
    """Return True when file_name follows the FHIR bulk-export chunk convention.

    The FHIR Bulk Data spec names chunks ``{ResourceType}.{NNN}.ndjson[.gz]``
    (e.g. ``Observation.000.ndjson``).  Files whose stem does not match the
    resourceType exactly are distinct logical tables that happen to share a
    resourceType string (e.g. ``MimicObservationLabevents.ndjson.gz``).
    """
    stem = file_name.lower()
    for ext in (".gz", ".ndjson"):
        if stem.endswith(ext):
            stem = stem[: -len(ext)]
    stem = re.sub(r"\.\d+$", "", stem)
    return stem == resource_type.lower()


def _is_fhir_object(obj) -> bool:
    """Return True if *obj* is a dict with a ``resourceType`` key."""
    return isinstance(obj, dict) and "resourceType" in obj


class FHIRHandler(FileTypeHandler):
    """Handler for FHIR datasets in NDJSON bulk-export or JSON Bundle format.

    Detection strategy:
    - ``.ndjson`` / ``.ndjson.gz``: accepted without content sniffing.
    - ``.json`` / ``.json.gz``: accepted only after peeking at the file to
      confirm the presence of a ``resourceType`` key. This ensures non-FHIR
      JSON files (e.g. a future OMOP JSON handler) are not claimed here.
    """

    EXTENSIONS = (".ndjson", ".ndjson.gz", ".json", ".json.gz")
    FORMAT_NAME = "FHIR"
    FORMAT_DESCRIPTION = "Resource types, field names and types per resource"

    def can_handle(self, file_path: Path) -> bool:
        name = file_path.name.lower()
        if name.endswith(".ndjson") or name.endswith(".ndjson.gz"):
            return True
        if name.endswith(".json") or name.endswith(".json.gz"):
            return self._sniff_fhir_json(file_path)
        return False

    def _sniff_fhir_json(self, file_path: Path) -> bool:
        """Peek at the first 4 KB of a JSON file to confirm it is FHIR."""
        try:
            with open_text_file(file_path) as fh:
                head = fh.read(4096)
            head = head.strip()
            if not head.startswith("{"):
                return False
            try:
                obj = json.loads(head)
            except json.JSONDecodeError:
                return bool(re.search(r'"resourceType"\s*:\s*"[A-Z]', head))
            return _is_fhir_object(obj)
        except (OSError, UnicodeDecodeError):
            return False

    def extract_metadata(self, file_path: Path, **kwargs) -> dict:
        """Extract FHIR metadata from a NDJSON or JSON Bundle file.

        Returns:
            Metadata dict. NDJSON returns ``column_types`` + ``fhir_resource_type``.
            Bundle returns ``fhir_resource_groups``.

        Raises:
            ValueError: If the file contains no valid FHIR resources.
        """
        sha256 = compute_file_hash(file_path)
        file_size = file_path.stat().st_size
        file_name = file_path.name
        name_lower = file_name.lower()
        if name_lower.endswith(".ndjson") or name_lower.endswith(".ndjson.gz"):
            return self._extract_ndjson(file_path, file_name, sha256, file_size)
        return self._extract_bundle(file_path, file_name, sha256, file_size)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _extract_ndjson(
        self, file_path: Path, file_name: str, sha256: str, file_size: int
    ) -> dict:
        """Extract metadata from a FHIR NDJSON (bulk-export) file.

        Streams line by line. The first ``SCHEMA_SAMPLE`` records are
        collected for schema inference (struct expansion needs actual objects).
        Row counting always covers the full file.

        Rows whose ``resourceType`` differs from the primary type (established
        from the first row) are skipped — this handles OperationOutcome error
        rows that servers may inline per the Bulk Data spec.

        Raises:
            ValueError: If no valid FHIR resources are found.
        """
        schema_samples: list = []
        resource_type: Optional[str] = None
        num_rows = 0

        with open_text_file(file_path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed JSON line in %s", file_name)
                    continue
                if not _is_fhir_object(obj):
                    continue

                row_type = obj.get("resourceType", "FHIRResource")
                if resource_type is None:
                    resource_type = row_type
                elif row_type != resource_type:
                    logger.debug("Skipping inline %s row in %s", row_type, file_name)
                    continue

                num_rows += 1
                if len(schema_samples) < SCHEMA_SAMPLE:
                    schema_samples.append(obj)

        if resource_type is None:
            raise ValueError(f"No valid FHIR resources found in {file_name}")

        if num_rows > SCHEMA_SAMPLE:
            logger.warning(
                "Sampled %d of %d records for schema inference in %s — rare fields may be missing",
                SCHEMA_SAMPLE,
                num_rows,
                file_name,
            )

        column_types = infer_json_schema(schema_samples)
        encoding = (
            "application/gzip"
            if file_path.name.lower().endswith(".gz")
            else "application/fhir+ndjson"
        )
        return {
            "file_path": str(file_path),
            "file_name": file_name,
            "file_size": file_size,
            "sha256": sha256,
            "encoding_format": encoding,
            "fhir_resource_type": resource_type,
            "column_types": column_types,
            "columns": list(column_types.keys()),
            "num_columns": len(column_types),
            "num_rows": num_rows,
        }

    def _extract_bundle(
        self, file_path: Path, file_name: str, sha256: str, file_size: int
    ) -> dict:
        """Extract metadata from a FHIR JSON Bundle or single-resource file.

        Loads the entire file into memory (bundles are typically per-patient and
        small). Large Synthea exports or institutional bulk exports can be
        multi-GB — use NDJSON bulk export format for large datasets instead.
        Groups ``entry[*].resource`` by ``resourceType``, expanding nested
        types into sub-schemas.

        Raises:
            ValueError: If the file cannot be parsed or contains no resources.
        """
        with open_text_file(file_path) as fh:
            try:
                doc = json.load(fh)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Cannot parse {file_name} as JSON: {exc}") from exc

        if not _is_fhir_object(doc):
            raise ValueError(f"No FHIR resourceType found in {file_name}")

        encoding = (
            "application/gzip"
            if file_path.name.lower().endswith(".gz")
            else "application/fhir+json"
        )

        if doc.get("resourceType") != "Bundle":
            rt = doc.get("resourceType", "FHIRResource")
            col_types = infer_json_schema([doc])
            return {
                "file_path": str(file_path),
                "file_name": file_name,
                "file_size": file_size,
                "sha256": sha256,
                "encoding_format": encoding,
                "fhir_resource_type": rt,
                "column_types": col_types,
                "columns": list(col_types.keys()),
                "num_columns": len(col_types),
                "num_rows": 1,
            }

        by_type_resources: dict = defaultdict(list)
        by_type_counts: dict = defaultdict(int)
        for entry in doc.get("entry", []):
            resource = entry.get("resource", {})
            if not _is_fhir_object(resource):
                continue
            rt = resource.get("resourceType", "Unknown")
            by_type_counts[rt] += 1
            if len(by_type_resources[rt]) < SCHEMA_SAMPLE:
                by_type_resources[rt].append(resource)

        if not by_type_counts:
            raise ValueError(f"No FHIR resources found in Bundle {file_name}")

        resource_groups = {}
        for rt, records in by_type_resources.items():
            col_types = infer_json_schema(records)
            resource_groups[rt] = {
                "column_types": col_types,
                "columns": list(col_types.keys()),
                "num_columns": len(col_types),
                "num_rows": by_type_counts[rt],
            }

        return {
            "file_path": str(file_path),
            "file_name": file_name,
            "file_size": file_size,
            "sha256": sha256,
            "encoding_format": encoding,
            "fhir_resource_groups": resource_groups,
        }

    def build_croissant(self, file_metas: list, file_ids: list) -> tuple:
        """Build Croissant distributions and RecordSets for all FHIR files.

        Handles both FHIR formats:
        - NDJSON standalone / single chunk → RecordSet per file → FileObject.
        - NDJSON ≥2 chunks (same resourceType, spec naming) → FileSet + merged RecordSet.
        - Bundle → one FileSet + one RecordSet per resourceType.

        Infrastructure resource types (``_SKIP_RESOURCE_TYPES``) are excluded.

        Returns:
            (additional_distributions, record_sets) — FileObjects are owned by
            the generator and are never returned here.
        """
        additional_distributions: list = []
        record_sets: list = []

        ndjson_by_type: dict = defaultdict(list)
        bundle_metas: list = []

        for file_id, meta in zip(file_ids, file_metas):
            if meta.get("fhir_resource_type") in _SKIP_RESOURCE_TYPES:
                continue
            if "fhir_resource_groups" in meta:
                bundle_metas.append(meta)
            elif "fhir_resource_type" in meta:
                ndjson_by_type[meta["fhir_resource_type"]].append((file_id, meta))

        for resource_type, id_meta_pairs in ndjson_by_type.items():
            chunks = [
                (fid, m)
                for fid, m in id_meta_pairs
                if _is_bulk_chunk(m["file_name"], resource_type)
            ]
            standalone = [
                (fid, m)
                for fid, m in id_meta_pairs
                if not _is_bulk_chunk(m["file_name"], resource_type)
            ]
            if len(chunks) == 1:
                standalone.extend(chunks)
                chunks = []

            for fid, meta in standalone:
                rs_id = sanitize_id(get_clean_record_name(meta["file_name"]))
                num_rows = meta.get("num_rows")
                row_desc = f" ({num_rows} rows)" if num_rows is not None else ""
                record_sets.append(
                    mlc.RecordSet(
                        id=rs_id,
                        name=resource_type,
                        description=f"Records from {meta['file_name']}{row_desc}",
                        fields=self._build_fields(
                            meta["column_types"],
                            rs_id,
                            source_ref={"file_object": fid},
                        ),
                    )
                )

            if chunks:
                _, first_meta = chunks[0]
                fileset_id = f"fhir-{sanitize_id(resource_type)}-files"
                additional_distributions.append(
                    mlc.FileSet(
                        id=fileset_id,
                        name=f"{resource_type} NDJSON files",
                        description=f"{len(chunks)} NDJSON chunk files for FHIR {resource_type}",
                        encoding_formats=[first_meta["encoding_format"]],
                        includes=[
                            m.get("relative_path", m["file_name"]) for _, m in chunks
                        ],
                    )
                )
                merged = merge_fhir_column_types(
                    [fm["column_types"] for _, fm in chunks]
                )
                total_rows = sum(fm.get("num_rows", 0) for _, fm in chunks)
                rs_id = sanitize_id(resource_type)
                record_sets.append(
                    mlc.RecordSet(
                        id=rs_id,
                        name=resource_type,
                        description=f"FHIR {resource_type} from {len(chunks)} NDJSON chunk files ({total_rows} rows)",
                        fields=self._build_fields(
                            merged,
                            rs_id,
                            source_ref={"file_set": fileset_id},
                        ),
                    )
                )

        if bundle_metas:
            by_type_col_types: dict = defaultdict(list)
            by_type_counts: dict = defaultdict(int)
            for fm in bundle_metas:
                for rt, group in fm["fhir_resource_groups"].items():
                    by_type_col_types[rt].append(group["column_types"])
                    by_type_counts[rt] += group["num_rows"]

            bundle_fileset_id = "fhir-bundles"
            additional_distributions.append(
                mlc.FileSet(
                    id=bundle_fileset_id,
                    name="FHIR Bundle files",
                    description=f"{len(bundle_metas)} FHIR Bundle files",
                    encoding_formats=sorted(
                        set(fm["encoding_format"] for fm in bundle_metas)
                    ),
                    includes=[
                        fm.get("relative_path", fm["file_name"]) for fm in bundle_metas
                    ],
                )
            )
            for rt in sorted(by_type_col_types.keys()):
                if rt in _SKIP_RESOURCE_TYPES:
                    continue
                merged = merge_fhir_column_types(by_type_col_types[rt])
                rs_id = sanitize_id(rt)
                total_rows = by_type_counts[rt]
                record_sets.append(
                    mlc.RecordSet(
                        id=rs_id,
                        name=rt,
                        description=f"FHIR {rt} from {len(bundle_metas)} Bundle files ({total_rows} resources)",
                        fields=self._build_fields(
                            merged,
                            rs_id,
                            source_ref={"file_set": bundle_fileset_id},
                            description_prefix=f"FHIR {rt} field",
                        ),
                    )
                )

        if not record_sets:
            logger.warning(
                "build_croissant produced no RecordSets — all FHIR resources were skipped "
                "(resource types: %s)",
                sorted({m.get("fhir_resource_type", "unknown") for m in file_metas}),
            )

        return additional_distributions, record_sets

    def _build_fields(
        self,
        col_schema: dict,
        parent_id: str,
        source_ref: dict,
        description_prefix: str = "Column",
        _col_path_prefix: str = "",
    ) -> list:
        """Delegate to the shared utils implementation."""
        return build_fields_from_json_schema(
            col_schema,
            parent_id,
            source_ref,
            description_prefix=description_prefix,
            _col_path_prefix=_col_path_prefix,
        )
