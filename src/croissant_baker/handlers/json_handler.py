"""Generic JSON and JSONL file handler.

Supports four file formats:
- ``.json`` / ``.json.gz``: a JSON array of objects (each object is one row)
  or a single JSON object (treated as one row).
- ``.jsonl`` / ``.jsonl.gz``: newline-delimited JSON (one JSON object per line).

FHIR ``.json`` / ``.json.gz`` files (those containing ``"resourceType": "<UpperCase…"``)
are intentionally excluded — they are claimed by FHIRHandler instead.
"""

import json
import logging
import re
from pathlib import Path

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

# Pattern that identifies a FHIR JSON file — resourceType value starts with uppercase.
_FHIR_PATTERN = re.compile(r'"resourceType"\s*:\s*"[A-Z]')


class JSONHandler(FileTypeHandler):
    """Handler for generic JSON and JSONL datasets.

    Detection strategy:
    - ``.jsonl`` / ``.jsonl.gz``: always accepted (FHIR uses ``.ndjson``, not ``.jsonl``).
    - ``.json`` / ``.json.gz``: accepted only when the first 4 KB does NOT match the
      FHIR resourceType pattern and the content starts with ``[`` (array) or ``{`` (object).
    """

    def can_handle(self, file_path: Path) -> bool:
        name = file_path.name.lower()
        if name.endswith(".jsonl") or name.endswith(".jsonl.gz"):
            return True
        if name.endswith(".json") or name.endswith(".json.gz"):
            return self._sniff_json(file_path)
        return False

    def _sniff_json(self, file_path: Path) -> bool:
        """Peek at the first 4 KB to confirm the file is non-FHIR JSON.

        FHIR top-level objects always start with ``{``, so the FHIR exclusion
        check is only applied to ``{``-rooted content. Arrays are always
        accepted — a nested ``resourceType`` key inside an array element is
        not a FHIR document.
        """
        try:
            with open_text_file(file_path) as fh:
                head = fh.read(4096)
            head = head.strip()
            if head.startswith("{"):
                return not _FHIR_PATTERN.search(head)
            return head.startswith("[")
        except (OSError, UnicodeDecodeError):
            return False

    def extract_metadata(self, file_path: Path, **kwargs) -> dict:
        """Extract metadata from a JSON or JSONL file.

        Returns a dict with keys:
            file_path, file_name, file_size, sha256, encoding_format,
            column_types, columns, num_columns, num_rows.

        Raises:
            ValueError: If the file cannot be parsed or contains no records.
        """
        sha256 = compute_file_hash(file_path)
        file_size = file_path.stat().st_size
        file_name = file_path.name
        name_lower = file_name.lower()

        is_gz = name_lower.endswith(".gz")
        is_jsonl = name_lower.endswith(".jsonl") or name_lower.endswith(".jsonl.gz")

        if is_jsonl:
            return self._extract_jsonl(file_path, file_name, sha256, file_size, is_gz)
        return self._extract_json(file_path, file_name, sha256, file_size, is_gz)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _extract_jsonl(
        self,
        file_path: Path,
        file_name: str,
        sha256: str,
        file_size: int,
        is_gz: bool,
    ) -> dict:
        """Stream a JSONL file line-by-line.

        Collects up to ``SCHEMA_SAMPLE`` records for schema inference while
        counting all rows.
        """
        schema_samples: list = []
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
                if not isinstance(obj, dict):
                    continue
                num_rows += 1
                if len(schema_samples) < SCHEMA_SAMPLE:
                    schema_samples.append(obj)

        if num_rows == 0:
            raise ValueError(f"No valid JSON objects found in {file_name}")

        if num_rows > SCHEMA_SAMPLE:
            logger.warning(
                "Sampled %d of %d records for schema inference in %s — rare fields may be missing",
                SCHEMA_SAMPLE,
                num_rows,
                file_name,
            )

        column_types = infer_json_schema(schema_samples, _top_level=False)
        encoding = "application/gzip" if is_gz else "application/jsonl"
        return {
            "file_path": str(file_path),
            "file_name": file_name,
            "file_size": file_size,
            "sha256": sha256,
            "encoding_format": encoding,
            "column_types": column_types,
            "columns": list(column_types.keys()),
            "num_columns": len(column_types),
            "num_rows": num_rows,
        }

    def _extract_json(
        self,
        file_path: Path,
        file_name: str,
        sha256: str,
        file_size: int,
        is_gz: bool,
    ) -> dict:
        """Load a ``.json`` file entirely.

        - JSON array  → each element is a row (only dicts are kept).
        - JSON object → treated as a single row.
        """
        with open_text_file(file_path) as fh:
            try:
                doc = json.load(fh)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Cannot parse {file_name} as JSON: {exc}") from exc

        if isinstance(doc, list):
            rows = [r for r in doc if isinstance(r, dict)]
        elif isinstance(doc, dict):
            rows = [doc]
        else:
            raise ValueError(f"{file_name} is not a JSON object or array of objects")

        if not rows:
            raise ValueError(f"No valid JSON objects found in {file_name}")

        num_rows = len(rows)
        schema_samples = rows[:SCHEMA_SAMPLE]

        if num_rows > SCHEMA_SAMPLE:
            logger.warning(
                "Sampled %d of %d records for schema inference in %s — rare fields may be missing",
                SCHEMA_SAMPLE,
                num_rows,
                file_name,
            )

        column_types = infer_json_schema(schema_samples, _top_level=False)
        encoding = "application/gzip" if is_gz else "application/json"
        return {
            "file_path": str(file_path),
            "file_name": file_name,
            "file_size": file_size,
            "sha256": sha256,
            "encoding_format": encoding,
            "column_types": column_types,
            "columns": list(column_types.keys()),
            "num_columns": len(column_types),
            "num_rows": num_rows,
        }

    def build_croissant(self, file_metas: list, file_ids: list) -> tuple:
        """Build Croissant RecordSets for all JSON/JSONL files.

        One RecordSet per file; no additional FileSet distributions are
        created (each file becomes its own FileObject, owned by the generator).

        Returns:
            ([], record_sets) — no additional distributions.
        """
        record_sets: list = []

        for file_id, meta in zip(file_ids, file_metas):
            file_name = meta.get("file_name", "unknown")
            rs_id = sanitize_id(get_clean_record_name(file_name))
            num_rows = meta.get("num_rows")
            row_desc = f" ({num_rows} rows)" if num_rows is not None else ""
            record_sets.append(
                mlc.RecordSet(
                    id=rs_id,
                    name=rs_id,
                    description=f"Records from {file_name}{row_desc}",
                    fields=build_fields_from_json_schema(
                        meta["column_types"],
                        rs_id,
                        source_ref={"file_object": file_id},
                    ),
                )
            )

        return [], record_sets
