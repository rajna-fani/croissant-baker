"""CSV file handler for tabular data processing."""

import logging
import re
from pathlib import Path

import mlcroissant as mlc
import pyarrow as pa
import pyarrow.csv as pa_csv

from croissant_baker.handlers.base_handler import FileTypeHandler
from croissant_baker.handlers.utils import (
    compute_file_hash,
    get_clean_record_name,
    infer_column_types_from_arrow_schema,
    make_field_id,
    make_record_set_ids,
)

logger = logging.getLogger(__name__)

# Pattern for extracting column index and inferred type from an ArrowInvalid
# exception message. PyArrow exposes no structured attributes on ArrowInvalid
# (verified PyArrow 19.0.1: only .args, .add_note, .with_traceback), so the
# message string is the only source of column information.
#
# Two alternatives were evaluated and ruled out:
#   DuckDB  — automatic type promotion with no regex, but does not support
#             .csv.bz2 or .csv.xz (upstream issue duckdb/duckdb#12232, open
#             as of 2026-04). Keeping both libraries would add ~40 MB for a
#             net capability regression.
#   Polars  — structured Schema object, no message parsing. However,
#             collect_schema() on a 35 MB .csv.gz uses ~200 MB RSS vs ~11 MB
#             for PyArrow's block-based streaming reader. Unacceptable at
#             MIMIC-IV scale. If .bz2/.xz support is ever dropped, DuckDB's
#             read_csv_auto is the right long-term replacement.
_ARROW_COL_RE = re.compile(r"In CSV column #(\d+): CSV conversion error to (\w+)")

# Max promotions before falling back to all-string types. One retry per conflicting
# column; beyond this we read everything as strings to bound I/O.
_MAX_TYPE_CONFLICT_RETRIES = 50


class CSVHandler(FileTypeHandler):
    """
    Handler for CSV and compressed CSV files with automatic type inference.

    Supports:
    - Standard CSV files (.csv)
    - Gzip-compressed CSV files (.csv.gz)
    - Bzip2-compressed CSV files (.csv.bz2)
    - XZ-compressed CSV files (.csv.xz)
    - Automatic column type detection using PyArrow
    - SHA256 hash computation for file integrity

    Uses PyArrow's streaming CSV reader (open_csv) which:
    - Auto-detects compressed formats from filename extension
    - Infers precise types (timestamp[s], date32, int64, float64, etc.)
    - Streams data for constant memory usage regardless of file size

    Type inference works in two stages:
    1. PyArrow infers column types from the first block of data.
    2. If a later block contains values incompatible with the inferred type
       (e.g. a float in an integer column), the affected column is promoted
       to a wider type and the file is re-read. Integer columns are first
       widened to float64; any remaining conflicts fall back to string.
       Only the conflicting column is overridden — all others keep their
       inferred types. If the conflicting column cannot be identified, the
       file falls back to all-string types to preserve correctness.

    Subclass this to support other delimiter-separated formats — override
    can_handle(), _delimiter(), and _encoding_format(). See TSVHandler.
    """

    EXTENSIONS = (".csv", ".csv.gz", ".csv.bz2", ".csv.xz")
    FORMAT_NAME = "CSV"
    FORMAT_DESCRIPTION = "Column names, inferred types, optional row count"

    # Common timestamp formats for medical/clinical data beyond ISO-8601.
    # PyArrow uses ISO8601 by default; these cover additional patterns found
    # in datasets like MIMIC, eICU, and OMOP.
    _TIMESTAMP_PARSERS = [
        pa_csv.ISO8601,
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]

    # ------------------------------------------------------------------
    # Streaming with per-column type promotion
    # ------------------------------------------------------------------

    def _stream_csv(self, file_path: Path, count_rows: bool = False):
        """Return (column_types, columns, num_rows) by streaming the CSV."""
        overrides: dict = {}
        col_names: list | None = None
        delimiter = self._delimiter(file_path)

        for _ in range(_MAX_TYPE_CONFLICT_RETRIES):
            opts = pa_csv.ConvertOptions(
                timestamp_parsers=self._TIMESTAMP_PARSERS,
                column_types=overrides or None,
            )
            try:
                result = self._read_streaming(
                    file_path, opts, count_rows=count_rows, delimiter=delimiter
                )
                if overrides:
                    logger.info(
                        "%s: promoted %d column(s) due to type conflicts",
                        file_path.name,
                        len(overrides),
                    )
                return result
            except pa.lib.ArrowInvalid as exc:
                if col_names is None:
                    col_names = self._header(file_path, delimiter=delimiter)

                idx, inferred = self._parse_conflict(str(exc))

                if idx is not None and idx < len(col_names):
                    # Known conflict: promote the specific column.
                    name = col_names[idx]
                    # int/uint → float64 preserves numeric; other types → string.
                    if inferred.startswith(("int", "uint")) and name not in overrides:
                        overrides[name] = pa.float64()
                    else:
                        overrides[name] = pa.string()
                    logger.debug(
                        "%s: promoted column '%s' to %s",
                        file_path.name,
                        name,
                        overrides[name],
                    )
                else:
                    break

        # Last resort: read everything as strings.
        if col_names is None:
            col_names = self._header(file_path, delimiter=delimiter)
        if len(overrides) >= _MAX_TYPE_CONFLICT_RETRIES:
            logger.warning(
                "%s: hit type conflict limit (%d), falling back to all-string types",
                file_path.name,
                _MAX_TYPE_CONFLICT_RETRIES,
            )
        else:
            logger.warning(
                "%s: falling back to all-string types (could not parse type conflict)",
                file_path.name,
            )
        opts = pa_csv.ConvertOptions(
            column_types={n: pa.string() for n in col_names},
        )
        return self._read_streaming(
            file_path, opts, count_rows=count_rows, delimiter=delimiter
        )

    @staticmethod
    def _read_streaming(
        file_path: Path,
        convert_options,
        count_rows: bool = False,
        delimiter: str = ",",
    ):
        """Open a streaming reader, extract schema, and optionally count rows.

        Uses a context manager so the file descriptor is released immediately
        on exit — whether count_rows is True or False. Without it, CPython's
        reference-counting GC closes the fd on function return, but this is not
        guaranteed under PyPy or when an exception traceback holds a reference
        to the local frame. CSVStreamingReader implements __enter__/__exit__
        and has done so since PyArrow 3.x.
        """
        parse_options = pa_csv.ParseOptions(delimiter=delimiter)
        try:
            reader_cm = pa_csv.open_csv(
                str(file_path),
                convert_options=convert_options,
                parse_options=parse_options,
            )
        except UnicodeDecodeError as exc:
            raise ValueError(f"Encoding error in {file_path}: {exc}")

        with reader_cm as reader:
            schema = reader.schema
            column_types = infer_column_types_from_arrow_schema(schema)
            columns = schema.names
            num_rows = sum(batch.num_rows for batch in reader) if count_rows else None

        return column_types, columns, num_rows

    @staticmethod
    def _parse_conflict(msg: str):
        """Extract (column_index, inferred_type) from an ArrowInvalid message.

        Returns (None, None) when the message doesn't match the known format.
        The caller then falls back to all-string types for correctness.
        """
        m = _ARROW_COL_RE.search(msg)
        return (int(m.group(1)), m.group(2)) if m else (None, None)

    @staticmethod
    def _header(file_path: Path, delimiter: str = ",") -> list[str]:
        with pa_csv.open_csv(
            str(file_path),
            parse_options=pa_csv.ParseOptions(delimiter=delimiter),
        ) as reader:
            return reader.schema.names

    @staticmethod
    def _delimiter(file_path: Path) -> str:  # noqa: ARG004
        """Return the field delimiter for this file. Override in subclasses."""
        return ","

    @staticmethod
    def _encoding_format(file_path: Path) -> str:
        """Return the IANA media type for this file. Override in subclasses."""
        name_lower = file_path.name.lower()
        if name_lower.endswith(".csv.gz"):
            return "application/gzip"
        if name_lower.endswith(".csv.bz2"):
            return "application/x-bzip2"
        if name_lower.endswith(".csv.xz"):
            return "application/x-xz"
        return "text/csv"

    # ------------------------------------------------------------------
    # FileTypeHandler interface
    # ------------------------------------------------------------------

    def can_handle(self, file_path: Path) -> bool:
        """
        Check if the file is a CSV or compressed CSV file.

        Args:
            file_path: Path to check

        Returns:
            True if file has supported CSV extension
        """
        name_lower = file_path.name.lower()
        return (
            file_path.suffix.lower() == ".csv"
            or name_lower.endswith(".csv.gz")
            or name_lower.endswith(".csv.bz2")
            or name_lower.endswith(".csv.xz")
        )

    def extract_metadata(
        self, file_path: Path, count_rows: bool = False, **kwargs
    ) -> dict:
        """
        Extract comprehensive metadata from a CSV file.

        Uses PyArrow to read the CSV with automatic type inference,
        including timestamp detection and precise numeric types.

        Args:
            file_path: Path to the CSV file
            count_rows: If True, scan entire file for exact row count.
                        Defaults to False for performance (returns num_rows=None).

        Returns:
            Dictionary containing:
            - Basic file info (path, name, size, hash)
            - Format information (encoding)
            - Data structure (columns, types, row count)

        Raises:
            ValueError: If the CSV file cannot be read or processed
            FileNotFoundError: If the file doesn't exist
        """
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        column_types, columns, num_rows = self._stream_csv(
            file_path, count_rows=count_rows
        )

        if count_rows and num_rows == 0:
            raise ValueError(f"CSV file is empty: {file_path}")

        # Extract file properties
        file_size = file_path.stat().st_size
        sha256_hash = compute_file_hash(file_path)

        # Determine encoding format based on file extension
        encoding_format = self._encoding_format(file_path)

        return {
            "file_path": str(file_path),
            "file_name": file_path.name,
            "file_size": file_size,
            "sha256": sha256_hash,
            "encoding_format": encoding_format,
            "column_types": column_types,
            "num_rows": num_rows,
            "num_columns": len(columns),
            "columns": columns,
        }

    def build_croissant(self, file_metas: list, file_ids: list) -> tuple:
        record_sets = []
        rs_ids = make_record_set_ids(file_metas)
        for file_id, file_meta, rs_id in zip(file_ids, file_metas, rs_ids):
            rs_name = get_clean_record_name(file_meta["file_name"])

            used_field_ids: set = set()
            fields = []
            for col_name, col_type in file_meta["column_types"].items():
                field_id = make_field_id(rs_id, col_name, used_field_ids)
                field = mlc.Field(
                    id=field_id,
                    name=col_name,
                    description=f"Column '{col_name}' from {file_meta['file_name']}",
                    data_types=[col_type],
                    source=mlc.Source(
                        file_object=file_id,
                        extract=mlc.Extract(column=col_name),
                    ),
                )
                fields.append(field)

            num_rows = file_meta.get("num_rows")
            row_desc = f" ({num_rows} rows)" if num_rows is not None else ""
            record_sets.append(
                mlc.RecordSet(
                    id=rs_id,
                    name=rs_name,
                    description=f"Records from {file_meta['file_name']}{row_desc}",
                    fields=fields,
                )
            )

        return [], record_sets
