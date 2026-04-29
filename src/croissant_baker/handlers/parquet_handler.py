"""Parquet file handler for tabular event streams (e.g., MEDS)."""

import logging
from collections import defaultdict
from pathlib import Path

import mlcroissant as mlc
from pyarrow.parquet import ParquetFile

from croissant_baker.handlers.base_handler import FileTypeHandler
from croissant_baker.handlers.utils import (
    _build_fields,
    compute_file_hash,
    get_clean_record_name,
    infer_column_types_from_arrow_schema,
    sanitize_id,
)

logger = logging.getLogger(__name__)

# Apache Parquet file format spec: every Parquet file begins AND ends with
# the 4-byte ASCII magic "PAR1" — the trailing copy is the footer marker.
# A valid file is therefore at least 8 bytes long.
# Reference: https://parquet.apache.org/docs/file-format/
_PARQUET_MAGIC = b"PAR1"
_PARQUET_MAGIC_LEN = len(_PARQUET_MAGIC)


def _has_parquet_magic(file_path: Path) -> bool:
    """Return True iff ``file_path`` has the Parquet magic at start and end.

    Files that fail any check (too small, missing PAR1 header, missing PAR1
    footer) are rejected and a WARNING is logged with the specific reason so
    the user can see which files were skipped and why. Missing or unreadable
    files return False without logging (caller errors, not impostors).
    """
    try:
        size = file_path.stat().st_size
    except OSError:
        return False
    if size < _PARQUET_MAGIC_LEN * 2:
        logger.warning(
            "Skipping %s: file is too small (%d bytes) to be a valid Parquet file",
            file_path,
            size,
        )
        return False
    try:
        with open(file_path, "rb") as f:
            if f.read(_PARQUET_MAGIC_LEN) != _PARQUET_MAGIC:
                logger.warning(
                    "Skipping %s: missing Parquet PAR1 header magic",
                    file_path,
                )
                return False
            f.seek(-_PARQUET_MAGIC_LEN, 2)
            if f.read(_PARQUET_MAGIC_LEN) != _PARQUET_MAGIC:
                logger.warning(
                    "Skipping %s: missing Parquet PAR1 footer magic "
                    "(file may be truncated)",
                    file_path,
                )
                return False
            return True
    except OSError:
        return False


class ParquetHandler(FileTypeHandler):
    """
    Handler for Parquet files (.parquet) with schema-based type inference.

    - Uses pyarrow to read schema and row count without loading full data
    - Emits Croissant-compatible column types via shared map_arrow_type()
    - Computes SHA256 for reproducibility
    - Keeps memory usage minimal (schema-only)
    """

    EXTENSIONS = (".parquet",)
    FORMAT_NAME = "Parquet"
    FORMAT_DESCRIPTION = "Arrow schema, column names and types, row count"

    def can_handle(self, file_path: Path) -> bool:
        if file_path.suffix.lower() != ".parquet":
            return False
        return _has_parquet_magic(file_path)

    def extract_metadata(self, file_path: Path, **kwargs) -> dict:
        """Extract metadata from a Parquet file via pyarrow schema inspection."""
        if not file_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {file_path}")

        try:
            with ParquetFile(str(file_path)) as pq:
                schema = pq.schema_arrow
                num_rows = pq.metadata.num_rows if pq.metadata is not None else 0

                # Use the shared Arrow type mapper (same as CSV handler)
                column_types = infer_column_types_from_arrow_schema(schema)
                columns = [field.name for field in schema]

            file_size = file_path.stat().st_size
            sha256_hash = compute_file_hash(file_path)

            return {
                "file_path": str(file_path),
                "file_name": file_path.name,
                "file_size": file_size,
                "sha256": sha256_hash,
                "encoding_format": "application/vnd.apache.parquet",
                "column_types": column_types,
                "arrow_schema": schema,
                "num_rows": num_rows,
                "num_columns": len(columns),
                "columns": columns,
            }
        except Exception as e:
            raise ValueError(f"Failed to process Parquet file {file_path}: {e}") from e

    def build_croissant(self, file_metas: list, file_ids: list) -> tuple:
        """Build FileSets and RecordSets for all Parquet files in this dataset.

        Groups files by parent directory. Directories with >=2 files become
        partitioned tables (one FileSet + one RecordSet). All other files get
        per-file RecordSets. Root-level files (parent == ".") are never grouped.
        """
        additional_distributions = []
        record_sets = []

        # Group by parent directory to detect partitioned tables.
        # A non-root directory with >=2 .parquet files is treated as one logical
        # table: one FileSet + one RecordSet (schema taken from first partition).
        # Root-level files (parent == ".") and single-file directories are
        # always standalone — one RecordSet per file.
        dir_groups: dict = defaultdict(list)
        for file_id, file_meta in zip(file_ids, file_metas):
            parent = str(Path(file_meta["relative_path"]).parent)
            dir_groups[parent].append((file_id, file_meta))

        for dir_path, pairs in dir_groups.items():
            is_partitioned = len(pairs) >= 2 and dir_path != "."

            if is_partitioned:
                _, first_meta = pairs[0]
                table_name = Path(dir_path).name
                dir_id = sanitize_id(dir_path)
                fileset_id = f"{dir_id}-fileset"

                _suffix = "".join(Path(pairs[0][1]["file_name"]).suffixes)
                additional_distributions.append(
                    mlc.FileSet(
                        id=fileset_id,
                        name=f"{table_name} partition files",
                        description=f"{len(pairs)} Parquet partition files for table '{table_name}'",
                        encoding_formats=["application/vnd.apache.parquet"],
                        includes=[f"{dir_path}/*{_suffix}"],
                    )
                )

                if "arrow_schema" in first_meta:
                    fields = _build_fields(
                        first_meta["arrow_schema"],
                        sanitize_id(table_name),
                        {"file_set": fileset_id},
                    )
                else:
                    fields = []
                    for col_name, col_type in first_meta["column_types"].items():
                        safe_name = sanitize_id(col_name)
                        fields.append(
                            mlc.Field(
                                id=f"{dir_id}/{safe_name}",
                                name=col_name,
                                description=f"Column '{col_name}' from table '{table_name}'",
                                data_types=[col_type],
                                source=mlc.Source(
                                    file_set=fileset_id,
                                    extract=mlc.Extract(column=col_name),
                                ),
                            )
                        )

                num_rows = sum(m.get("num_rows", 0) for _, m in pairs)
                record_sets.append(
                    mlc.RecordSet(
                        id=sanitize_id(table_name),
                        name=table_name,
                        description=f"Partitioned table '{table_name}' ({len(pairs)} Parquet files, {num_rows} total rows)",
                        fields=fields,
                    )
                )
            else:
                # Standalone: one RecordSet per file
                for file_id, file_meta in pairs:
                    rel_dir = str(Path(file_meta["relative_path"]).parent)
                    if rel_dir != ".":
                        rs_name = Path(rel_dir).name
                    else:
                        rs_name = get_clean_record_name(file_meta["file_name"])
                    rs_id = sanitize_id(rs_name)

                    if "arrow_schema" in file_meta:
                        fields = _build_fields(
                            file_meta["arrow_schema"],
                            rs_id,
                            {"file_object": file_id},
                        )
                    else:
                        fields = []
                        for col_name, col_type in file_meta["column_types"].items():
                            safe_name = sanitize_id(col_name)
                            fields.append(
                                mlc.Field(
                                    id=f"{rs_id}/{safe_name}",
                                    name=col_name,
                                    description=f"Column '{col_name}' from {file_meta['file_name']}",
                                    data_types=[col_type],
                                    source=mlc.Source(
                                        file_object=file_id,
                                        extract=mlc.Extract(column=col_name),
                                    ),
                                )
                            )

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

        return additional_distributions, record_sets
