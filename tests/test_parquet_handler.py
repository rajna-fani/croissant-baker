"""Tests for Parquet handler."""

from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from croissant_baker.handlers.parquet_handler import ParquetHandler


@pytest.fixture
def handler() -> ParquetHandler:
    return ParquetHandler()


@pytest.fixture
def sample_parquet(tmp_path: Path) -> Path:
    """Create a minimal Parquet file for testing."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
            "score": pa.array([9.5, 8.3, 7.1], type=pa.float64()),
        }
    )
    path = tmp_path / "test.parquet"
    pq.write_table(table, str(path))
    return path


# ---------------------------------------------------------------------------
# can_handle
# ---------------------------------------------------------------------------


def test_can_handle_parquet(handler: ParquetHandler) -> None:
    """Test Parquet handler file type detection."""
    assert handler.can_handle(Path("data.parquet"))
    assert handler.can_handle(Path("data.PARQUET"))
    assert not handler.can_handle(Path("data.csv"))
    assert not handler.can_handle(Path("data.txt"))


# ---------------------------------------------------------------------------
# extract_metadata
# ---------------------------------------------------------------------------


def test_extract_metadata(handler: ParquetHandler, sample_parquet: Path) -> None:
    """Test Parquet metadata extraction returns correct structure."""
    metadata = handler.extract_metadata(sample_parquet)

    assert metadata["file_name"] == "test.parquet"
    assert metadata["encoding_format"] == "application/vnd.apache.parquet"
    assert metadata["file_size"] > 0
    assert len(metadata["sha256"]) == 64
    assert metadata["num_rows"] == 3
    assert metadata["num_columns"] == 3
    assert metadata["columns"] == ["id", "name", "score"]

    column_types = metadata["column_types"]
    assert column_types["id"] == "cr:Int64"
    assert column_types["name"] == "sc:Text"
    assert column_types["score"] == "cr:Float64"


# ---------------------------------------------------------------------------
# Resource leak: file handle must be closed after extract_metadata (#54)
# ---------------------------------------------------------------------------


def test_parquet_file_handle_closed(
    handler: ParquetHandler, sample_parquet: Path
) -> None:
    """Verify the underlying file handle is closed after metadata extraction.

    Regression test for GitHub issue #54: ParquetFile was opened without a
    context manager, leaking file descriptors until garbage collection.
    """
    captured_handles: list[pq.ParquetFile] = []
    _OrigParquetFile = pq.ParquetFile

    def _spy(*args, **kwargs):
        pf = _OrigParquetFile(*args, **kwargs)
        captured_handles.append(pf)
        return pf

    with patch(
        "croissant_baker.handlers.parquet_handler.ParquetFile", side_effect=_spy
    ):
        handler.extract_metadata(sample_parquet)

    assert len(captured_handles) == 1, "ParquetFile should be opened exactly once"
    assert captured_handles[0].reader.closed, (
        "ParquetFile reader must be closed after extract_metadata returns"
    )


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_extract_metadata_not_found(handler: ParquetHandler) -> None:
    """Test that missing file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        handler.extract_metadata(Path("/nonexistent/data.parquet"))


# ---------------------------------------------------------------------------
# build_croissant
# ---------------------------------------------------------------------------


def _pq_meta(name, rel_path, cols=None):
    return {
        "file_name": name,
        "relative_path": rel_path,
        "column_types": cols or {"id": "sc:Text", "value": "cr:Float64"},
        "num_rows": 10,
        "encoding_format": "application/vnd.apache.parquet",
    }


def test_parquet_build_croissant_standalone(handler: ParquetHandler) -> None:
    filesets, record_sets = handler.build_croissant(
        [_pq_meta("data.parquet", "data.parquet")], ["file_0"]
    )
    assert filesets == []
    assert record_sets[0].name == "data"


def test_parquet_build_croissant_single_file_in_subdir(handler: ParquetHandler) -> None:
    filesets, record_sets = handler.build_croissant(
        [_pq_meta("part-00000.parquet", "events/part-00000.parquet")], ["file_0"]
    )
    assert filesets == []
    assert record_sets[0].name == "events"


def test_parquet_build_croissant_partitioned(handler: ParquetHandler) -> None:
    metas = [
        _pq_meta("part-00000.parquet", "events/part-00000.parquet"),
        _pq_meta("part-00001.parquet", "events/part-00001.parquet"),
    ]
    filesets, record_sets = handler.build_croissant(metas, ["file_0", "file_1"])
    assert len(filesets) == 1
    assert len(record_sets) == 1
    assert record_sets[0].name == "events"
    assert "events/*.parquet" in filesets[0].includes
