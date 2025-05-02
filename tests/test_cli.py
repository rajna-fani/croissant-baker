"""Tests for Croissant Maker CLI."""

from pathlib import Path
import pytest
from typer.testing import CliRunner
from croissant_maker.__main__ import app
from croissant_maker.handlers import FileTypeHandler, register_handler

runner = CliRunner()


class DummyTextHandler(FileTypeHandler):
    def can_handle(self, file_path: Path) -> bool:
        """Check if the file has a .txt extension."""
        return file_path.suffix == ".txt"

    def extract_metadata(self, file_path: Path) -> dict:
        """Return dummy metadata for testing."""
        return {"type": "text"}


@pytest.fixture
def setup_handlers() -> None:
    """Register the dummy handler and clean up after the test."""
    from croissant_maker.handlers import _registry

    original_registry = _registry.copy()
    register_handler(DummyTextHandler())
    yield
    _registry.clear()
    _registry.extend(original_registry)


def test_cli_valid_directory(tmp_path: Path, setup_handlers: None) -> None:
    """Test CLI with a directory containing files."""
    (tmp_path / "file1.txt").write_text("test")
    (tmp_path / "file2.csv").write_text("test")

    result = runner.invoke(app, [str(tmp_path)])
    assert result.exit_code == 0
    assert "File: file1.txt -> Supported" in result.stdout
    assert "File: file2.csv -> Unsupported" in result.stdout


def test_cli_empty_directory(tmp_path: Path, setup_handlers: None) -> None:
    """Test CLI with an empty directory."""
    result = runner.invoke(app, [str(tmp_path)])
    assert result.exit_code == 0
    assert "No files found in the directory." in result.stdout


def test_cli_nonexistent_directory(tmp_path: Path) -> None:
    """Test CLI with a nonexistent directory."""
    nonexistent = tmp_path / "nonexistent"
    result = runner.invoke(app, [str(nonexistent)])
    assert result.exit_code == 1
    assert "Error: Directory not found" in result.stdout


def test_cli_not_a_directory(tmp_path: Path) -> None:
    """Test CLI with a file instead of a directory."""
    file_path = tmp_path / "file.txt"
    file_path.write_text("test")
    result = runner.invoke(app, [str(file_path)])
    assert result.exit_code == 1
    assert "Error: Directory not found: " in result.stdout
    assert "is not a directory" in result.stdout
