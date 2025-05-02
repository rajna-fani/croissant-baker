"""Tests for file handler framework."""

from pathlib import Path
from croissant_maker.handlers import FileTypeHandler, register_handler, find_handler


class DummyHandler(FileTypeHandler):
    def can_handle(self, file_path: Path) -> bool:
        return file_path.suffix == ".txt"

    def extract_metadata(self, file_path: Path) -> dict:
        return {"type": "text"}


class DummyHandler2(FileTypeHandler):
    def can_handle(self, file_path: Path) -> bool:
        return file_path.suffix == ".csv"

    def extract_metadata(self, file_path: Path) -> dict:
        return {"type": "csv"}


def test_register_and_find_handler() -> None:
    """Test registering handlers and finding the correct one."""
    handler1 = DummyHandler()
    handler2 = DummyHandler2()
    register_handler(handler1)
    register_handler(handler2)

    assert find_handler(Path("test.txt")) == handler1
    assert find_handler(Path("test.csv")) == handler2
    assert find_handler(Path("test.jpg")) is None


def test_find_handler_empty_registry() -> None:
    """Test find_handler returns None with empty registry."""
    from croissant_maker.handlers import _registry

    original_registry = _registry.copy()
    _registry.clear()

    assert find_handler(Path("test.txt")) is None

    _registry.extend(original_registry)
