"""Tests for file handler framework."""

from pathlib import Path
from croissant_baker.handlers.registry import find_handler, register_all_handlers
from croissant_baker.handlers.csv_handler import CSVHandler
from croissant_baker.handlers.utils import (
    ARRAY_SHAPE_UNKNOWN_1D,
    normalize_array_shape,
)


def test_find_handler_with_real_handlers() -> None:
    """Test finding handlers with registered real handlers."""
    # Register all handlers
    register_all_handlers()

    # Test CSV handler is found
    csv_handler = find_handler(Path("test.csv"))
    assert csv_handler is not None
    assert isinstance(csv_handler, CSVHandler)

    # Test unsupported file returns None
    unsupported_handler = find_handler(Path("test.xyz"))
    assert unsupported_handler is None


def test_handler_registry_isolation() -> None:
    """Test that handler registration doesn't leak between tests."""
    from croissant_baker.handlers.registry import _registry

    initial_count = len(_registry)

    register_all_handlers()

    # Registry should have at least CSV handler
    assert len(_registry) >= initial_count

    # Should find CSV handler
    assert find_handler(Path("data.csv")) is not None


def test_normalize_array_shape_accepts_tuple_and_bare_forms() -> None:
    """Tuple-style shapes (numpy.shape repr) coerce to mlc-accepted form."""
    assert normalize_array_shape(ARRAY_SHAPE_UNKNOWN_1D) == "-1"
    assert normalize_array_shape("-1") == "-1"
    assert normalize_array_shape("(-1,)") == "-1"
    assert normalize_array_shape("(-1, -1)") == "-1,-1"
    assert normalize_array_shape("(28, 28)") == "28,28"
    assert normalize_array_shape("28,28") == "28,28"
    assert normalize_array_shape("-1,-1,3") == "-1,-1,3"
