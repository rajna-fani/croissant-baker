"""File handler framework for Croissant Maker."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional


class FileTypeHandler(ABC):
    """Abstract base class for file type handlers."""

    @abstractmethod
    def can_handle(self, file_path: Path) -> bool:
        """
        Check if the handler can process the given file.

        Args:
            file_path: Path to the file.

        Returns:
            True if the handler can process the file, False otherwise.
        """

    @abstractmethod
    def extract_metadata(self, file_path: Path) -> dict:
        """
        Extract metadata from the file.

        Args:
            file_path: Path to the file.

        Returns:
            Metadata extracted from the file as a dictionary.
        """


_registry: list[FileTypeHandler] = []


def register_handler(handler: FileTypeHandler) -> None:
    """
    Register a file type handler.

    Args:
        handler: Handler to register.
    """
    if handler not in _registry:
        _registry.append(handler)


def find_handler(file_path: Path) -> Optional[FileTypeHandler]:
    """
    Find the first handler that can process the given file.

    Args:
        file_path: Path to the file.

    Returns:
        Matching handler or None if no handler is found.
    """
    for handler in _registry:
        if handler.can_handle(file_path):
            return handler
    return None
