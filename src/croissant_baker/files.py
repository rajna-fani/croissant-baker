"""File discovery utilities for Croissant Maker."""

import logging
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


def discover_files(dir_path: str) -> List[Path]:
    """
    Recursively discover all files in a directory (skipping and logging hidden directories) and return their relative
    paths.

    Args:
        dir_path: Path to the directory to scan.

    Returns:
        List of relative file paths found in the directory.

    Raises:
        FileNotFoundError: If the directory does not exist or is not a directory.
        PermissionError: If the directory cannot be accessed.
    """
    try:
        directory = Path(dir_path).resolve()
        if not directory.is_dir():
            raise FileNotFoundError(f"{dir_path} is not a directory")

        skipped_count = 0
        skipped_examples = []

        files = []
        for file in directory.rglob("*"):
            if not file.is_file():
                continue

            rel_path = file.relative_to(directory)

            if any(part.startswith(".") for part in rel_path.parts):
                skipped_count += 1
                skipped_examples.append(str(rel_path))
                continue

            files.append(rel_path)

        if skipped_count:
            logger.debug(
                "Skipping %d file(s) in hidden directories. Examples: %s",
                skipped_count,
                skipped_examples,
            )

        return files
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Directory not found: {e}")
    except PermissionError as e:
        raise PermissionError(f"Permission denied: {e}")
