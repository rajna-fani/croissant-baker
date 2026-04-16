"""TSV file handler — extends CSVHandler with tab delimiter."""

from pathlib import Path

from croissant_baker.handlers.csv_handler import CSVHandler


class TSVHandler(CSVHandler):
    """
    Handler for TSV and compressed TSV files.

    TSV is structurally identical to CSV with a tab delimiter. This handler
    inherits all type inference, row counting, compression, and Croissant
    generation logic from CSVHandler. The only differences are the file
    extensions it claims and the delimiter passed to PyArrow.

    Supported: .tsv, .tsv.gz, .tsv.bz2, .tsv.xz

    To add another delimiter-separated format (e.g. pipe-separated): subclass
    CSVHandler, override can_handle() and _delimiter(), register the instance
    in registry.py. No other files need to change.
    """

    EXTENSIONS = (".tsv", ".tsv.gz", ".tsv.bz2", ".tsv.xz")
    FORMAT_NAME = "TSV"
    FORMAT_DESCRIPTION = "Column names, inferred types, optional row count"

    def can_handle(self, file_path: Path) -> bool:
        name_lower = file_path.name.lower()
        return (
            file_path.suffix.lower() == ".tsv"
            or name_lower.endswith(".tsv.gz")
            or name_lower.endswith(".tsv.bz2")
            or name_lower.endswith(".tsv.xz")
        )

    @staticmethod
    def _delimiter(file_path: Path) -> str:  # noqa: ARG004
        return "\t"

    @staticmethod
    def _encoding_format(file_path: Path) -> str:
        name_lower = file_path.name.lower()
        if name_lower.endswith(".tsv.gz"):
            return "application/gzip"
        if name_lower.endswith(".tsv.bz2"):
            return "application/x-bzip2"
        if name_lower.endswith(".tsv.xz"):
            return "application/x-xz"
        return "text/tab-separated-values"
