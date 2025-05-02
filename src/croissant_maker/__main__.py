"""Command-line interface for Croissant Maker."""

import typer
from pathlib import Path
from .files import discover_files
from .handlers import find_handler

# Create the Typer application instance
app = typer.Typer(
    name="croissant-maker",
    help="A tool to automatically generate Croissant metadata for datasets.",
    add_completion=False,  # Simple version for now
)


@app.command()
def main(
    dir_path: str = typer.Argument(..., help="Directory to scan for dataset files"),
) -> None:
    """
    Scan a directory and identify files that can be processed by registered handlers.

    Args:
        dir_path: Path to the directory containing dataset files.

    Raises:
        typer.Exit: If the directory is invalid or inaccessible, exits with code 1.
    """
    try:
        files = discover_files(dir_path)
        if not files:
            typer.echo("No files found in the directory.")
            return

        for file_path in files:
            handler = find_handler(Path(dir_path) / file_path)
            status = "Supported" if handler else "Unsupported"
            typer.echo(f"File: {file_path} -> {status}")
    except (FileNotFoundError, PermissionError) as e:
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
