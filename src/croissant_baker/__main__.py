"""Command-line interface for Croissant Baker."""

import csv
from datetime import datetime
import json
import tempfile
import importlib.metadata
from pathlib import Path
from typing import List, Optional

import typer
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
)

from croissant_baker.metadata_generator import MetadataGenerator, serialize_datetime
from croissant_baker.files import discover_files
from croissant_baker.handlers.registry import find_handler
import mlcroissant as mlc

# Create the Typer application instance
app = typer.Typer(
    name="croissant-baker",
    help="🥐 Generate Croissant metadata for datasets with automatic type inference",
    add_completion=False,
    rich_markup_mode="markdown",
)


def _save_dict(metadata_dict: dict, output_path: str, validate: bool) -> None:
    """
    Save a pre-computed metadata dict to a JSON-LD file, with optional validation.

    This function exists because MetadataGenerator.save_metadata() always calls
    generate_metadata() internally, regenerating the dict from scratch. That makes
    it unusable once the dict has already been built and modified — for example,
    after RAI attributes have been injected via inject_rai(). This function takes
    the already-computed dict and handles the save + validation step directly,
    keeping MetadataGenerator unchanged.

    It is used in two places:
      - The main generate command, when --rai-config is provided (or not, to keep
        a single consistent save path after generate_metadata() is called once).
      - The rai-apply command, which loads an existing .jsonld, injects RAI, and
        saves it back without invoking MetadataGenerator at all.

    Args:
        metadata_dict: Already-computed Croissant metadata dict (may include RAI).
        output_path:   Path where the JSON-LD file should be written.
        validate:      When True, validates via mlcroissant before writing.

    Raises:
        ValueError: If mlcroissant validation fails.
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if validate:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".jsonld", delete=False
        ) as tmp:
            json.dump(
                metadata_dict,
                tmp,
                indent=2,
                ensure_ascii=False,
                default=serialize_datetime,
            )
            tmp_path = tmp.name
        try:
            mlc.Dataset(tmp_path)
            _write_jsonld(metadata_dict, output_file)
        except Exception as e:
            raise ValueError(f"Validation failed: {e}")
        finally:
            Path(tmp_path).unlink(missing_ok=True)
    else:
        _write_jsonld(metadata_dict, output_file)


def _write_jsonld(metadata_dict: dict, output_file: Path) -> None:
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(
            metadata_dict, f, indent=2, ensure_ascii=False, default=serialize_datetime
        )
        f.write("\n")


def _get_version() -> str:
    """Get version from package metadata."""
    try:
        return importlib.metadata.version("croissant-baker")
    except importlib.metadata.PackageNotFoundError:
        return "unknown (not installed as package)"


def _get_default_output_name(input_path: str) -> str:
    """Generate default output filename based on input path."""
    dataset_name = Path(input_path).name
    return f"{dataset_name}-croissant.jsonld"


# Croissant spec: these CLI flags map to fields that *must* be specified for every dataset.
# https://docs.mlcommons.org/croissant/docs/croissant-spec.html
# The tool generates defaults for all of them, but warns when user hasn't explicitly set them.
_SPEC_REQUIRED_FLAGS = {
    "creator": "--creator",
    "description": "--description",
    "url": "--url",
    "license": "--license",
    "date_published": "--date-published",
}

_RAI_CONFORMS_TO = "http://mlcommons.org/croissant/RAI/1.0"


def _warn_missing_spec_fields(**provided: object) -> None:
    """Warn about spec-required fields that were not explicitly provided."""
    missing = [
        flag for key, flag in _SPEC_REQUIRED_FLAGS.items() if not provided.get(key)
    ]
    if missing:
        typer.echo(
            f"\nWarning: {', '.join(missing)} are required by the Croissant spec but were not provided.\n"
            "  The tool used defaults — review them before publishing.\n"
            "  See: https://docs.mlcommons.org/croissant/docs/croissant-spec.html",
            err=True,
        )


def _normalize_optional_text(value: Optional[str]) -> Optional[str]:
    """Strip a text option and collapse empty strings to None."""
    if value is None:
        return None
    value = value.strip()
    return value or None


def _normalize_optional_text_list(values: Optional[List[str]]) -> Optional[List[str]]:
    """Strip repeated text options and drop empty items."""
    if not values:
        return None
    normalized = [value.strip() for value in values if value and value.strip()]
    return normalized or None


def _validate_iso_datetimes(option_name: str, values: Optional[List[str]]) -> None:
    """Validate repeated date/datetime options and raise a CLI-friendly error."""
    if not values:
        return
    for value in values:
        try:
            datetime.fromisoformat(value)
        except ValueError as e:
            raise ValueError(
                f"Invalid date format for {option_name}: '{value}'. "
                "Expected ISO format like '2023-12-15' or '2023-12-15T10:30:00'. "
                f"Error: {e}"
            )


def _build_native_rai_fields(
    *,
    rai_data_collection: Optional[str],
    rai_data_collection_type: Optional[List[str]],
    rai_data_collection_missing_data: Optional[str],
    rai_data_collection_raw_data: Optional[str],
    rai_data_collection_timeframe: Optional[List[str]],
    rai_data_imputation_protocol: Optional[str],
    rai_data_preprocessing_protocol: Optional[List[str]],
    rai_data_manipulation_protocol: Optional[str],
    rai_data_annotation_protocol: Optional[List[str]],
    rai_data_annotation_platform: Optional[List[str]],
    rai_data_annotation_analysis: Optional[List[str]],
    rai_annotations_per_item: Optional[str],
    rai_annotator_demographics: Optional[List[str]],
    rai_machine_annotation_tools: Optional[List[str]],
    rai_data_biases: Optional[List[str]],
    rai_data_use_cases: Optional[List[str]],
    rai_data_limitations: Optional[List[str]],
    rai_data_social_impact: Optional[str],
    rai_personal_sensitive_information: Optional[List[str]],
    rai_data_release_maintenance_plan: Optional[str],
) -> dict[str, object]:
    """Collect native mlcroissant RAI fields from CLI options."""
    rai_fields: dict[str, object] = {
        "data_collection": _normalize_optional_text(rai_data_collection),
        "data_collection_type": _normalize_optional_text_list(rai_data_collection_type),
        "data_collection_missing_data": _normalize_optional_text(
            rai_data_collection_missing_data
        ),
        "data_collection_raw_data": _normalize_optional_text(
            rai_data_collection_raw_data
        ),
        "data_collection_timeframe": _normalize_optional_text_list(
            rai_data_collection_timeframe
        ),
        "data_imputation_protocol": _normalize_optional_text(
            rai_data_imputation_protocol
        ),
        "data_preprocessing_protocol": _normalize_optional_text_list(
            rai_data_preprocessing_protocol
        ),
        "data_manipulation_protocol": _normalize_optional_text(
            rai_data_manipulation_protocol
        ),
        "data_annotation_protocol": _normalize_optional_text_list(
            rai_data_annotation_protocol
        ),
        "data_annotation_platform": _normalize_optional_text_list(
            rai_data_annotation_platform
        ),
        "data_annotation_analysis": _normalize_optional_text_list(
            rai_data_annotation_analysis
        ),
        "annotations_per_item": _normalize_optional_text(rai_annotations_per_item),
        "annotator_demographics": _normalize_optional_text_list(
            rai_annotator_demographics
        ),
        "machine_annotation_tools": _normalize_optional_text_list(
            rai_machine_annotation_tools
        ),
        "data_biases": _normalize_optional_text_list(rai_data_biases),
        "data_use_cases": _normalize_optional_text_list(rai_data_use_cases),
        "data_limitations": _normalize_optional_text_list(rai_data_limitations),
        "data_social_impact": _normalize_optional_text(rai_data_social_impact),
        "personal_sensitive_information": _normalize_optional_text_list(
            rai_personal_sensitive_information
        ),
        "data_release_maintenance_plan": _normalize_optional_text(
            rai_data_release_maintenance_plan
        ),
    }
    _validate_iso_datetimes(
        "--rai-data-collection-timeframe", rai_fields["data_collection_timeframe"]
    )
    return {key: value for key, value in rai_fields.items() if value is not None}


def _ensure_rai_conforms_to(metadata_dict: dict, force: bool = False) -> None:
    """Declare the RAI spec when RAI metadata is present or explicitly requested."""
    if not force and not any(key.startswith("rai:") for key in metadata_dict):
        return

    conforms_to = metadata_dict.get("conformsTo")
    if conforms_to is None:
        metadata_dict["conformsTo"] = [_RAI_CONFORMS_TO]
        return
    if isinstance(conforms_to, str):
        metadata_dict["conformsTo"] = (
            [conforms_to]
            if conforms_to == _RAI_CONFORMS_TO
            else [conforms_to, _RAI_CONFORMS_TO]
        )
        return
    if isinstance(conforms_to, list) and _RAI_CONFORMS_TO not in conforms_to:
        conforms_to.append(_RAI_CONFORMS_TO)


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    input: str = typer.Option(
        None, "--input", "-i", help="Directory containing dataset files"
    ),
    output: str = typer.Option(None, "--output", "-o", help="Output file path"),
    validate: bool = typer.Option(
        True, "--validate/--no-validate", help="Validate metadata before saving"
    ),
    version: bool = typer.Option(False, "--version", help="Show version and exit"),
    # Metadata override options
    name: Optional[str] = typer.Option(
        None, "--name", help="Dataset name (defaults to directory name)"
    ),
    description: Optional[str] = typer.Option(
        None, "--description", help="Dataset description"
    ),
    url: Optional[str] = typer.Option(
        None, "--url", help="Dataset URL (e.g., https://example.com/dataset)"
    ),
    license: Optional[str] = typer.Option(
        None, "--license", help="License URL or SPDX identifier (e.g., CC-BY-4.0)"
    ),
    citation: Optional[str] = typer.Option(
        None, "--citation", help="Citation text (preferably BibTeX format)"
    ),
    dataset_version: Optional[str] = typer.Option(
        None, "--dataset-version", help="Dataset version (e.g., 1.0.0)"
    ),
    date_published: Optional[str] = typer.Option(
        None,
        "--date-published",
        help="Publication date (e.g., 2023-12-15 or 2023-12-15T10:30:00)",
    ),
    # Creator information following mlcroissant specification
    # Spec: creator is REQUIRED with cardinality MANY (supports multiple creators)
    # ExpectedType: Organization OR Person with flexible properties (name, email, url)
    # Examples: --creator "John Doe" --creator "Jane Smith,jane@example.com,https://jane.com"
    creator: Optional[List[str]] = typer.Option(
        None,
        "--creator",
        help="Creator information. Format: 'Name[,Email[,URL]]'. Use multiple times for multiple creators. Examples: --creator 'John Doe' --creator 'Jane Smith,jane@example.com,https://jane.com'",
    ),
    count_csv_rows: bool = typer.Option(
        False,
        "--count-csv-rows",
        help="Count exact row numbers for CSV files (slow for large datasets)",
    ),
    # Native mlcroissant RAI fields exposed directly as CLI flags.
    rai_data_collection: Optional[str] = typer.Option(
        None, "--rai-data-collection", help="How and where the data was gathered."
    ),
    rai_data_collection_type: Optional[List[str]] = typer.Option(
        None,
        "--rai-data-collection-type",
        help="Collection type, e.g. 'observational'. Can be used multiple times.",
    ),
    rai_data_collection_missing_data: Optional[str] = typer.Option(
        None,
        "--rai-data-collection-missing-data",
        help="How missing data was handled during collection.",
    ),
    rai_data_collection_raw_data: Optional[str] = typer.Option(
        None,
        "--rai-data-collection-raw-data",
        help="Description of the raw data before processing.",
    ),
    rai_data_collection_timeframe: Optional[List[str]] = typer.Option(
        None,
        "--rai-data-collection-timeframe",
        help=("Collection date or datetime in ISO format. Can be used multiple times."),
    ),
    rai_data_imputation_protocol: Optional[str] = typer.Option(
        None,
        "--rai-data-imputation-protocol",
        help="How missing values were imputed.",
    ),
    rai_data_preprocessing_protocol: Optional[List[str]] = typer.Option(
        None,
        "--rai-data-preprocessing-protocol",
        help="Preprocessing step. Can be used multiple times.",
    ),
    rai_data_manipulation_protocol: Optional[str] = typer.Option(
        None,
        "--rai-data-manipulation-protocol",
        help="Transformations applied to the data.",
    ),
    rai_data_annotation_protocol: Optional[List[str]] = typer.Option(
        None,
        "--rai-data-annotation-protocol",
        help="Annotation procedure. Can be used multiple times.",
    ),
    rai_data_annotation_platform: Optional[List[str]] = typer.Option(
        None,
        "--rai-data-annotation-platform",
        help="Annotation platform or tool. Can be used multiple times.",
    ),
    rai_data_annotation_analysis: Optional[List[str]] = typer.Option(
        None,
        "--rai-data-annotation-analysis",
        help="Annotation quality or agreement analysis. Can be used multiple times.",
    ),
    rai_annotations_per_item: Optional[str] = typer.Option(
        None,
        "--rai-annotations-per-item",
        help="Annotation density, e.g. '3 annotators per item'.",
    ),
    rai_annotator_demographics: Optional[List[str]] = typer.Option(
        None,
        "--rai-annotator-demographics",
        help="Annotator demographic note. Can be used multiple times.",
    ),
    rai_machine_annotation_tools: Optional[List[str]] = typer.Option(
        None,
        "--rai-machine-annotation-tools",
        help="Automated annotation tool. Can be used multiple times.",
    ),
    rai_data_biases: Optional[List[str]] = typer.Option(
        None,
        "--rai-data-biases",
        help="Known bias description. Can be used multiple times.",
    ),
    rai_data_use_cases: Optional[List[str]] = typer.Option(
        None,
        "--rai-data-use-cases",
        help="Intended use case. Can be used multiple times.",
    ),
    rai_data_limitations: Optional[List[str]] = typer.Option(
        None,
        "--rai-data-limitations",
        help="Known limitation. Can be used multiple times.",
    ),
    rai_data_social_impact: Optional[str] = typer.Option(
        None,
        "--rai-data-social-impact",
        help="Potential social impact of using the dataset.",
    ),
    rai_personal_sensitive_information: Optional[List[str]] = typer.Option(
        None,
        "--rai-personal-sensitive-information",
        help="Sensitive information note. Can be used multiple times.",
    ),
    rai_data_release_maintenance_plan: Optional[str] = typer.Option(
        None,
        "--rai-data-release-maintenance-plan",
        help="How the dataset release will be maintained over time.",
    ),
    rai_config: Optional[Path] = typer.Option(
        None,
        "--rai-config",
        help="Path to a RAI config YAML file (see rai-example.yaml for the template)",
        exists=True,
        dir_okay=False,
    ),
    include: Optional[List[str]] = typer.Option(
        None,
        "--include",
        "-I",
        help="Glob pattern to include (e.g., '*.csv'). Can be used multiple times.",
    ),
    exclude: Optional[List[str]] = typer.Option(
        None,
        "--exclude",
        "-E",
        help="Glob pattern to exclude (e.g., '*.tmp'). Can be used multiple times.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Perform a dry run to list matching files without generating metadata.",
    ),
) -> None:
    """🥐 **Croissant Baker** - Generate rich metadata for your datasets"""

    if version:
        typer.echo(f"🥐 croissant-baker {_get_version()}")
        return

    if ctx.invoked_subcommand is not None:
        return

    if not input:
        typer.echo("croissant-baker: try 'croissant-baker --help' for more information")
        typer.echo("")
        typer.echo("Usage: croissant-baker --input <dataset-path> [--output <file>]")
        typer.echo("       croissant-baker validate <file>")
        typer.echo(
            "       croissant-baker rai-apply <file.jsonld> --rai-config rai.yaml"
        )
        typer.echo("       croissant-baker --version")
        typer.echo("       croissant-baker --help")
        return

    if not output and not dry_run:
        output = _get_default_output_name(input)
        typer.echo(f"Auto-generated output filename: {output}")

    # Validate required fields and input path
    # 1. Dataset path must exist and be a directory
    dataset_path_obj = Path(input)
    if not dataset_path_obj.is_dir():
        typer.echo(f"Error: Dataset path '{input}' is not a directory", err=True)
        raise typer.Exit(code=1)

    # 2. At least one creator required by the Croissant spec (cardinality MANY)
    if not creator and not dry_run:
        typer.echo(
            "Error: At least one '--creator' option is required "
            "to comply with the Croissant specification.",
            err=True,
        )
        typer.echo(
            "Example: --creator 'John Doe,john@example.com' or --creator 'Jane Smith'",
            err=True,
        )
        raise typer.Exit(code=1)

    # Dry run: list files that would be processed, then exit
    if dry_run:
        try:
            all_files = discover_files(
                input, include_patterns=include, exclude_patterns=exclude
            )
            matched_files = [f for f in all_files if find_handler(Path(input) / f)]
            typer.echo(
                f"Dry run: {len(matched_files)} file(s) would be processed in '{input}':"
            )
            for f in matched_files:
                typer.echo(f"  {f}")
        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            raise typer.Exit(code=1)
        return

    try:
        native_rai_fields = _build_native_rai_fields(
            rai_data_collection=rai_data_collection,
            rai_data_collection_type=rai_data_collection_type,
            rai_data_collection_missing_data=rai_data_collection_missing_data,
            rai_data_collection_raw_data=rai_data_collection_raw_data,
            rai_data_collection_timeframe=rai_data_collection_timeframe,
            rai_data_imputation_protocol=rai_data_imputation_protocol,
            rai_data_preprocessing_protocol=rai_data_preprocessing_protocol,
            rai_data_manipulation_protocol=rai_data_manipulation_protocol,
            rai_data_annotation_protocol=rai_data_annotation_protocol,
            rai_data_annotation_platform=rai_data_annotation_platform,
            rai_data_annotation_analysis=rai_data_annotation_analysis,
            rai_annotations_per_item=rai_annotations_per_item,
            rai_annotator_demographics=rai_annotator_demographics,
            rai_machine_annotation_tools=rai_machine_annotation_tools,
            rai_data_biases=rai_data_biases,
            rai_data_use_cases=rai_data_use_cases,
            rai_data_limitations=rai_data_limitations,
            rai_data_social_impact=rai_data_social_impact,
            rai_personal_sensitive_information=rai_personal_sensitive_information,
            rai_data_release_maintenance_plan=rai_data_release_maintenance_plan,
        )

        if rai_config and native_rai_fields:
            typer.echo(
                "Error: native --rai-* flags cannot be combined with --rai-config. "
                "Use direct flags for native mlcroissant RAI fields, or --rai-config "
                "for the richer YAML-based workflow.",
                err=True,
            )
            raise typer.Exit(code=1)

        # Parse creators following mlcroissant specification
        # Allows flexible Person/Organization objects with optional properties
        parsed_creators = []
        if creator:
            for creator_info in creator:
                creator_info = creator_info.strip()

                # Preferred: semicolon
                if ";" in creator_info:
                    creator_parts = [p.strip() for p in creator_info.split(";")]

                else:
                    # Use CSV parsing for comma cases (handles quotes properly)
                    creator_parts = next(csv.reader([creator_info]))
                    creator_parts = [p.strip() for p in creator_parts]

                if not creator_parts or not creator_parts[0]:
                    continue

                creator_obj = {"name": creator_parts[0]}

                if len(creator_parts) > 1 and creator_parts[1]:
                    creator_obj["email"] = creator_parts[1]

                if len(creator_parts) > 2 and creator_parts[2]:
                    creator_obj["url"] = creator_parts[2]

                parsed_creators.append(creator_obj)

        # Warn early if --count-csv-rows is set but dataset has no CSV files
        if count_csv_rows:
            csv_extensions = {".csv", ".csv.gz", ".csv.bz2", ".csv.xz"}
            all_files = discover_files(
                input, include_patterns=include, exclude_patterns=exclude
            )
            has_csv = any(
                any(str(f).endswith(ext) for ext in csv_extensions) for f in all_files
            )
            if not has_csv:
                typer.echo(
                    "Warning: --count-csv-rows has no effect: no CSV files found in dataset",
                    err=True,
                )

        generator = MetadataGenerator(
            dataset_path=input,
            name=name,
            description=description,
            url=url,
            license=license,
            citation=citation,
            version=dataset_version,
            date_published=date_published,
            creators=parsed_creators if parsed_creators else None,
            count_csv_rows=count_csv_rows,
            includes=include,
            excludes=exclude,
            rai_fields=native_rai_fields,
        )

        # Generate metadata with per-file progress bar
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("{task.fields[current_file]}"),
        ) as progress:
            file_task = progress.add_task(
                "Scanning files...", total=None, current_file=""
            )

            def _progress_callback(current: int, total: int, file_path: str) -> None:
                progress.update(
                    file_task,
                    total=total,
                    completed=current,
                    current_file=file_path,
                )

            metadata_dict = generator.generate_metadata(
                progress_callback=_progress_callback
            )
            progress.update(
                file_task,
                completed=progress.tasks[0].total,
                current_file="",
                description="Scanning files... done",
            )

        # Inject RAI attributes when a config file is provided
        if rai_config:
            from croissant_baker.rai import inject_rai, load_rai_config

            rai = load_rai_config(rai_config)
            metadata_dict = inject_rai(metadata_dict, rai)

        _ensure_rai_conforms_to(
            metadata_dict, force=bool(rai_config or native_rai_fields)
        )

        # Save and optionally validate
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
        ) as progress:
            if validate:
                save_task = progress.add_task("Validating and saving...", total=None)
                _save_dict(metadata_dict, output, validate=True)
                progress.update(save_task, description="Validation completed!")
            else:
                save_task = progress.add_task("Saving metadata...", total=None)
                _save_dict(metadata_dict, output, validate=False)
                progress.update(save_task, description="Save completed!")

        # Show results
        file_count = len(metadata_dict.get("distribution", []))
        record_count = len(metadata_dict.get("recordSet", []))

        typer.echo(
            f"Success! Generated {'validated ' if validate else ''}Croissant metadata"
        )
        typer.echo(f"Files: {file_count}")
        typer.echo(f"Record sets: {record_count}")
        typer.echo(f"Saved to: {output}")

        if not validate:
            typer.echo(
                f"Tip: Run `croissant-baker validate {output}` to validate later"
            )

        _warn_missing_spec_fields(
            creator=creator,
            description=description,
            url=url,
            license=license,
            date_published=date_published,
        )

    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.echo(f"Unexpected error: {e}", err=True)
        raise typer.Exit(code=1)


@app.command(name="rai-apply")
def rai_apply(
    file_path: str = typer.Argument(..., help="Croissant metadata file to update"),
    rai_config: Path = typer.Option(
        ...,
        "--rai-config",
        help="RAI config YAML file",
        exists=True,
        dir_okay=False,
    ),
    output: Optional[str] = typer.Option(
        None,
        "--output",
        "-o",
        help="Output path (defaults to overwriting the input file)",
    ),
    validate: bool = typer.Option(
        True, "--validate/--no-validate", help="Validate after applying RAI attributes"
    ),
) -> None:
    """Apply RAI attributes from a config YAML to an existing Croissant file."""
    from croissant_baker.rai import inject_rai, load_rai_config

    input_path = Path(file_path)
    if not input_path.is_file():
        typer.echo(f"Error: '{file_path}' is not a file", err=True)
        raise typer.Exit(code=1)

    try:
        with open(input_path, encoding="utf-8") as fh:
            metadata_dict = json.load(fh)

        rai = load_rai_config(rai_config)
        metadata_dict = inject_rai(metadata_dict, rai)
        _ensure_rai_conforms_to(metadata_dict, force=True)

        dest = str(Path(output) if output else input_path)
        _save_dict(metadata_dict, dest, validate=validate)

        typer.echo(f"RAI attributes applied and saved to: {dest}")
        if not validate:
            typer.echo(f"Tip: Run `croissant-baker validate {dest}` to validate later")

    except ValueError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.echo(f"Unexpected error: {e}", err=True)
        raise typer.Exit(code=1)


@app.command()
def validate(
    file_path: str = typer.Argument(..., help="Path to Croissant metadata file"),
) -> None:
    """Validate a Croissant metadata file."""
    try:
        typer.echo(f"Validating: {file_path}")
        import mlcroissant as mlc

        dataset = mlc.Dataset(file_path)
        typer.echo("Valid! Croissant file passed validation")
        typer.echo(f"Dataset: {dataset.metadata.name}")
        typer.echo(f"Description: {dataset.metadata.description}")

        if hasattr(dataset.metadata, "distribution"):
            file_count = (
                len(dataset.metadata.distribution)
                if dataset.metadata.distribution
                else 0
            )
            typer.echo(f"Files: {file_count}")

        if hasattr(dataset.metadata, "record_sets"):
            record_count = (
                len(dataset.metadata.record_sets) if dataset.metadata.record_sets else 0
            )
            typer.echo(f"Record sets: {record_count}")

    except ImportError:
        typer.echo("Error: mlcroissant is required for validation", err=True)
        typer.echo("Fix: pip install mlcroissant", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.echo(f"Validation failed: {e}", err=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
