"""Croissant metadata generator for datasets."""

import json
import tempfile
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict
import mlcroissant as mlc

from croissant_baker.files import discover_files
from croissant_baker.handlers.registry import find_handler, register_all_handlers

# Register all handlers
register_all_handlers()


def serialize_datetime(obj):
    """Convert datetime objects to ISO format strings for JSON serialization."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class MetadataGenerator:
    """
    Generates Croissant metadata for datasets with automatic type inference.

    Discovers files, delegates format-specific logic to registered handlers
    via the build_croissant protocol, and assembles the final JSON-LD.
    """

    def __init__(
        self,
        dataset_path: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        url: Optional[str] = None,
        license: Optional[str] = None,
        citation: Optional[str] = None,
        version: Optional[str] = None,
        date_published: Optional[str] = None,
        creators: Optional[List[Dict[str, str]]] = None,
        count_csv_rows: bool = False,
        includes: Optional[List[str]] = None,
        excludes: Optional[List[str]] = None,
    ):
        """
        Initialize the metadata generator for a dataset.

        Args:
            dataset_path: Path to the directory containing dataset files.
            name: Dataset name (defaults to directory name).
            description: Dataset description.
            url: Dataset URL.
            license: License URL or SPDX identifier (e.g. "CC-BY-4.0").
            citation: Citation text, preferably BibTeX format.
            version: Dataset version string.
            date_published: Publication date in ISO format ("2023-12-15" or
                "2023-12-15T10:30:00").
            creators: List of dicts with "name", "email", and/or "url" keys.
            count_csv_rows: If True, scan each CSV fully for exact row counts.
                Defaults to False for performance.
            includes: Glob patterns to include. Applied before excludes.
            excludes: Glob patterns to exclude. Applied after includes.

        Raises:
            ValueError: If dataset_path is not a directory.
        """
        self.dataset_path = Path(dataset_path).resolve()
        if not self.dataset_path.is_dir():
            raise ValueError(f"Dataset path {dataset_path} is not a directory")

        self.name = name
        self.description = description
        self.url = url
        self.license = license
        self.citation = citation
        self.version = version
        self.date_published = date_published
        self.creators = creators
        self.includes = includes
        self.excludes = excludes
        # Generic options forwarded to every handler via **kwargs.
        # Handlers declare what they use; others ignore the rest.
        # To add a new handler-specific flag: add one key here — the call site never changes.
        self._handler_kwargs = {
            "count_rows": count_csv_rows,
        }

    def generate_metadata(self) -> dict:
        """Generate complete Croissant metadata for the dataset."""
        files = discover_files(
            str(self.dataset_path),
            include_patterns=self.includes,
            exclude_patterns=self.excludes,
        )

        # Extract metadata as (handler, meta) pairs so handler identity is
        # stored by reference, not by id() — no fragility if dicts are copied.
        file_metadata: list[tuple] = []
        for file_path in files:
            full_path = self.dataset_path / file_path
            handler = find_handler(full_path)
            if handler:
                try:
                    meta = handler.extract_metadata(full_path, **self._handler_kwargs)
                    meta["relative_path"] = str(file_path)
                    file_metadata.append((handler, meta))
                except Exception as e:
                    print(f"Warning: Failed to process {file_path}: {e}")

        if not file_metadata:
            raise ValueError("No supported files found in the dataset")

        metadata = mlc.Metadata(
            name=self.name or self.dataset_path.name,
            description=self._build_description(file_metadata),
            url=self.url or f"file://{self.dataset_path}",
            license=self._resolve_license(),
            creators=self._build_creators(),
            date_published=self._resolve_date(),
            version=self.version or "1.0.0",
            cite_as=self._build_citation(),
        )

        # distributions holds both FileObjects and FileSets — the full contents
        # of the Croissant `distribution` array per the spec.
        distributions = []
        record_sets = []
        # Use a counter (not enumerate) for unique FileObject IDs: some formats
        # (e.g. WFDB) create multiple FileObjects per meta via related_files,
        # so enumerate would produce ID collisions.
        file_counter = 0
        _batch_handlers: dict = defaultdict(list)

        for handler, file_meta in file_metadata:
            file_id = f"file_{file_counter}"
            file_counter += 1

            distributions.append(
                mlc.FileObject(
                    id=file_id,
                    name=file_meta["file_name"],
                    content_url=file_meta["relative_path"],
                    encoding_formats=[file_meta["encoding_format"]],
                    content_size=str(file_meta["file_size"]),
                    sha256=file_meta["sha256"],
                )
            )

            # Multi-file records (e.g. WFDB: .hea + .dat + .atr): the generator
            # owns FileObject creation for every physical file. RecordSet
            # construction is delegated to the handler via build_croissant.
            if "related_files" in file_meta:
                for related in file_meta["related_files"]:
                    related_id = f"file_{file_counter}"
                    file_counter += 1
                    rel_path = Path(related["path"])
                    distributions.append(
                        mlc.FileObject(
                            id=related_id,
                            name=related["name"],
                            content_url=str(rel_path.relative_to(self.dataset_path)),
                            encoding_formats=[related["encoding"]],
                            content_size=str(related["size"]),
                            sha256=related["sha256"],
                        )
                    )

            _batch_handlers[handler].append((file_id, file_meta))

        # Each handler builds its FileSets + RecordSets and returns them.
        # Handlers never return FileObjects — those are owned by the generator.
        # TODO: future improvements per handler:
        #   - references: detect foreign-key columns (e.g. subject_id) and emit
        #     cr:references links between RecordSets — high-impact for EHR data.
        #   - enumerations: for low-cardinality categorical columns, emit
        #     sc:Enumeration RecordSets.
        for _h, pairs in _batch_handlers.items():
            try:
                filesets, rs = _h.build_croissant(
                    [m for _, m in pairs],
                    [fid for fid, _ in pairs],
                )
                distributions.extend(filesets)
                record_sets.extend(rs)
            except Exception as e:
                print(f"Warning: {type(_h).__name__}.build_croissant failed: {e}")

        metadata.distribution = distributions
        metadata.record_sets = record_sets

        return metadata.to_json()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_description(self, file_metadata: list) -> str:
        if self.description:
            return self.description
        file_types = {m.get("encoding_format", "unknown") for _, m in file_metadata}
        return (
            f"Dataset containing {len(file_metadata)} files "
            f"({', '.join(sorted(file_types))}) with automatically inferred types and structure"
        )

    def _resolve_license(self) -> str:
        if not self.license:
            return "https://creativecommons.org/licenses/by/4.0/"
        if self.license.startswith(("http://", "https://")):
            return self.license
        spdx_to_url = {
            "CC-BY-4.0": "https://creativecommons.org/licenses/by/4.0/",
            "CC-BY-SA-4.0": "https://creativecommons.org/licenses/by-sa/4.0/",
            "CC-BY-NC-4.0": "https://creativecommons.org/licenses/by-nc/4.0/",
            "CC-BY-ND-4.0": "https://creativecommons.org/licenses/by-nd/4.0/",
            "CC0-1.0": "https://creativecommons.org/publicdomain/zero/1.0/",
            "MIT": "https://opensource.org/licenses/MIT",
            "Apache-2.0": "https://www.apache.org/licenses/LICENSE-2.0",
            "GPL-3.0": "https://www.gnu.org/licenses/gpl-3.0.html",
            "BSD-3-Clause": "https://opensource.org/licenses/BSD-3-Clause",
        }
        return spdx_to_url.get(self.license, self.license)

    def _build_creators(self) -> list:
        if not self.creators:
            return [mlc.Person(name="Dataset Creator", email="creator@example.com")]
        return [
            mlc.Person(**{k: v for k, v in c.items() if k in ("name", "email", "url")})
            for c in self.creators
        ]

    def _build_citation(self) -> str:
        if self.citation:
            return self.citation
        year = datetime.now().year
        name = self.name or self.dataset_path.name
        return f"Dataset Creator. ({year}). {name} Dataset. Generated with automated type inference."

    def _resolve_date(self) -> datetime:
        if not self.date_published:
            return datetime.now()
        try:
            return datetime.fromisoformat(self.date_published)
        except ValueError as e:
            raise ValueError(
                f"Invalid date format for --date-published: '{self.date_published}'. "
                f"Expected ISO format like '2023-12-15' or '2023-12-15T10:30:00'. Error: {e}"
            )

    def save_metadata(self, output_path: str, validate: bool = True) -> None:
        """Generate and save Croissant metadata to a file.

        Args:
            output_path: Path where the JSON-LD metadata file will be written.
            validate: If True (default), validates with mlcroissant before saving.

        Raises:
            ValueError: If validation fails or the file cannot be saved.
        """
        metadata_dict = self.generate_metadata()
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        if validate:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".jsonld", delete=False
            ) as tmp_file:
                json.dump(
                    metadata_dict,
                    tmp_file,
                    indent=2,
                    ensure_ascii=False,
                    default=serialize_datetime,
                )
                tmp_path = tmp_file.name
            try:
                mlc.Dataset(tmp_path)
                self._save_to_file(metadata_dict, output_file)
            except Exception as e:
                raise ValueError(f"Validation failed: {e}")
            finally:
                Path(tmp_path).unlink(missing_ok=True)
        else:
            self._save_to_file(metadata_dict, output_file)

    def _save_to_file(self, metadata_dict: dict, output_file: Path) -> None:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(
                metadata_dict,
                f,
                indent=2,
                ensure_ascii=False,
                default=serialize_datetime,
            )
            f.write("\n")
