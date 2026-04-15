<p align="center">
  <img src="assets/baker_logo.png" alt="croissant-baker" width="180">
</p>

# croissant-baker

**croissant-baker** automatically generates [Croissant](https://mlcommons.org/working-groups/data/croissant/) JSON-LD metadata for ML datasets. Point it at a dataset directory and it produces a standards-compliant `.jsonld` file — ready for submission to repositories like [PhysioNet](https://physionet.org/), [NeurIPS Datasets & Benchmarks](https://neurips.cc/Conferences/2026/EvaluationsDatasetsHosting), or any platform that benefits from standardized dataset metadata.

```bash
pip install croissant-baker

croissant-baker \
  --input ./my-dataset \
  --creator "Jane Smith,jane@example.com" \
  --description "My dataset" \
  --license "CC-BY-4.0"
```

See the [Getting Started](getting-started.md) guide to run your first example.

## Features

- **Automatic type inference** — reads CSV/TSV, Parquet, FHIR NDJSON, JSON/JSONL, WFDB, and images; maps column and field types to the Croissant schema
- **Metadata overrides** — sensible defaults for every field; use flags to set name, description, license, creators, citation, and more
- **RAI metadata** — document responsible AI fields (data collection, biases, limitations, sensitive information) via CLI flags or a YAML config
- **Validation built-in** — validates against the Croissant spec via `mlcroissant` before writing; use `--no-validate` to skip
- **Dry-run mode** — preview which files would be processed with `--dry-run` before committing
- **Include / exclude filters** — glob patterns to include or exclude files by name
