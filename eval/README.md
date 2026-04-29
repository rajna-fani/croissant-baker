# Evaluation Datasets

This directory contains full-scale datasets used to evaluate Croissant Baker
output quality against external metadata references.

Evaluations are separate from the unit and integration tests in `tests/`. A
lightweight subset of each evaluation dataset is committed under
`tests/data/input/` so that `uv run pytest` covers the core code paths without
requiring any downloads. The full evaluations here go further: they compare
generated metadata against either human-authored or standards-grounded
references and report quantitative metrics (RecordSet coverage, field matching,
type agreement).

Each subdirectory is self-contained: download script, reference source,
evaluation script, and generated output artifacts.

| Dataset | Reference Type | Description |
|---------|----------------|-------------|
| `open_targets/` | Human-authored Croissant | [Open Targets Platform](https://platform.opentargets.org) — 55 Parquet datasets with a published `croissant.json` ground truth |
| `fhir_smart_bulk/` | Standards-grounded (US Core + FHIR R4) | [SMART Health IT 10-patient bulk sample](https://github.com/smart-on-fhir/sample-bulk-fhir-datasets) — FHIR NDJSON bulk export with official profile/spec comparison |
| `fhir_bundle/` | Structural fidelity | [SMART Health IT custom sample data](https://github.com/smart-on-fhir/custom-sample-data) — 5 FHIR JSON Bundle files exercising the Bundle handler code path |

## Running an evaluation

```bash
# Open Targets
bash eval/open_targets/download.sh
uv run pytest eval/open_targets/ -v

# SMART FHIR bulk NDJSON
bash eval/fhir_smart_bulk/download.sh
uv run pytest eval/fhir_smart_bulk/ -v

# FHIR JSON Bundle
bash eval/fhir_bundle/download.sh
uv run pytest eval/fhir_bundle/ -v
```

Evaluations are not collected by the default `uv run pytest` run. They must be
invoked explicitly as shown above.
