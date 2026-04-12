# Evaluation Datasets

This directory contains full-scale datasets used to evaluate Croissant Baker
output quality against independently authored Croissant metadata.

Evaluations are separate from the unit and integration tests in `tests/`. A
lightweight subset of each evaluation dataset is committed under
`tests/data/input/` so that `uv run pytest` covers the core code paths without
requiring any downloads. The full evaluations here go further: they compare
generated metadata against human-authored ground truth and report quantitative
metrics (RecordSet coverage, field matching, type agreement).

Each subdirectory is self-contained: download script, ground truth, evaluation
script, and generated output artifacts.

| Dataset | Description |
|---------|-------------|
| `open_targets/` | [Open Targets Platform](https://platform.opentargets.org) — 55 Parquet datasets with a published `croissant.json` ground truth |

## Running an evaluation

```bash
# 1. Download the data (one-time)
bash eval/open_targets/download.sh

# 2. Run the evaluation
uv run pytest eval/open_targets/ -v
```

Evaluations are not collected by the default `uv run pytest` run. They must be
invoked explicitly as shown above.
