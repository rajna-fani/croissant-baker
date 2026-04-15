# Examples

## MIMIC-IV Demo (bundled test data)

The repo includes a small MIMIC-IV demo dataset under `tests/data/`. This is the easiest way to try the tool immediately after cloning:

```bash
# Minimal
croissant-baker \
  --input tests/data/input/mimiciv_demo/physionet.org/files/mimic-iv-demo/ \
  --creator "Me" \
  --output mimic-iv-demo-croissant.jsonld
```

With full metadata (suitable for publishing):

```bash
croissant-baker \
  --input tests/data/input/mimiciv_demo/physionet.org/files/mimic-iv-demo/ \
  --name "MIMIC-IV Clinical Database Demo" \
  --description "Demo subset of MIMIC-IV containing 100 de-identified patients from Beth Israel Deaconess Medical Center (2008-2019), intended for workshops and rapid prototyping" \
  --url "https://physionet.org/content/mimic-iv-demo/2.2/" \
  --license "https://opendatacommons.org/licenses/odbl/1-0/" \
  --dataset-version "2.2" \
  --date-published "2023-06-22" \
  --citation "Johnson, A., et al. (2023). MIMIC-IV Clinical Database Demo (version 2.2). PhysioNet. https://doi.org/10.13026/dp1f-ex47" \
  --creator "Alistair Johnson,aewj@mit.edu,https://physionet.org/" \
  --creator "Lucas Bulgarelli,,https://mit.edu/" \
  --creator "Tom Pollard,tpollard@mit.edu,https://physionet.org/" \
  --creator "Leo Anthony Celi,lceli@mit.edu,https://lcp.mit.edu/" \
  --creator "Roger Mark,,https://lcp.mit.edu/" \
  --output mimic-iv-demo-croissant.jsonld
```

## MIMIC-IV Full dataset

```bash
croissant-baker \
  --input /path/to/mimic-iv/ \
  --name "MIMIC-IV" \
  --description "MIMIC-IV v3.1: de-identified EHR data from ~300,000 patients at Beth Israel Deaconess Medical Center (2008-2022)" \
  --url "https://physionet.org/content/mimiciv/3.1/" \
  --license "PhysioNet Credentialed Health Data License 1.5.0" \
  --dataset-version "3.1" \
  --date-published "2024-11-12" \
  --citation "Johnson, A., et al. (2024). MIMIC-IV (version 3.1). PhysioNet. https://doi.org/10.13026/kpb9-mt58" \
  --creator "Alistair Johnson,aewj@mit.edu,https://physionet.org/" \
  --creator "Tom Pollard,tpollard@mit.edu,https://physionet.org/" \
  --creator "Leo Anthony Celi,lceli@mit.edu,https://lcp.mit.edu/" \
  --output mimic-iv-croissant.jsonld
```

## MIMIC-IV MEDS Demo

```bash
croissant-baker \
  --input tests/data/input/mimiciv_demo_meds/physionet.org/files/mimic-iv-demo-meds/0.0.1/ \
  --name "MIMIC-IV Demo Data in MEDS Format" \
  --description "MIMIC-IV demo data converted to the Medical Event Data Standard (MEDS) format, providing a standardized longitudinal patient event representation for 100 demo patients" \
  --url "https://physionet.org/content/mimic-iv-ext-meds/" \
  --license "https://opendatacommons.org/licenses/odbl/1-0/" \
  --dataset-version "0.0.1" \
  --date-published "2024-06-18" \
  --creator "Alistair Johnson,aewj@mit.edu,https://physionet.org/" \
  --creator "Tom Pollard,tpollard@mit.edu,https://physionet.org/" \
  --output mimic-iv-demo-meds-croissant.jsonld
```

## Multiple creators

Creator format: `Name[,Email[,URL]]`. Use `--creator` once per person.

```bash
croissant-baker \
  --input ./dataset \
  --creator "Alice Lim,alice@example.com,https://alice.example.com" \
  --creator "Bob Chen,bob@example.com" \
  --creator "Carol Wu"
```

## Custom output path

```bash
croissant-baker \
  --input ./dataset \
  --creator "Jane Smith" \
  --output metadata/my-dataset-croissant.jsonld
```

Parent directories are created automatically.

## Include / exclude file patterns

Process only CSV files:

```bash
croissant-baker --input ./dataset --creator "Jane Smith" --include "*.csv"
```

Exclude temporary files:

```bash
croissant-baker --input ./dataset --creator "Jane Smith" \
  --exclude "*.tmp" --exclude "*.bak"
```

## Dry run — preview without writing

```bash
croissant-baker --input ./dataset --dry-run
# Dry run: 4 file(s) would be processed in './dataset':
#   patients.csv
#   vitals.parquet
#   images/scan_001.png
#   images/scan_002.png
```

## Skip validation

Useful during development when the dataset is incomplete:

```bash
croissant-baker --input ./dataset --creator "Jane Smith" --no-validate
# Tip: Run `croissant-baker validate output.jsonld` to validate later
```

## Validate an existing file

```bash
croissant-baker validate dataset-croissant.jsonld
# Validating: dataset-croissant.jsonld
# Valid! Croissant file passed validation
# Dataset: my-dataset
# Files: 3
# Record sets: 2
```

## Programmatic usage (Python)

```python
from croissant_baker.metadata_generator import MetadataGenerator

gen = MetadataGenerator(
    dataset_path="./my-dataset",
    creators=[{"name": "Jane Smith", "email": "jane@example.com"}],
    description="My dataset description",
    license="CC-BY-4.0",
)

# Generate and save (validates by default)
gen.save_metadata("my-dataset-croissant.jsonld")

# Or inspect the dict first
metadata = gen.generate_metadata()
print(metadata["name"])
```
