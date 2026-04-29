#!/bin/bash
# Download FHIR NDJSON bulk export sample data from the SMART Health IT project
# for out-of-distribution evaluation of the Croissant Baker FHIR handler.
#
# Source: https://github.com/smart-on-fhir/sample-bulk-fhir-datasets (10-patients branch)
# Data: 10 synthetic patients, ~19 MB unzipped, CC0 license
# Format: NDJSON bulk (one resource type per .ndjson file) — same wire format as
#   MIMIC-IV FHIR, but with standard FHIR naming (Patient, Condition, Observation vs
#   MimicPatient, MimicObservationLabevents) from an independent organization.
#
# Usage:
#   bash eval/fhir_smart_bulk/download.sh
#
# The downloaded NDJSON files are git-ignored; only this script is committed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NDJSON_DIR="$SCRIPT_DIR/ndjson"
STANDARDS_DIR="$SCRIPT_DIR/standards"
US_CORE_DIR="$STANDARDS_DIR/us_core"
FHIR_CORE_DIR="$STANDARDS_DIR/fhir_r4_core"

mkdir -p "$NDJSON_DIR"
if [ -d "$NDJSON_DIR" ] && [ "$(find "$NDJSON_DIR" -name '*.ndjson' | wc -l | tr -d ' ')" -gt 0 ]; then
  echo "SMART bulk FHIR data already present ($(find "$NDJSON_DIR" -name '*.ndjson' | wc -l | tr -d ' ') files). Skipping data download."
else
  # The 10-patient dataset is stored in its own branch and available as a zip archive.
  ZIP_URL="https://github.com/smart-on-fhir/sample-bulk-fhir-datasets/archive/refs/heads/10-patients.zip"
  ZIP_PATH="$SCRIPT_DIR/smart_bulk_10.zip"

  echo "Downloading SMART bulk FHIR 10-patient sample (~1.9 MB zipped)..."
  curl -fL --retry 3 --retry-delay 2 -o "$ZIP_PATH" "$ZIP_URL"
  echo "Download complete. Extracting..."
  unzip -q "$ZIP_PATH" -d "$SCRIPT_DIR/smart_extract"

  # The zip extracts to a directory named sample-bulk-fhir-datasets-10-patients/
  EXTRACT_DIR=$(find "$SCRIPT_DIR/smart_extract" -maxdepth 1 -type d | grep -v "^$SCRIPT_DIR/smart_extract$" | head -1)
  find "$EXTRACT_DIR" -name '*.ndjson' -exec cp {} "$NDJSON_DIR/" \;

  rm -rf "$SCRIPT_DIR/smart_extract" "$ZIP_PATH"
fi

total=$(find "$NDJSON_DIR" -name '*.ndjson' | wc -l | tr -d ' ')
echo "NDJSON files available: $total"
du -sh "$NDJSON_DIR"

if [ -d "$US_CORE_DIR/package" ] && [ -d "$FHIR_CORE_DIR/package" ]; then
  echo "FHIR standards packages already present. Skipping standards download."
else
  mkdir -p "$US_CORE_DIR" "$FHIR_CORE_DIR"

  echo "Downloading official HL7 standards packages for standards-grounded evaluation..."
  curl -fL --retry 3 --retry-delay 2 -o "$US_CORE_DIR/package.tgz" \
    "https://www.hl7.org/fhir/us/core/STU7/package.tgz"
  curl -fL --retry 3 --retry-delay 2 -o "$FHIR_CORE_DIR/package.tgz" \
    "https://packages2.fhir.org/packages/hl7.fhir.r4.core/4.0.1"

  echo "Extracting standards packages..."
  tar -xzf "$US_CORE_DIR/package.tgz" -C "$US_CORE_DIR"
  tar -xzf "$FHIR_CORE_DIR/package.tgz" -C "$FHIR_CORE_DIR"
  rm -f "$US_CORE_DIR/package.tgz" "$FHIR_CORE_DIR/package.tgz"
fi

echo ""
echo "Done. Run the evaluation with:"
echo "  uv run pytest eval/fhir_smart_bulk/ -v"
