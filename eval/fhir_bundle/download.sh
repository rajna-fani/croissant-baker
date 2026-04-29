#!/bin/bash
# Download FHIR transaction Bundle sample data from the SMART Health IT
# custom-sample-data repository for out-of-distribution evaluation of the
# Croissant Baker FHIR handler.
#
# Source: https://github.com/smart-on-fhir/custom-sample-data (fhir-resources/)
# Data: 5 FHIR transaction Bundle JSON files (~150 KB total), CC0 license
# Format: JSON Bundle (resourceType = "Bundle", type = "transaction") —
#   exercises the Bundle path of the FHIR handler (fhir_handler.py lines 245–321)
#   against non-MIMIC FHIR data with standard FHIR resource naming.
#
# Usage:
#   bash eval/fhir_bundle/download.sh
#
# The downloaded JSON files are git-ignored; only this script is committed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FHIR_DIR="$SCRIPT_DIR/fhir"

if [ -d "$FHIR_DIR" ] && [ "$(find "$FHIR_DIR" -name '*.json' | wc -l | tr -d ' ')" -ge 5 ]; then
  echo "FHIR Bundle data already present ($(find "$FHIR_DIR" -name '*.json' | wc -l | tr -d ' ') files). Skipping."
  exit 0
fi

mkdir -p "$FHIR_DIR"

RAW_BASE="https://raw.githubusercontent.com/smart-on-fhir/custom-sample-data/master/fhir-resources"
FILES=(
  "hca-pat-64.json"
  "hca-pat-67.json"
  "hca-pat-77.json"
  "diabetes.json"
  "nest_patient_data.json"
)

echo "Downloading FHIR Bundle sample files from smart-on-fhir/custom-sample-data..."
for fname in "${FILES[@]}"; do
  dest="$FHIR_DIR/$fname"
  if [ -f "$dest" ]; then
    echo "  [skip] $fname (already present)"
    continue
  fi
  curl -fsSL --retry 3 -o "$dest" "$RAW_BASE/$fname"
  echo "  Downloaded: $fname ($(du -sh "$dest" | cut -f1))"
done

total=$(find "$FHIR_DIR" -name '*.json' | wc -l | tr -d ' ')
echo ""
echo "Downloaded $total FHIR Bundle JSON files to $FHIR_DIR"
du -sh "$FHIR_DIR"
echo ""
echo "Done. Run the evaluation with:"
echo "  uv run pytest eval/fhir_bundle/ -v"
