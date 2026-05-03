#!/bin/bash
# Download a representative subset of the dcm_validate corpus
# (Rorden et al., Sci Data 2025, https://doi.org/10.5281/zenodo.15310934)
# for paired DICOM + NIfTI evaluation of Croissant Baker.
#
# Source repos are the per-module GitHub mirrors maintained by Chris Rorden
# at https://github.com/neurolabusc/dcm_qa_*. Each module ships:
#   - In/  : original DICOM files (one or more series)
#   - Ref/ : validated NIfTI volumes plus paired BIDS JSON sidecars
#
# Module picks (vendor / format coverage):
#   dcm_qa_ge       GE MR (~150 MB In/, 4125 .dcm)
#   dcm_qa_nih      NIH multi-vendor (GE + Siemens) (~31 MB)
#   dcm_qa_polar    GE epi_pepolar phase-encoding variant (~124 MB, 960 .dcm)
#   dcm_qa_stc      Slice timing correction reference (~269 MB, 589 .dcm)
#   dcm_qa_uih      United Imaging Healthcare (UIH) MR (~477 MB, 394 .dcm)
#   dcm_qa_enh      Enhanced DICOM cross-vendor (~65 MB, 8 multi-frame .dcm)
#
# Usage:
#   bash eval/dcm_validate/download.sh
#
# The cloned module directories are git-ignored; only this script and
# the test harness are committed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

MODULES=(
  dcm_qa_ge
  dcm_qa_nih
  dcm_qa_polar
  dcm_qa_stc
  dcm_qa_uih
  dcm_qa_enh
)

cd "$SCRIPT_DIR"

for mod in "${MODULES[@]}"; do
  if [ -d "$mod" ] && [ -d "$mod/In" ] && [ -d "$mod/Ref" ]; then
    echo "  [skip] $mod (already present)"
    continue
  fi
  echo "  Cloning $mod..."
  git clone --depth 1 --quiet "https://github.com/neurolabusc/$mod.git"
done

echo ""
echo "Module footprints:"
for mod in "${MODULES[@]}"; do
  if [ -d "$mod" ]; then
    in_files=$(find "$mod/In" -type f 2>/dev/null | wc -l | tr -d ' ')
    ref_files=$(find "$mod/Ref" -type f 2>/dev/null | wc -l | tr -d ' ')
    sz=$(du -sh "$mod" | cut -f1)
    printf "  %-22s  %s  (%d files In/, %d files Ref/)\n" "$mod" "$sz" "$in_files" "$ref_files"
  fi
done

echo ""
echo "Done. Run the evaluation with:"
echo "  uv run pytest eval/dcm_validate/ -v"
