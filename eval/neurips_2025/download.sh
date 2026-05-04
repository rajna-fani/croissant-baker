#!/bin/bash
# Fetch the dataset payloads for the NeurIPS 2025 Datasets & Benchmarks
# cross-domain evaluation of Croissant Baker.
#
# Picks are a pre-registered seeded draw across the 11 OpenReview
# primary-area buckets covering all major scientific and applied-ML domains
# represented in the track. Resolved repository handles are listed in
# picks.json. Producer-authored Croissants for each pick are bundled in
# producer_croissants/ at the paper-submission-time snapshot used as
# ground truth.
#
# Usage:
#   huggingface-cli login
#   bash eval/neurips_2025/download.sh
#
# The downloaded dataset directories are git-ignored; only this script,
# picks.json, producer_croissants/, and the test harness are committed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PICKS="$SCRIPT_DIR/picks.json"
DEST="$SCRIPT_DIR/datasets"

if ! command -v huggingface-cli >/dev/null 2>&1; then
  echo "ERROR: huggingface-cli is not on PATH. Install with: pip install -U huggingface_hub" >&2
  exit 1
fi

if [ ! -f "$PICKS" ]; then
  echo "ERROR: picks.json not found at $PICKS" >&2
  exit 1
fi

mkdir -p "$DEST"

# Sort by listed order in picks.json.unique_datasets for deterministic logs.
REPOS=$(python3 -c "
import json
m = json.load(open('$PICKS'))
print('\n'.join(u['repo'] for u in m['unique_datasets']))
")

ok=0
skipped=0
fail=0
for repo in $REPOS; do
  target="$DEST/${repo//\//__}"
  if [ -d "$target" ] && [ -n "$(ls -A "$target" 2>/dev/null)" ]; then
    echo "SKIP $repo (already present)"
    skipped=$((skipped + 1))
    continue
  fi
  echo "[$(date '+%H:%M:%S')] $repo"
  if huggingface-cli download "$repo" --repo-type dataset \
       --local-dir "$target" --max-workers 4 >/dev/null 2>&1; then
    ok=$((ok + 1))
  else
    echo "  FAIL $repo (see huggingface-cli output above)"
    fail=$((fail + 1))
  fi
done

echo ""
echo "Result: $ok newly downloaded, $skipped already present, $fail failed."
echo "Total disk under $DEST:"
du -sh "$DEST"
