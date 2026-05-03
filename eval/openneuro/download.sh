#!/bin/bash
# Download a configurable subset of the smallest publicly available
# OpenNeuro datasets [https://openneuro.org] for NIfTI + BIDS sidecar
# evaluation of Croissant Baker.
#
# Source: s3://openneuro.org/ (public, no credentials required)
# Format: BIDS-organized neuroimaging datasets, each containing
#         .nii / .nii.gz volumes, paired BIDS .json sidecars, and
#         (typically) participants.tsv / events.tsv tabular files.
# License: per-dataset (mostly CC0 / PDDL); see each dataset_description.json.
#
# The OpenNeuro S3 bucket name contains a dot ("openneuro.org") so virtual-
# hosted-style HTTPS URLs would fail TLS hostname verification. All requests
# below use path-style URLs (https://s3.amazonaws.com/openneuro.org/...).
#
# Per-dataset sizes come from S3 ListObjectsV2 (Contents/Size) rather than
# the OpenNeuro GraphQL API, whose totals reflect git-annex pointer files
# instead of real object bytes.
#
# Usage:
#   bash eval/openneuro/download.sh                  # default: 50 datasets, <=2 GB each
#   LIMIT=10 bash eval/openneuro/download.sh         # quick pass for development
#
# Tunable via env:
#   LIMIT          number of datasets to download                (default 50)
#   MAX_SIZE_MB    skip datasets larger than this                (default 2000)
#   MIN_SIZE_MB    skip datasets smaller than this (likely empty) (default 1)
#
# The downloaded dataset directories and the size index are git-ignored;
# only this script and the test harness are committed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LIMIT="${LIMIT:-50}"
MAX_SIZE_MB="${MAX_SIZE_MB:-2000}"
MIN_SIZE_MB="${MIN_SIZE_MB:-1}"
INDEX_FILE="$SCRIPT_DIR/.dataset_sizes.json"

cd "$SCRIPT_DIR"

if [ ! -f "$INDEX_FILE" ]; then
  echo "Building OpenNeuro size index (one-time, ~5-10 min for ~1000 datasets)..."
  python3 - "$INDEX_FILE" <<'PY'
import json, sys, urllib.request, xml.etree.ElementTree as ET
from urllib.parse import quote

BASE = "https://s3.amazonaws.com/openneuro.org"
NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
OUT = sys.argv[1]


def list_v2(prefix="", delimiter=None, token=None, max_keys=1000):
    qs = ["list-type=2", f"max-keys={max_keys}"]
    if prefix:
        qs.append(f"prefix={prefix}")
    if delimiter:
        qs.append(f"delimiter={delimiter}")
    if token:
        qs.append(f"continuation-token={quote(token, safe='')}")
    url = f"{BASE}/?" + "&".join(qs)
    with urllib.request.urlopen(url, timeout=60) as r:
        return ET.fromstring(r.read())


def all_dataset_ids():
    ids = []
    token = None
    while True:
        root = list_v2(delimiter="/", token=token, max_keys=1000)
        for cp in root.findall("s3:CommonPrefixes", NS):
            ids.append(cp.find("s3:Prefix", NS).text.rstrip("/"))
        if root.find("s3:IsTruncated", NS).text != "true":
            break
        nxt = root.find("s3:NextContinuationToken", NS)
        if nxt is None or nxt.text is None:
            break
        token = nxt.text
    return ids


def dataset_size(ds, max_pages=20):
    total = n = 0
    pages = 0
    token = None
    while pages < max_pages:
        root = list_v2(prefix=f"{ds}/", token=token, max_keys=1000)
        for c in root.findall("s3:Contents", NS):
            total += int(c.find("s3:Size", NS).text)
            n += 1
        if root.find("s3:IsTruncated", NS).text != "true":
            break
        nxt = root.find("s3:NextContinuationToken", NS)
        if nxt is None or nxt.text is None:
            break
        token = nxt.text
        pages += 1
    return total, n, (pages == max_pages)


print("Listing all dataset IDs...", flush=True)
ids = all_dataset_ids()
print(f"  {len(ids)} datasets to size", flush=True)

sized = []
for i, ds in enumerate(ids, 1):
    try:
        sz, n, capped = dataset_size(ds)
    except Exception as e:
        print(f"  [{i}/{len(ids)}] {ds}: ERROR {e}", flush=True)
        continue
    sized.append({"id": ds, "size_bytes": sz, "n_objects": n, "capped": capped})
    if i % 25 == 0 or i == len(ids):
        print(f"  [{i}/{len(ids)}] last: {ds} = {sz / 1e6:.1f} MB", flush=True)

with open(OUT, "w") as f:
    json.dump(sized, f)
    f.write("\n")
print(f"Wrote {OUT} ({len(sized)} entries)")
PY
else
  echo "  [skip] dataset size index already present at $INDEX_FILE"
fi

echo ""
echo "Picking $LIMIT smallest publicly accessible datasets (size between $MIN_SIZE_MB and $MAX_SIZE_MB MB)..."

# Some datasets in the bucket index require authenticated access for object
# retrieval. We HEAD-probe each candidate in size order and keep only those
# whose first listed file is reachable anonymously. The probe cap (LIMIT * 6)
# bounds wasted work in pathological cases.
PICKS=$(python3 - "$INDEX_FILE" "$LIMIT" "$MIN_SIZE_MB" "$MAX_SIZE_MB" <<'PY'
import json, sys, urllib.request, xml.etree.ElementTree as ET
from urllib.parse import quote
idx_path, limit, min_mb, max_mb = sys.argv[1], int(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])
NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
BASE = "https://s3.amazonaws.com/openneuro.org"
with open(idx_path) as f:
    entries = json.load(f)
ok = [
    e for e in entries
    if not e["capped"]
    and min_mb * 1e6 <= e["size_bytes"] <= max_mb * 1e6
]
ok.sort(key=lambda e: e["size_bytes"])

picked = 0
for e in ok[: limit * 6]:
    ds = e["id"]
    try:
        with urllib.request.urlopen(
            f"{BASE}/?list-type=2&max-keys=1&prefix={ds}/", timeout=15
        ) as r:
            root = ET.fromstring(r.read())
    except Exception:
        continue
    c = root.find("s3:Contents", NS)
    if c is None:
        continue
    key = c.find("s3:Key", NS).text
    req = urllib.request.Request(f"{BASE}/{quote(key)}", method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status != 200:
                continue
    except Exception:
        continue
    print(f"{e['id']}\t{e['size_bytes']}")
    picked += 1
    if picked >= limit:
        break
PY
)

if [ -z "$PICKS" ]; then
  echo "  [error] no publicly accessible datasets in size band; relax MIN_SIZE_MB / MAX_SIZE_MB"
  exit 1
fi

echo ""
echo "Downloading datasets:"
while IFS=$'\t' read -r ds size_bytes; do
  size_mb=$(awk "BEGIN{printf \"%.1f\", $size_bytes/1e6}")
  dest="$SCRIPT_DIR/$ds"
  if [ -d "$dest" ] && [ -n "$(ls -A "$dest" 2>/dev/null)" ]; then
    echo "  [skip] $ds (${size_mb} MB) already present"
    continue
  fi
  echo "  Downloading $ds (${size_mb} MB)..."
  python3 - "$ds" "$dest" <<'PY'
import os, sys, urllib.request, xml.etree.ElementTree as ET
from urllib.parse import quote
ds, dest = sys.argv[1], sys.argv[2]
BASE = "https://s3.amazonaws.com/openneuro.org"
NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

token = None
while True:
    qs = ["list-type=2", "max-keys=1000", f"prefix={ds}/"]
    if token:
        qs.append(f"continuation-token={quote(token, safe='')}")
    with urllib.request.urlopen(f"{BASE}/?" + "&".join(qs), timeout=60) as r:
        root = ET.fromstring(r.read())
    for c in root.findall("s3:Contents", NS):
        key = c.find("s3:Key", NS).text
        rel = key[len(ds) + 1:]  # strip "ds00XXXX/"
        if not rel:
            continue
        out = os.path.join(dest, rel)
        os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
        if os.path.exists(out):
            continue
        url = f"{BASE}/{quote(key)}"
        with urllib.request.urlopen(url, timeout=120) as src, open(out, "wb") as dst:
            while True:
                chunk = src.read(1 << 20)
                if not chunk:
                    break
                dst.write(chunk)
    if root.find("s3:IsTruncated", NS).text != "true":
        break
    nxt = root.find("s3:NextContinuationToken", NS)
    if nxt is None or nxt.text is None:
        break
    token = nxt.text
PY
done <<< "$PICKS"

echo ""
echo "Datasets downloaded to $SCRIPT_DIR:"
for d in "$SCRIPT_DIR"/ds*/; do
  [ -d "$d" ] || continue
  printf "  %-12s  %s\n" "$(basename "$d")" "$(du -sh "$d" | cut -f1)"
done

echo ""
echo "Done. Run the evaluation with:"
echo "  uv run pytest eval/openneuro/ -v"
