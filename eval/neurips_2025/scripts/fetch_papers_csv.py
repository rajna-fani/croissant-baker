"""Re-extract scripts/papers.csv from OpenReview.

This is the provenance script for the frozen papers.csv shipped alongside
this evaluation. It paginates the OpenReview API for the NeurIPS 2025
Datasets & Benchmarks track, walks the accepted-paper notes, and projects
the columns retained in the snapshot: paper_id, primary_area, host, repo,
croissant_cached, size_bytes.

Usage:

    python eval/neurips_2025/scripts/fetch_papers_csv.py [--out PATH]

OpenReview state is mutable: papers may be edited or withdrawn after
acceptance, and size_bytes reflects the dataset host's reported size at
the moment of fetch. Re-running this script may therefore produce a CSV
that differs from the pinned snapshot. The pinned snapshot in this
directory is the canonical record of the candidate pool from which
picks.json was drawn.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

VENUE_ID = "NeurIPS.cc/2025/Datasets_and_Benchmarks_Track"
API = "https://api2.openreview.net"
HF_REPO_RX = re.compile(
    r"https?://(?:huggingface\.co|hf\.co)/datasets/([^/?#]+/[^/?#]+)"
)


def _api_get(path: str) -> dict:
    url = f"{API}{path}"
    req = urllib.request.Request(url, headers={"User-Agent": "neurips-eval/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def _classify_host(url: str) -> str:
    u = (url or "").lower()
    if "huggingface.co" in u or "hf.co" in u:
        return "huggingface"
    if "kaggle.com" in u:
        return "kaggle"
    if "github.com" in u:
        return "github"
    if "dataverse" in u:
        return "dataverse"
    if "zenodo.org" in u or "doi.org/10.5281" in u:
        return "zenodo"
    if "doi.org" in u:
        return "doi"
    if "physionet.org" in u:
        return "physionet"
    return "other" if u else "none"


def _hf_repo(url: str) -> str:
    m = HF_REPO_RX.search(url or "")
    return m.group(1).rstrip("/") if m else ""


def _hf_total_bytes(repo: str) -> int:
    try:
        url = f"https://huggingface.co/api/datasets/{repo}?blobs=true"
        req = urllib.request.Request(url, headers={"User-Agent": "x"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.load(r)
    except Exception:
        return -1
    sibs = d.get("siblings") or []
    return sum((s.get("size") or 0) for s in sibs)


def fetch_accepted_papers(verbose: bool = True) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    page = 200
    while True:
        params = urllib.parse.urlencode(
            {
                "content.venueid": VENUE_ID,
                "details": "presentation",
                "limit": page,
                "offset": offset,
            }
        )
        try:
            payload = _api_get(f"/notes?{params}")
        except urllib.error.HTTPError as e:
            sys.stderr.write(f"OpenReview HTTP {e.code} at offset {offset}; stopping\n")
            break
        notes = payload.get("notes", [])
        if not notes:
            break
        for n in notes:
            content = n.get("content", {}) or {}

            def _val(key: str) -> str:
                v = content.get(key, {})
                if isinstance(v, dict):
                    return v.get("value", "") or ""
                return v or ""

            rows.append(
                {
                    "paper_id": n.get("id", ""),
                    "primary_area": _val("primary_area"),
                    "dataset_url": _val("dataset_URL"),
                    "croissant_file": _val("croissant_file"),
                }
            )
        if verbose:
            sys.stderr.write(
                f"  offset={offset:>4d} fetched {len(notes)} (total {len(rows)})\n"
            )
        if len(notes) < page:
            break
        offset += page
    return rows


def project_rows(notes: list[dict], with_sizes: bool, verbose: bool) -> list[dict]:
    out: list[dict] = []
    for i, n in enumerate(notes, 1):
        host = _classify_host(n["dataset_url"])
        repo = _hf_repo(n["dataset_url"])
        size_bytes = ""
        if with_sizes and host == "huggingface" and repo:
            sz = _hf_total_bytes(repo)
            size_bytes = str(sz) if sz >= 0 else ""
            if verbose and i % 25 == 0:
                sys.stderr.write(f"  sized {i}/{len(notes)}\n")
            time.sleep(0.4)
        out.append(
            {
                "paper_id": n["paper_id"],
                "primary_area": n["primary_area"],
                "host": host,
                "repo": repo,
                "croissant_cached": "true" if n["croissant_file"] else "false",
                "size_bytes": size_bytes,
            }
        )
    out.sort(key=lambda r: r["paper_id"])
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent / "papers.csv",
        help="Where to write the regenerated CSV (default: scripts/papers.csv).",
    )
    parser.add_argument(
        "--no-sizes",
        action="store_true",
        help="Skip the per-repo HF size lookup; size_bytes column is left blank.",
    )
    parser.add_argument(
        "--quiet", action="store_true", help="Suppress progress output."
    )
    args = parser.parse_args()

    notes = fetch_accepted_papers(verbose=not args.quiet)
    if not notes:
        sys.stderr.write("No notes fetched. Aborting.\n")
        sys.exit(1)
    rows = project_rows(notes, with_sizes=not args.no_sizes, verbose=not args.quiet)
    fieldnames = [
        "paper_id",
        "primary_area",
        "host",
        "repo",
        "croissant_cached",
        "size_bytes",
    ]
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    sys.stderr.write(f"\nWrote {len(rows)} rows to {args.out}\n")


if __name__ == "__main__":
    main()
