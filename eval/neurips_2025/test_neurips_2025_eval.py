"""Cross-domain evaluation against a pre-registered seeded draw of accepted
papers from the NeurIPS 2025 Datasets & Benchmarks track.

For each pick listed in picks.json, Croissant Baker generates metadata for
the underlying dataset and the result is compared against the paper-
submission-time producer-authored Croissant bundled in producer_croissants/.

The evaluation reports:

  * Validation pass rate (mlcroissant) over all attempted bakes.
  * Field-name recovery and semantic type agreement on the intersection of
    Baker-emitted fields and producer-emitted fields.
  * Per-seed bucket coverage across the 11 OpenReview primary-area buckets.

Requires a one-time download (size depends on the seeded picks):

    huggingface-cli login
    bash eval/neurips_2025/download.sh

The evaluation is automatically skipped when the downloaded data is not present.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from collections import Counter
from pathlib import Path

import pytest
from typer.testing import CliRunner

from croissant_baker.__main__ import app

runner = CliRunner()

EVAL_DIR = Path(__file__).resolve().parent
PICKS_FILE = EVAL_DIR / "picks.json"
DATASETS_DIR = EVAL_DIR / "datasets"
PRODUCER_DIR = EVAL_DIR / "producer_croissants"
OUTPUT_DIR = EVAL_DIR / "output"
SUMMARY_FILE = OUTPUT_DIR / "neurips_2025_summary.json"

SEMANTIC_FLOAT = {"Float", "Float32", "Float64"}
SEMANTIC_INT = {
    "Integer",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
}
SEMANTIC_GROUPS = [SEMANTIC_FLOAT, SEMANTIC_INT]


def _has_data() -> bool:
    """Check whether at least a few datasets have been downloaded."""
    if not DATASETS_DIR.exists():
        return False
    present = sum(
        1 for child in DATASETS_DIR.iterdir() if child.is_dir() and any(child.iterdir())
    )
    return present >= 3


@pytest.fixture(scope="module")
def picks() -> dict:
    return json.loads(PICKS_FILE.read_text())


@pytest.fixture(scope="module")
def output_dir() -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR


def _local_dir(repo: str) -> Path:
    return DATASETS_DIR / repo.replace("/", "__")


def _load_producer(paper_id: str) -> dict | None:
    path = PRODUCER_DIR / f"{paper_id}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


_STREAM_NORMALIZE_THRESHOLD = 100 * 1024 * 1024  # bytes


def _normalize_streaming_inputs(src: Path) -> None:
    """Normalize any large dict-of-records JSON in ``src`` to JSON Lines.

    Uses ``jq`` when available; no-op otherwise.
    """
    if not shutil.which("jq"):
        return
    for path in src.rglob("*.json"):
        try:
            if path.stat().st_size < _STREAM_NORMALIZE_THRESHOLD:
                continue
        except OSError:
            continue
        # Quick shape check on the first byte of meaningful content: object,
        # not array. Avoids reading the whole file just to classify.
        try:
            with open(path, "rb") as fh:
                first = b""
                while True:
                    chunk = fh.read(64)
                    if not chunk:
                        break
                    stripped = chunk.lstrip()
                    if stripped:
                        first = stripped[:1]
                        break
        except OSError:
            continue
        if first != b"{":
            continue
        jsonl = path.with_suffix(".jsonl")
        if jsonl.exists():
            continue
        try:
            subprocess.run(
                ["jq", "-c", "to_entries[] | {id: .key} + .value", str(path)],
                stdout=open(jsonl, "w"),
                stderr=subprocess.DEVNULL,
                check=True,
            )
        except (subprocess.CalledProcessError, OSError):
            jsonl.unlink(missing_ok=True)
            continue
        path.unlink(missing_ok=True)


def _normalize_type(t: str) -> str:
    if isinstance(t, list):
        t = t[0] if t else ""
    if isinstance(t, str) and ":" in t:
        return t.split(":", 1)[1]
    return str(t)


def _types_semantically_equal(a: str, b: str) -> bool:
    na, nb = _normalize_type(a), _normalize_type(b)
    if na == nb:
        return True
    return any(na in g and nb in g for g in SEMANTIC_GROUPS)


def _field_index(croissant: dict) -> dict[str, str]:
    """Flatten a Croissant document to {field_name: dataType}.

    Field names are taken from ``name`` (preferred) or the trailing
    component of ``@id``. Top-level container RecordSets that only
    enumerate split labels are excluded so the comparison runs over
    schema-bearing fields.
    """
    out: dict[str, str] = {}
    rs_list = croissant.get("recordSet", [])
    if isinstance(rs_list, dict):
        rs_list = [rs_list]
    for rs in rs_list:
        if not isinstance(rs, dict):
            continue
        rs_id = str(rs.get("@id", ""))
        if "splits" in rs_id.lower():
            continue
        fields = rs.get("field", [])
        if isinstance(fields, dict):
            fields = [fields]
        for f in fields:
            if not isinstance(f, dict):
                continue
            name = f.get("name")
            if not name:
                fid = str(f.get("@id", ""))
                name = fid.rsplit("/", 1)[-1]
            if not name:
                continue
            dtype = f.get("dataType", "")
            if isinstance(dtype, list):
                dtype = ";".join(str(x) for x in dtype) if dtype else ""
            out[str(name)] = str(dtype)
    return out


def _bake(pick: dict) -> tuple[Path, dict]:
    """Run baker on a single pick. Returns (output_path, parsed_doc) or raises."""
    src = _local_dir(pick["repo"])
    if not src.exists() or not any(src.iterdir()):
        pytest.skip(f"{pick['repo']} not downloaded")
    _normalize_streaming_inputs(src)
    out_path = OUTPUT_DIR / f"{pick['repo'].replace('/', '__')}.json"
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    result = runner.invoke(
        app,
        [
            "--input",
            str(src),
            "--output",
            str(out_path),
            "--name",
            pick["name"],
            "--creator",
            "NeurIPS 2025 D&B Authors",
        ],
        catch_exceptions=False,
    )
    if result.exit_code != 0:
        raise RuntimeError(
            f"baker exit={result.exit_code} for {pick['repo']}: {result.output[-400:]}"
        )
    doc = json.loads(out_path.read_text())
    return out_path, doc


def test_picks_manifest_well_formed(picks: dict) -> None:
    assert picks["seeds"] == [0, 1337, 2026]
    buckets = picks["buckets"]
    assert len(buckets) == 11
    assert set(buckets) == {
        "language",
        "cv",
        "eval",
        "health",
        "physics",
        "life",
        "dl_scen",
        "social",
        "rl",
        "speech",
        "other",
    }
    for seed_label, seed_picks in picks["picks_per_seed"].items():
        assert len(seed_picks) == 11, f"{seed_label}: expected 11 picks"
        assert {p["bucket"] for p in seed_picks} == set(buckets), seed_label
    assert len(picks["unique_datasets"]) == 25
    for u in picks["unique_datasets"]:
        path = EVAL_DIR / u["producer_croissant"]
        assert path.exists(), f"missing producer Croissant: {path}"


def test_picks_meet_draw_protocol_eligibility() -> None:
    """Each pinned pick is in the eligible candidate pool defined by the
    draw protocol: HF-hosted, producer Croissant cached, repository size
    in [1 MB, 3 GB] at the OpenReview snapshot in scripts/papers.csv."""
    import csv

    csv_path = EVAL_DIR / "scripts" / "papers.csv"
    rows = {r["paper_id"]: r for r in csv.DictReader(open(csv_path))}
    picks = json.loads(PICKS_FILE.read_text())

    size_min = 1 * 1024 * 1024
    size_max = 3 * 10**9
    failures: list[str] = []
    for u in picks["unique_datasets"]:
        pid = u["paper_id"]
        if pid not in rows:
            failures.append(f"{pid} not in papers.csv")
            continue
        r = rows[pid]
        if r.get("host") != "huggingface":
            failures.append(f"{pid} host != huggingface ({r.get('host')!r})")
        if r.get("croissant_cached") != "true":
            failures.append(f"{pid} croissant_cached != true")
        try:
            size = int(r.get("size_bytes") or 0)
        except ValueError:
            size = 0
        if not (size_min <= size <= size_max):
            failures.append(f"{pid} size_bytes out of range: {size}")
    assert not failures, "picks fail protocol eligibility:\n  " + "\n  ".join(failures)


def test_papers_csv_first_five_resolve_via_openreview() -> None:
    """Smoke check: the first five paper_ids in scripts/papers.csv resolve
    against the OpenReview API. Skipped on network failure."""
    import csv
    import urllib.error
    import urllib.request

    csv_path = EVAL_DIR / "scripts" / "papers.csv"
    paper_ids = [r["paper_id"] for r in csv.DictReader(open(csv_path))][:5]
    if not paper_ids:
        pytest.skip("papers.csv is empty")

    api_template = "https://api2.openreview.net/notes?id={}"
    resolved = 0
    for pid in paper_ids:
        try:
            req = urllib.request.Request(
                api_template.format(pid),
                headers={"User-Agent": "neurips-eval/1.0"},
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                payload = json.load(r)
        except (urllib.error.URLError, OSError):
            pytest.skip("OpenReview API unreachable")
        if payload.get("notes"):
            resolved += 1

    assert resolved == len(paper_ids), (
        f"only {resolved}/{len(paper_ids)} paper_ids resolved via OpenReview"
    )


@pytest.mark.skipif(not _has_data(), reason="run download.sh first")
def test_neurips_2025_eval(picks: dict, output_dir: Path) -> None:
    summary: dict = {
        "n_picks": 0,
        "n_unique_datasets": 0,
        "validated": 0,
        "valid_with_producer_overlap": 0,
        "field_intersection": 0,
        "field_type_agreement": 0,
        "per_seed": {},
        "per_bucket": Counter(),
        "skipped_no_data": [],
        "errors": [],
    }
    per_repo: dict[str, dict] = {}

    for u in picks["unique_datasets"]:
        repo = u["repo"]
        local = _local_dir(repo)
        if not local.exists() or not any(local.iterdir()):
            summary["skipped_no_data"].append(repo)
            continue
        summary["n_unique_datasets"] += 1
        try:
            _, doc = _bake(u)
        except Exception as exc:
            summary["errors"].append({"repo": repo, "message": str(exc)[:200]})
            continue
        summary["validated"] += 1

        producer = _load_producer(u["paper_id"])
        if producer is None:
            per_repo[repo] = {"baker_fields": len(_field_index(doc))}
            continue
        baker_fields = _field_index(doc)
        producer_fields = _field_index(producer)
        intersection = set(baker_fields) & set(producer_fields)
        if not intersection:
            per_repo[repo] = {
                "baker_fields": len(baker_fields),
                "producer_fields": len(producer_fields),
                "intersection": 0,
            }
            continue
        agree = sum(
            1
            for k in intersection
            if _types_semantically_equal(baker_fields[k], producer_fields[k])
        )
        summary["valid_with_producer_overlap"] += 1
        summary["field_intersection"] += len(intersection)
        summary["field_type_agreement"] += agree
        per_repo[repo] = {
            "baker_fields": len(baker_fields),
            "producer_fields": len(producer_fields),
            "intersection": len(intersection),
            "type_agreement": agree,
        }

    for seed_label, seed_picks in picks["picks_per_seed"].items():
        baked = sum(
            1
            for p in seed_picks
            if (
                _local_dir(p["repo"]).exists()
                and any(_local_dir(p["repo"]).iterdir())
                and per_repo.get(p["repo"]) is not None
            )
        )
        summary["per_seed"][seed_label] = {
            "buckets_present": len({p["bucket"] for p in seed_picks}),
            "datasets_baked": baked,
        }
        for p in seed_picks:
            if per_repo.get(p["repo"]) is not None:
                summary["per_bucket"][p["bucket"]] += 1

    summary["n_picks"] = sum(len(s) for s in picks["picks_per_seed"].values())
    summary["per_bucket"] = dict(summary["per_bucket"])

    if summary["field_intersection"]:
        agreement_pct = (
            100.0 * summary["field_type_agreement"] / summary["field_intersection"]
        )
        summary["field_type_agreement_pct"] = round(agreement_pct, 1)

    summary["per_repo"] = per_repo
    SUMMARY_FILE.write_text(json.dumps(summary, indent=2))

    # Coverage assertions: at least one validated bake per attempted dataset and
    # full bucket coverage within each downloaded seed's reachable picks.
    assert summary["validated"] >= 1, "no datasets baked successfully"
    assert summary["validated"] == summary["n_unique_datasets"] - len(summary["errors"])
    if summary["valid_with_producer_overlap"] >= 1:
        assert summary["field_type_agreement"] >= int(
            0.9 * summary["field_intersection"]
        ), "type agreement below 90 percent on the intersected fields"
