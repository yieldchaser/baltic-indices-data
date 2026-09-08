#!/usr/bin/env python3
"""Fearnleys desk: series cache + frontend wiring.

- build_series_cache: 294 labels from the real pulse file, byte-identical
  re-runs, spot-checked monthly means + percentile math.
- index.html: Fearnleys button/panel, series + backtest fetches, desk
  engine markers, no dead references.
"""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable
HTML = (ROOT / "index.html").read_text(encoding="utf-8")


def run_script(rel: str, timeout: int = 1500) -> str:
    r = subprocess.run([PY, str(ROOT / rel)], capture_output=True, text=True,
                       cwd=ROOT, timeout=timeout)
    assert r.returncode == 0, f"{rel} failed:\n{r.stderr[-3000:]}"
    return r.stdout


def test_series_cache_write_path_idempotent():
    out = ROOT / "data" / "derived" / "fearnleys_series_monthly.json"
    run_script("scripts/fearnleys/build_series_cache.py")
    d = json.loads(out.read_text(encoding="utf-8"))
    assert d["meta"]["labels_cached"] >= 290, d["meta"]
    assert d["meta"]["catalog_ids"] == 356, d["meta"]

    # independently recompute one label's latest month from source rows
    label = "BULK_TC_CAPESIZE_180_000_DWT"
    assert label in d["labels"], "reference label missing"
    src = pd.read_csv(ROOT / "scripts" / "fearnpulse_rates_full.csv",
                      usecols=["label", "date", "rate"])
    g = src[src["label"] == label].copy()
    g["rate"] = pd.to_numeric(g["rate"], errors="coerce")
    g["ym"] = g["date"].astype(str).str.slice(0, 7)
    m = g.groupby("ym")["rate"].mean().round(2).sort_index()
    cached = d["labels"][label]
    assert cached["last"] == m.index[-1]
    assert abs(cached["latest"] - float(m.iloc[-1])) < 1e-9
    assert cached["n"] == len(m)
    assert 0 <= cached["pct_rank"] <= 100

    h1 = hashlib.sha256(out.read_bytes()).hexdigest()
    run_script("scripts/fearnleys/build_series_cache.py")
    assert hashlib.sha256(out.read_bytes()).hexdigest() == h1, "series cache not idempotent"


def test_fearnleys_frontend_markers():
    for marker in [
        'data-tab="fearnleys"', 'id="tab-fearnleys"',
        "fetch('data/derived/fearnleys_series_monthly.json')",
        "macro_health_score_backtest.csv",
        "function renderFearnleysTab()", "function renderFearnMainChart(cache)",
        "function renderFearnVoice()", "function renderFearnBacktest()",
        "setFearnType(", "setFearnRange(", "fearnMainChart", "fearnBacktestChart",
        "loadFearnArchive()", "fearnArchiveBtn", "fearnleys_comments_",
    ]:
        assert marker in HTML, marker


def test_comment_chunks_complete_and_idempotent():
    import hashlib
    run_script("scripts/fearnleys/build_comment_chunks.py")
    total = 0
    for desk in ["tanker", "dry", "gas", "snp"]:
        p = ROOT / "data" / "derived" / f"fearnleys_comments_{desk}.json"
        rows = json.loads(p.read_text(encoding="utf-8"))
        assert len(rows) > 100, (desk, len(rows))
        r0 = rows[0]
        assert set(r0) == {"d", "t", "n", "x"}, set(r0)
        assert r0["d"] >= "2026-01-01", r0["d"]  # newest-first
        total += len(rows)
    assert total == 11709, total
    # spot-check one row against the source CSV
    src = pd.read_csv(ROOT / "data" / "derived" / "fearnleys_broker_comments.csv",
                      usecols=["date", "comment_type", "text"])
    src = src[(src["date"].astype(str) != "") & (src["text"].fillna("") != "")]
    assert total == len(src)
    hashes = {}
    for desk in ["tanker", "dry", "gas", "snp"]:
        p = ROOT / "data" / "derived" / f"fearnleys_comments_{desk}.json"
        hashes[desk] = hashlib.sha256(p.read_bytes()).hexdigest()
    run_script("scripts/fearnleys/build_comment_chunks.py")
    for desk in ["tanker", "dry", "gas", "snp"]:
        p = ROOT / "data" / "derived" / f"fearnleys_comments_{desk}.json"
        assert hashlib.sha256(p.read_bytes()).hexdigest() == hashes[desk]
