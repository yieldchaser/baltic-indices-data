#!/usr/bin/env python3
"""Fearnleys desk: series cache + frontend wiring.

- build_series_cache: 294 labels from the real pulse file, byte-identical
  re-runs, spot-checked monthly means + percentile math.
- index.html: Fearnleys button/panel, sectioned desk engine (11 sub-nav
  sections over the wave-3 caches), series + backtest fetches, no dead refs.
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
        # sectioned desk engine (wave-3)
        "id=\"fearnSubNav\"", "function switchFearnSection(",
        "function renderFearnSection(", "id=\"fearnSec1\"", "id=\"fearnSec11\"",
        "function renderFearnOverview()", "function renderFearnTc()",
        "function renderFearnTank()", "function renderFearnNb()",
        "function renderFearnAc()", "function renderFearnGas(kind)",
        "renderFearnGas('LNG')", "renderFearnGas('LPG')", "function renderFearnFx()",
        "function renderFearnMuseum()", "function openFearnMusModal(",
        "fearnKpiGrid", "fearnTcChart", "fearnTankChart", "fearnNbChart",
        "fearnAcChart", "fearnLngChart", "fearnLpgChart", "fearnFxBody",
        "fearnMusWall", "fearnMusModal",
        # lazy cache loaders for the wave-3 caches
        "fearnleys_desk_tenor.json", "fearnleys_nb_prices.json",
        "fearnleys_asset_curves.json", "fearnleys_gas_extra.json",
        "fearnleys_fixtures_tape.json", "fearnleys_fixtures_facets.json",
        "DATA.fearnDeskTenor", "DATA.fearnNbPrices", "DATA.fearnAssetCurves",
        "DATA.fearnGasExtra", "DATA.fearnFixturesTape", "DATA.fearnFixturesFacets",
        # audit facts encoded in UI copy
        "2Y+ weekly only since 2021", "Coverage 2024 forward",
        "DAILY native", "discontinued ",
    ]:
        assert marker in HTML, marker


def test_fearnleys_desk_cache_shapes():
    """The six wave-3 caches the sectioned desk consumes must be loadable and
    structurally sound (the frontend consumes them; no rebuild here)."""
    base = ROOT / "data" / "derived"
    tenor = json.loads((base / "fearnleys_desk_tenor.json").read_text(encoding="utf-8"))
    assert tenor["meta"]["rows"] >= 10000
    assert tenor["rows"] and len(tenor["rows"][0]) == 4
    classes = {r[1] for r in tenor["rows"]}
    assert {"Capesize", "Panamax", "Supramax", "Handysize", "VLCC", "Suezmax", "Aframax"} <= classes
    assert {"4-6M", "1Y", "2Y", "3Y"} <= {r[2] for r in tenor["rows"]}

    nb = json.loads((base / "fearnleys_nb_prices.json").read_text(encoding="utf-8"))
    assert nb["meta"]["series"] == 17 and len(nb["series"]) == 17
    fams = {k.split("|")[0] for k in nb["series"]}
    # gas newbuildings are published in the 'other' family (LNG DF / LPG DF classes)
    assert {"bulk", "tanker", "other"} <= fams
    assert any("LNG DF" in k or "LPG DF" in k for k in nb["series"] if k.startswith("other|"))

    ac = json.loads((base / "fearnleys_asset_curves.json").read_text(encoding="utf-8"))
    assert ac["meta"]["classes"] == 33 and len(ac["classes"]) == 33
    assert len(ac["scrap"]) == 8
    assert {"dry_india", "tanker_india"} <= set(ac["scrap"])

    ge = json.loads((base / "fearnleys_gas_extra.json").read_text(encoding="utf-8"))
    assert {"lng_charter", "lpg_tc", "lpg_spot"} <= set(ge)

    tape = json.loads((base / "fearnleys_fixtures_tape.json").read_text(encoding="utf-8"))
    assert tape["meta"]["rows"] == len(tape["rows"]) >= 18000
    schema = tape["meta"]["schema"]
    short = set(schema.values())
    for row in tape["rows"][:50]:
        assert set(row) <= short | {"d"}, row

    facets = json.loads((base / "fearnleys_fixtures_facets.json").read_text(encoding="utf-8"))
    assert facets["total_rows"] >= 300000
    for k in ["dept", "segment", "charterer", "route", "commodity"]:
        assert isinstance(facets[k], list) and facets[k]
    assert facets["ytd_charterer_league"] and facets["monthly_counts"]


def test_comment_chunks_complete_and_idempotent():
    run_script("scripts/fearnleys/build_comment_chunks.py")
    total = 0
    hashes = {}
    for desk in ["tanker", "dry", "gas", "snp"]:
        p = ROOT / "data" / "derived" / f"fearnleys_comments_{desk}.json"
        rows = json.loads(p.read_text(encoding="utf-8"))
        assert len(rows) > 100, (desk, len(rows))
        r0 = rows[0]
        assert set(r0) == {"d", "t", "n", "x"}, set(r0)
        assert r0["d"] >= "2026-01-01", r0["d"]  # newest-first
        total += len(rows)
        hashes[desk] = hashlib.sha256(p.read_bytes()).hexdigest()
    src = pd.read_csv(ROOT / "data" / "derived" / "fearnleys_broker_comments.csv",
                      usecols=["date", "comment_type", "text"])
    src = src[(src["date"].astype(str) != "") & (src["text"].fillna("") != "")]
    assert total == len(src)
    run_script("scripts/fearnleys/build_comment_chunks.py")
    for desk in ["tanker", "dry", "gas", "snp"]:
        p = ROOT / "data" / "derived" / f"fearnleys_comments_{desk}.json"
        assert hashlib.sha256(p.read_bytes()).hexdigest() == hashes[desk]
