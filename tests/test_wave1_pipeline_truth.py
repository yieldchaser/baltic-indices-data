#!/usr/bin/env python3
"""
Wave-1 pipeline truth: write-path + idempotency + hub reconciliation.

Every builder test proves the write path the same way:
  run script -> re-read output from disk -> assert real content ->
  SHA-256 -> re-run -> assert byte-identical (idempotent).

No fabricated data: all expectations are recomputed from the real source rows.
"""

import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sha256_stable(path: Path) -> str:
    """SHA over JSON with volatile meta.generated_at neutralized.

    generated_at is a live wall-clock stamp by design (see
    build_bunker_cache.py meta block), so byte-identity across runs is
    impossible; everything else must be deterministic.
    """
    raw = path.read_bytes()
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return hashlib.sha256(raw).hexdigest()
    if isinstance(data, dict) and isinstance(data.get("meta"), dict):
        data["meta"] = {k: v for k, v in data["meta"].items() if k != "generated_at"}
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode("utf-8")).hexdigest()


def run_script(rel: str, timeout: int = 1500) -> str:
    r = subprocess.run(
        [PY, str(ROOT / rel)],
        capture_output=True, text=True, cwd=ROOT, timeout=timeout,
    )
    assert r.returncode == 0, f"{rel} failed:\n{r.stderr[-3000:]}"
    return r.stdout


def load_universe():
    """Load the canonical universe without package assumptions."""
    spec = importlib.util.spec_from_file_location(
        "port_universe", ROOT / "scripts" / "congestion" / "port_universe.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_canonical_universe_single_source():
    """50 port-asset series across 41 physical hubs, single-sourced."""
    uni = load_universe()
    assert uni.SERIES_COUNT == 50
    assert uni.PHYSICAL_HUB_COUNT == 41
    assert len(uni.PORT_METADATA) == 50
    assert len(uni.PHYSICAL_HUBS) == 41
    counts = pd.Series([e["asset_class"] for e in uni.PORT_METADATA]).value_counts().to_dict()
    assert counts == {"Dry Bulk": 16, "Tankers": 11, "LPG": 11, "LNG": 12}


def test_bunker_cache_write_path_idempotent():
    """Bunker cache: write -> re-read -> real change_7d -> re-run unchanged."""
    out = ROOT / "data" / "bunkers" / "bunker_frontend_summary.json"
    run_script("scripts/bunkers/build_bunker_cache.py")

    # re-read after write
    data = json.loads(out.read_text(encoding="utf-8"))
    ports = {p["name"]: p for p in data["ports"]}
    assert "Singapore" in ports
    sg = ports["Singapore"]

    # independently recompute Singapore VLSFO 7d change from master rows
    master = pd.read_csv(ROOT / "data" / "bunkers" / "bunker_master_historical.csv",
                         usecols=["observation_date", "port_name", "grade", "price_usd"])
    v = master[(master["port_name"] == "Singapore") & (master["grade"] == "VLSFO")].copy()
    v["observation_date"] = pd.to_datetime(v["observation_date"], errors="coerce")
    v["price_usd"] = pd.to_numeric(v["price_usd"], errors="coerce")
    v = v.dropna().sort_values("observation_date")
    latest_dt = pd.to_datetime(sg["latest_date"])
    cand = v[v["observation_date"] <= latest_dt]
    last = cand.iloc[-1]
    ref = cand[cand["observation_date"] <= (last["observation_date"] - pd.Timedelta(days=7))].iloc[-1]
    expected = round(float(last["price_usd"]) - float(ref["price_usd"]), 2)

    assert sg["change_7d"] == expected, f"{sg['change_7d']} != {expected}"
    assert sg["change_7d"] != 0.0, "change_7d still stuck at 0.0"
    assert sg["vlsfo"] and sg["vlsfo"] > 0

    h1 = sha256_stable(out)
    run_script("scripts/bunkers/build_bunker_cache.py")
    assert sha256_stable(out) == h1, "bunker cache re-run changed content (not idempotent)"


def test_port_stress_matrix_write_path_idempotent():
    """Stress matrix: write -> re-read 50 series / 41 hubs -> re-run unchanged."""
    out = ROOT / "data" / "derived" / "port_stress_matrix.csv"
    run_script("scripts/compute_port_stress_matrix.py")

    df = pd.read_csv(out)
    assert len(df) > 0
    assert df["port_locode"].nunique() == 41, df["port_locode"].nunique()
    assert df.groupby(["port_locode", "asset_class"]).ngroups == 50
    assert set(df["asset_class"].unique()) == {"Dry Bulk", "Tankers", "LPG", "LNG"}
    assert (df["hist_mean"].notna()).all()

    h1 = sha256(out)
    run_script("scripts/compute_port_stress_matrix.py")
    assert sha256(out) == h1, "stress matrix re-run changed bytes (not idempotent)"


def test_port_stress_cache_write_path_idempotent():
    """Stress cache: write -> re-read 50 hubs -> re-run unchanged."""
    out = ROOT / "data" / "derived" / "port_stress_summary.json"
    run_script("scripts/congestion/build_port_stress_cache.py")

    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["summary"]["total_monitored"] == 50
    assert data["metadata"]["total_hubs"] == 50
    assert len(data["hubs"]) == 50
    assert len({h["locode"] for h in data["hubs"]}) == 41
    assert len(data["ports_series"]) == 50
    keys = [k.lower() for k in data["ports_series"]]
    assert any("auphe" in k for k in keys)

    h1 = sha256(out)
    run_script("scripts/congestion/build_port_stress_cache.py")
    assert sha256(out) == h1, "stress cache re-run changed bytes (not idempotent)"


def test_frontend_fallback_matches_canonical():
    """Frontend has no hub list of its own; its || 50 default matches truth."""
    html = (ROOT / "index.html").read_text(encoding="utf-8", errors="replace")
    assert "sm.total_monitored || 50" in html
    assert "portStressSummaryPromise" in html
