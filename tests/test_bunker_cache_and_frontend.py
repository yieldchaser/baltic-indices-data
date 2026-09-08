#!/usr/bin/env python3
"""
Unit and Integration Tests for Bunker Frontend Cache and Dashboard Integration
"""

import json
import os
import subprocess
import pytest

def test_bunker_frontend_summary_json_exists():
    path = "data/bunkers/bunker_frontend_summary.json"
    assert os.path.exists(path), f"Missing cache file: {path}"
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert "kpis" in data
    assert "ports" in data
    assert "forward_curves_12m" in data
    assert "physical_volumes" in data
    assert "scrubber_economics" in data
    assert "monthly_series" in data
    assert "daily_series" in data

    # Verify benchmark daily series
    daily_series = data["daily_series"]
    assert "Singapore" in daily_series
    assert "Rotterdam" in daily_series
    assert len(daily_series["Singapore"]) > 200
    assert "d" in daily_series["Singapore"][0]
    assert "vlsfo" in daily_series["Singapore"][0]

    # Verify 221 ports
    ports = data["ports"]
    assert len(ports) == 221, f"Expected 221 ports, got {len(ports)}"

    # Check key global hubs
    port_names = set(p["name"] for p in ports)
    assert "Singapore" in port_names
    assert "Rotterdam" in port_names
    assert "Houston" in port_names
    assert "Fujairah" in port_names

    # Verify coordinates exist on all ports
    for p in ports:
        assert "lat" in p and "lon" in p
        assert isinstance(p["lat"], (int, float))
        assert isinstance(p["lon"], (int, float))
        assert -90 <= p["lat"] <= 90
        assert -180 <= p["lon"] <= 180

    # Verify KPIs
    kpis = data["kpis"]
    assert kpis["global_vlsfo"] > 0
    assert kpis["global_hsfo"] > 0
    assert kpis["global_mgo"] > 0
    assert kpis["singapore_hi5"] > 0
    assert kpis["eu_ets_carbon_eur"] > 0
    assert kpis["singapore_monthly_vol_mt"] > 1000000

def test_bunker_kpis_live_chg_provenance():
    """Wave-1: KPI levels/deltas derived from owned CSVs, never stale hardcodes."""
    import csv
    with open("data/bunkers/bunker_frontend_summary.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    kpis = data["kpis"]
    for field in ["global_vlsfo_chg", "global_hsfo_chg", "global_mgo_chg",
                  "singapore_hi5_chg", "global_obs", "global_prev_obs",
                  "hi5_obs", "hi5_prev_obs", "singapore_vol_period"]:
        assert field in kpis, f"Missing live KPI field: {field}"

    # Cross-check global composite vs bunker_prices_daily.csv latest/prev obs
    rows = list(csv.DictReader(open("data/bunkers/bunker_prices_daily.csv")))
    g = [r for r in rows if r["port"] == "global_average_bunker_price"]
    dates = sorted(set(r["date"] for r in g))
    px = {(r["date"], r["fuel_grade"]): float(r["price_usd_mt"]) for r in g}
    latest, prev = dates[-1], dates[-2]
    assert kpis["global_obs"] == latest
    assert kpis["global_vlsfo"] == px[(latest, "VLSFO")]
    assert kpis["global_mgo"] == px[(latest, "MGO")]
    assert kpis["global_hsfo"] == px[(latest, "IFO380")]
    assert kpis["global_vlsfo_chg"] == round(px[(latest, "VLSFO")] - px[(prev, "VLSFO")], 2)
    assert kpis["global_mgo_chg"] == round(px[(latest, "MGO")] - px[(prev, "MGO")], 2)
    assert kpis["global_hsfo_chg"] == round(px[(latest, "IFO380")] - px[(prev, "IFO380")], 2)

    # Cross-check Singapore monthly volume + YoY vs physical volumes CSV
    vol = list(csv.DictReader(open("data/bunkers/bunker_physical_sales_volumes.csv")))
    sgm = sorted([r for r in vol if r["port"] == "Singapore" and r["metric"] == "Sales_Monthly_MT"],
                 key=lambda r: r["period"])
    assert kpis["singapore_monthly_vol_mt"] == float(sgm[-1]["volume_mt"])
    assert kpis["singapore_vol_period"] == sgm[-1]["period"]

def test_bunker_bix_coverage():
    """Wave-1: BIX strip source — 150 rows, 5 indices x 3 grades, change/high/low present."""
    with open("data/bunkers/bunker_frontend_summary.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    bix = data["benchmarks_bix"]
    assert len(bix) == 150, f"Expected 150 BIX rows, got {len(bix)}"
    assert set(x["index"] for x in bix) == {"BIX_World", "BIX_World3", "BIX_APAC", "BIX_EMEA", "BIX_Americas"}
    assert set(x["grade"] for x in bix) == {"VLSFO", "IFO380", "MGO"}
    for r in bix:
        for field in ["date", "index", "grade", "price", "change", "change_pct", "low", "high"]:
            assert field in r, f"BIX row missing {field}"
        assert r["price"] > 0 and r["low"] > 0 and r["high"] >= r["low"]

def test_bunker_altfuels_no_zerofill():
    """Wave-1: LNG/MEOH/EUA parsed; non-null only; nulls are None, never 0-filled as data."""
    with open("data/bunkers/bunker_frontend_summary.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    ports = data["ports"]
    for key in ["lng", "meoh", "eua"]:
        nn = [p for p in ports if p.get(key) is not None]
        assert len(nn) >= 1, f"No verified {key.upper()} indications"
        for p in nn:
            assert p[key] > 0, f"{key} zero-filled at {p['name']}"
    for p in ports:
        for key in ["lng", "meoh", "eua", "bio"]:
            assert p.get(key) is None or p[key] > 0, f"{key} invalid at {p['name']}: {p.get(key)}"

def test_bunker_coverage_honesty():
    """Wave-1: 35 daily ports -> 186 monthly-only; volumes SG+RTM only."""
    with open("data/bunkers/bunker_frontend_summary.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    n_ports = len(data["ports"])
    n_daily = len(data["daily_series"])
    assert n_ports == 221
    assert n_daily == 35, f"Expected 35 daily ports, got {n_daily}"
    assert n_ports - n_daily == 186, "monthly-only fallback count must be 186"
    assert set(data["physical_volumes"].keys()) == {"Singapore", "Rotterdam"}
    assert len(data["forward_curves_12m"]) == 6

def test_build_bunker_cache_script():
    script_path = "scripts/bunkers/build_bunker_cache.py"
    assert os.path.exists(script_path), f"Missing build script: {script_path}"
    
    # Run script and ensure exit code 0
    res = subprocess.run(["python", script_path], capture_output=True, text=True)
    assert res.returncode == 0, f"Script failed with output: {res.stderr}"
    assert "Successfully generated" in res.stdout

def test_index_html_bunkers_integration():
    with open("index.html", "r", encoding="utf-8") as f:
        c = f.read()

    # Verify tab button and panel
    assert '<button class="tab-btn" data-tab="bunkers">Bunkers</button>' in c
    assert '<div class="tab-panel" id="tab-bunkers">' in c
    assert '<div id="bunkerGeoMap"' in c
    assert '<canvas id="bunkerMainChart"' in c

    # Verify all 4 subviews
    assert 'id="bunkersSubviewSpot"' in c
    assert 'id="bunkersSubviewForward"' in c
    assert 'id="bunkersSubviewVolumes"' in c
    assert 'id="bunkersSubviewScrubber"' in c

    # Verify dynamic tooltip support
    assert "type === 'bunker-kpi'" in c
    assert "type === 'bunker-port-cell'" in c
    assert "type === 'bunker-fwd-cell'" in c
    assert "type === 'bunker-scrubber-cell'" in c
    assert "type === 'tracking-hud'" in c

    # Verify data pipeline wiring
    assert "bunkerSummary: null" in c
    assert "data/bunkers/bunker_frontend_summary.json" in c
    assert "renderBunkersTab()" in c

def test_index_html_bunkers_wave1():
    """Wave-1 rebuild markers: one-truth sync, BIX strip, alt fuels, live deltas,
    MoM fallback, selected-row fix, honesty labels, BNKR idiom."""
    with open("index.html", "r", encoding="utf-8") as f:
        c = f.read()

    # (1) legacy Tracking mini-view synced to one truth
    assert "ONE-TRUTH SYNC" in c
    assert "synced from BUNKERS tab" in c
    # (2) BIX benchmark strip/chart from benchmarks_bix
    assert 'id="bunkerBixStrip"' in c
    assert 'id="bunkerBixChart"' in c
    assert "renderBunkersBixStrip" in c
    assert "benchmarks_bix" in c
    # (3) alt-fuel subview, non-null only, never zero-filled
    assert 'id="bunkersSubviewAltfuels"' in c
    assert 'id="bunkersAltFuelsTableBody"' in c
    assert "renderBunkersAltFuels" in c
    assert "never zero-filled" in c
    # (4) live KPI deltas from kpis chg fields
    assert "bunkerKpiVlsfoSub" in c and "global_vlsfo_chg" in c
    assert "singapore_vol_period" in c
    # (5) spot deltas + sparklines with labeled MoM fallback
    assert "bunkerPortDelta" in c and "bunkerSpark12M" in c
    assert "MoM*" in c
    # (6) selected-row CSS fix (border-left on <tr> is dead under border-collapse)
    assert ".bunkers-port-row.selected td" in c
    assert "box-shadow: inset 3px 0 0 var(--accent)" in c
    # (7) honest coverage labels
    assert "2 PORTS ONLY" in c
    assert "MONTHLY FALLBACK" in c
    assert "monthly-only" in c
    # (8) BNKR HUD idiom distinct from tracking
    assert "bnkr-tag" in c
    assert "BNKR" in c
