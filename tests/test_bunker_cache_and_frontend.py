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
