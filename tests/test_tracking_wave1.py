#!/usr/bin/env python3
"""Wave-1 tracking tab: builders + frontend wiring.

- vessel_leg_economics: latest leg per IMO from the real voyage master,
  byte-identical re-runs, 100% lineup-IMO coverage.
- chokepoint summary carries gc/roro sector series for the new buttons.
- index.html: voyageLegs fetch present; dead sector/tonnage/envelope stores
  removed; gencargo/roro chart branches + leg tooltip rows present.

No fabricated data: expectations recomputed from real source rows.
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


def test_leg_economics_write_path_idempotent():
    out = ROOT / "data" / "derived" / "vessel_leg_economics.json"
    run_script("scripts/geospatial/build_vessel_leg_economics.py")
    legs = json.loads(out.read_text(encoding="utf-8"))
    assert len(legs) > 2000, len(legs)

    # independently recompute one IMO's latest leg from the master
    master = pd.read_csv(
        ROOT / "data" / "geospatial" / "vessel_voyage_tracks_master.csv",
        usecols=["imo_number", "portname", "arrival_date", "transit_days", "distance_nm"])
    master["imo_number"] = master["imo_number"].astype(str).str.strip()
    sample_imo = sorted(legs)[0]
    g = master[master["imo_number"] == sample_imo].copy()
    g["arrival_date"] = pd.to_datetime(g["arrival_date"], errors="coerce")
    last = g.sort_values("arrival_date").iloc[-1]
    assert legs[sample_imo]["transit_days"] == round(float(last["transit_days"]), 1)
    if pd.notna(last["distance_nm"]) and float(last["distance_nm"]) > 0:
        assert legs[sample_imo]["distance_nm"] == round(float(last["distance_nm"]), 1)
        assert legs[sample_imo]["avg_speed_kn"] > 0

    # every lineup IMO resolves (keys share the IMOxxxxxxxx format)
    lineup = pd.read_csv(ROOT / "data" / "geospatial" / "port_lineups_active.csv",
                         usecols=["imo_number"])
    lineup_imos = {str(v).strip() for v in lineup["imo_number"]}
    assert lineup_imos <= set(legs), lineup_imos - set(legs)

    h1 = hashlib.sha256(out.read_bytes()).hexdigest()
    run_script("scripts/geospatial/build_vessel_leg_economics.py")
    assert hashlib.sha256(out.read_bytes()).hexdigest() == h1, "legs re-run changed bytes"


def test_chokepoint_summary_has_gc_roro_series():
    d = json.load(open(ROOT / "data" / "congestion" / "chokepoint_geo_summary.json",
                       encoding="utf-8"))
    cps = d["chokepoints"]
    assert len(cps) == 28, len(cps)
    for cp in cps:
        m0 = cp["monthly_series"][0]
        assert "gc_avg" in m0 and "roro_avg" in m0, cp["name"]
        d0 = cp["recent_daily_series"][0]
        assert "gc" in d0 and "roro" in d0, cp["name"]
    # spot-check one monthly gc value against the daily CSV
    daily = pd.read_csv(ROOT / "data" / "congestion" / "chokepoint_transits_daily.csv",
                        usecols=["date", "portname", "n_general_cargo"])
    jan19 = daily[(daily["portname"] == "Malacca Strait")
                  & daily["date"].str.startswith("2019-01")]["n_general_cargo"].mean()
    mal = next(c for c in cps if c["name"] == "Malacca Strait")
    mjan = next(m for m in mal["monthly_series"] if m["m"] == "2019-01")
    assert abs(mjan["gc_avg"] - round(float(jan19), 1)) < 1e-9


def test_tracking_frontend_markers():
    for marker in [
        "fetch('data/derived/vessel_leg_economics.json')",
        "trackingLiveCounts()", "sortTrackingData(", "toggleTonnageLens()",
        "r.gc_avg", "r.roro_avg", "data-tt-leg-days", "Anchored Since",
        "setChokepointMetric('gencargo')", "setChokepointMetric('roro')",
    ]:
        assert marker in HTML, marker
    for dead in [
        "chokepoint_sector_monthly.json", "chokepoint_sector_latest.json",
        "portwatch_latest_tonnage.json", "DATA.envelopeMatrix",
        "portwatchTonnageByPort",
    ]:
        assert dead not in HTML, f"dead store still wired: {dead}"
