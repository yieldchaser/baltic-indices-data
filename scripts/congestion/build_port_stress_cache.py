#!/usr/bin/env python3
"""
Build Port Stress & Arrival Envelopes Pre-Aggregated Cache
=========================================================
Aggregates data/derived/port_stress_matrix.csv into a lightweight frontend JSON cache:
data/derived/port_stress_summary.json
"""

import json
import logging
from pathlib import Path
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT / "data" / "derived"
OUTPUT_FILE = DATA_DIR / "port_stress_summary.json"


def sanitize_float(val, default=0.0):
    if val is None or pd.isna(val):
        return default
    if isinstance(val, (int, float, np.integer, np.floating)):
        if np.isinf(val) or np.isnan(val):
            return default
        return round(float(val), 2)
    s = str(val).replace(",", "").strip()
    try:
        f = float(s)
        if np.isinf(f) or np.isnan(f):
            return default
        return round(f, 2)
    except (ValueError, TypeError):
        return default


def main():
    src_csv = DATA_DIR / "port_stress_matrix.csv"
    if not src_csv.exists():
        raise FileNotFoundError(f"Missing {src_csv}")

    logging.info("Reading %s...", src_csv)
    df = pd.read_csv(src_csv)

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["port_locode", "asset_class", "date"]).reset_index(drop=True)

    latest_date_str = df["date"].max().strftime("%Y-%m-%d")

    # Group by (port_locode, asset_class) to handle ports with multiple segments (e.g. NLRTM Dry Bulk vs Tankers)
    ports_map = {}
    hubs_latest = []

    groups = df.groupby(["port_locode", "asset_class"], as_index=False)

    for (locode, asset_class), group in df.groupby(["port_locode", "asset_class"]):
        group = group.sort_values("date").reset_index(drop=True)
        latest_row = group.iloc[-1]
        prev_row = group.iloc[-2] if len(group) > 1 else latest_row

        calls_now = sanitize_float(latest_row["live_weekly_calls"])
        calls_prev = sanitize_float(prev_row["live_weekly_calls"])
        wow_change = sanitize_float(calls_now - calls_prev)
        zscore = sanitize_float(latest_row["arrival_deviation_zscore"])
        stress = str(latest_row["stress_flag"]).upper()

        unique_key = f"{locode}_{asset_class.replace(' ', '_').lower()}"
        port_name = str(latest_row["portname"])
        country = str(latest_row["country"])
        portid = str(latest_row.get("portid", ""))

        # Series (keep weekly observations)
        series = []
        for _, r in group.iterrows():
            d_str = r["date"].strftime("%Y-%m-%d")
            series.append({
                "d": d_str,
                "c": sanitize_float(r["live_weekly_calls"]),
                "mean": sanitize_float(r["hist_mean"]),
                "min": sanitize_float(r["hist_min"]),
                "max": sanitize_float(r["hist_max"]),
                "z": sanitize_float(r["arrival_deviation_zscore"]),
                "flag": str(r["stress_flag"])
            })

        hub_info = {
            "key": unique_key,
            "locode": locode,
            "portid": portid,
            "name": port_name,
            "country": country,
            "asset_class": asset_class,
            "latest_date": latest_row["date"].strftime("%Y-%m-%d"),
            "weekly_calls": calls_now,
            "prev_calls": calls_prev,
            "wow_change": wow_change,
            "hist_mean": sanitize_float(latest_row["hist_mean"]),
            "hist_min": sanitize_float(latest_row["hist_min"]),
            "hist_max": sanitize_float(latest_row["hist_max"]),
            "hist_std": sanitize_float(latest_row["hist_std"]),
            "zscore": zscore,
            "stress_flag": stress,
            "interpretation": str(latest_row.get("signal_interpretation", "")),
            "series_count": len(series)
        }

        hubs_latest.append(hub_info)
        ports_map[unique_key] = {
            "metadata": hub_info,
            "series": series
        }

    # Summary metrics
    surge_count = sum(1 for h in hubs_latest if h["stress_flag"] == "SURGE")
    collapse_count = sum(1 for h in hubs_latest if h["stress_flag"] == "COLLAPSE")
    normal_count = sum(1 for h in hubs_latest if h["stress_flag"] == "NORMAL")

    asset_counts = {}
    for h in hubs_latest:
        ac = h["asset_class"]
        asset_counts[ac] = asset_counts.get(ac, 0) + 1

    # Sort hubs by stress severity: SURGE first (highest zscore), then COLLAPSE (lowest zscore), then NORMAL
    def sort_severity(h):
        flag = h["stress_flag"]
        z = h["zscore"]
        if flag == "SURGE":
            return (0, -z)
        elif flag == "COLLAPSE":
            return (1, z)
        return (2, -z)

    hubs_latest.sort(key=sort_severity)

    # Wave-1 hub-truth validation (non-fatal): compare observed counts against
    # the canonical universe (50 series / 41 physical hubs, single-sourced
    # from scripts/compute_port_stress_matrix.py via port_universe.py).
    # importlib-by-path: no package (__init__.py) assumptions, never fatal.
    try:
        import importlib.util

        _pu_path = Path(__file__).resolve().parent / "port_universe.py"
        _spec = importlib.util.spec_from_file_location("port_universe", _pu_path)
        _pu = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_pu)
        _pu.validate_hub_counts(len(hubs_latest), len({h["locode"] for h in hubs_latest}))
        logging.info("Hub counts match canonical truth: %d series / %d physical hubs.", _pu.SERIES_COUNT, _pu.PHYSICAL_HUB_COUNT)
    except Exception as e:  # noqa: BLE001 — validation must never break the cache
        logging.warning("Hub validation skipped/drifted vs canonical truth: %s", e)

    output_payload = {
        "metadata": {
            "source": "IMF PortWatch Daily AIS Gateway & Tonnage Squeeze Engine",
            "updated": latest_date_str,
            "total_hubs": len(hubs_latest),
            "coverage": "January 2019 – August 2026 (Continuous 5-Year Rolling Envelopes)",
            "methodology": "Z-score = (Live Weekly Calls - 5Y Mean) / (5Y StdDev). Squeeze Alert: |Z| >= 1.5"
        },
        "summary": {
            "total_monitored": len(hubs_latest),
            "surge_alerts": surge_count,
            "collapse_alerts": collapse_count,
            "normal_hubs": normal_count,
            "by_asset_class": asset_counts
        },
        "hubs": hubs_latest,
        "ports_series": ports_map
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, separators=(",", ":"))

    logging.info("Saved %s (%.1f KB).", OUTPUT_FILE, OUTPUT_FILE.stat().st_size / 1024)


if __name__ == "__main__":
    main()
