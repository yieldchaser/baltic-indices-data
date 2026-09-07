#!/usr/bin/env python3
"""
Build Offshore & OSV Pre-Aggregated Cache
=========================================
Aggregates data/derived/seabrokers_osv_dayrates.csv and reports/seabrokers_catalog.json
into a high-speed frontend cache: data/derived/offshore_summary.json
"""

import json
import logging
from pathlib import Path
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT / "data" / "derived"
REPORTS_DIR = ROOT / "reports"
OUTPUT_FILE = DATA_DIR / "offshore_summary.json"


def sanitize_float(val, default=0.0):
    if val is None or pd.isna(val):
        return default
    if isinstance(val, (int, float, np.integer, np.floating)):
        if np.isinf(val) or np.isnan(val):
            return default
        return round(float(val), 2)
    s = str(val).replace("%", "").replace(",", "").replace("+", "").strip()
    try:
        f = float(s)
        if np.isinf(f) or np.isnan(f):
            return default
        return round(f, 2)
    except (ValueError, TypeError):
        return default


def main():
    dayrates_csv = DATA_DIR / "seabrokers_osv_dayrates.csv"
    if not dayrates_csv.exists():
        raise FileNotFoundError(f"Missing {dayrates_csv}")

    logging.info("Reading %s...", dayrates_csv)
    df = pd.read_csv(dayrates_csv)

    # Clean date
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["date", "category"]).reset_index(drop=True)

    # Standardize category keys
    cat_map = {
        "AHTS DUTIES AHTS > 22,000 BHP": {
            "key": "large_ahts",
            "name": "Large AHTS (>22,000 BHP)",
            "short": "Large AHTS",
            "type": "AHTS",
            "spec": "Deepwater Anchor Handling (>200t Bollard Pull)",
            "util_col": "large_ahts_util"
        },
        "AHTS DUTIES AHTS < 22,000 BHP": {
            "key": "med_ahts",
            "name": "Medium AHTS (<22,000 BHP)",
            "short": "Med AHTS",
            "type": "AHTS",
            "spec": "Midwater Anchor Handling (150–200t Bollard Pull)",
            "util_col": "med_ahts_util"
        },
        "SUPPLY DUTIES PSVS > 900M2": {
            "key": "large_psv",
            "name": "Large PSV (>900 m²)",
            "short": "Large PSV",
            "type": "PSV",
            "spec": "Platform Supply Deepwater Deck (>900m² clear deck)",
            "util_col": "large_psv_util"
        },
        "SUPPLY DUTIES PSVS < 900M2": {
            "key": "med_psv",
            "name": "Medium PSV (<900 m²)",
            "short": "Med PSV",
            "type": "PSV",
            "spec": "Platform Supply Midwater Deck (<900m² clear deck)",
            "util_col": "med_psv_util"
        }
    }

    # Extract clean utilization percentage as number
    def parse_util(val):
        if pd.isna(val):
            return 0.0
        s = str(val).replace("%", "").strip()
        try:
            return float(s)
        except ValueError:
            return 0.0

    for ucol in ["med_psv_util", "large_psv_util", "med_ahts_util", "large_ahts_util"]:
        if ucol in df.columns:
            df[ucol + "_num"] = df[ucol].apply(parse_util)

    # Build category series and latest KPIs
    categories_out = {}
    ledger_rows = []

    latest_date_str = df["date"].max().strftime("%Y-%m-%d")
    latest_month_name = df[df["date"] == df["date"].max()]["report_month"].iloc[0]

    for raw_cat, meta in cat_map.items():
        sub = df[df["category"] == raw_cat].sort_values("date")
        if sub.empty:
            continue

        latest_row = sub.iloc[-1]
        util_val = latest_row.get(meta["util_col"] + "_num", 0.0)

        # 52-week (last 12 months) high and low
        last_12 = sub.tail(12)
        hi_52w = sanitize_float(last_12["avg_dayrate_gbp"].max())
        lo_52w = sanitize_float(last_12["avg_dayrate_gbp"].min())
        all_time_hi = sanitize_float(sub["max_dayrate_gbp"].max())
        all_time_lo = sanitize_float(sub["min_dayrate_gbp"].min())

        series = []
        for _, r in sub.iterrows():
            d_str = r["date"].strftime("%Y-%m-%d")
            m_label = r["date"].strftime("%b %Y")
            avg_rate = sanitize_float(r["avg_dayrate_gbp"])
            min_rate = sanitize_float(r["min_dayrate_gbp"])
            max_rate = sanitize_float(r["max_dayrate_gbp"])
            yoy_val = sanitize_float(r["yoy_change_pct"])
            prev_rate = sanitize_float(r["prev_year_dayrate_gbp"])
            u_pct = sanitize_float(r.get(meta["util_col"] + "_num", 0.0))

            series.append({
                "date": d_str,
                "label": m_label,
                "avg_dayrate": avg_rate,
                "min_dayrate": min_rate,
                "max_dayrate": max_rate,
                "prev_year_dayrate": prev_rate,
                "yoy_change_pct": yoy_val,
                "utilization_pct": u_pct
            })

            ledger_rows.append({
                "date": d_str,
                "label": m_label,
                "category_key": meta["key"],
                "category_name": meta["name"],
                "short_name": meta["short"],
                "type": meta["type"],
                "avg_dayrate": avg_rate,
                "min_dayrate": min_rate,
                "max_dayrate": max_rate,
                "prev_year_dayrate": prev_rate,
                "yoy_change_pct": yoy_val,
                "utilization_pct": u_pct
            })

        categories_out[meta["key"]] = {
            "key": meta["key"],
            "name": meta["name"],
            "short": meta["short"],
            "type": meta["type"],
            "spec": meta["spec"],
            "latest_dayrate_gbp": sanitize_float(latest_row["avg_dayrate_gbp"]),
            "prev_year_dayrate_gbp": sanitize_float(latest_row["prev_year_dayrate_gbp"]),
            "yoy_change_pct": sanitize_float(latest_row["yoy_change_pct"]),
            "min_dayrate_gbp": sanitize_float(latest_row["min_dayrate_gbp"]),
            "max_dayrate_gbp": sanitize_float(latest_row["max_dayrate_gbp"]),
            "utilization_pct": sanitize_float(util_val),
            "hi_52w": hi_52w,
            "lo_52w": lo_52w,
            "all_time_hi": all_time_hi,
            "all_time_lo": all_time_lo,
            "series": series
        }

    # Sort ledger rows descending by date, then category
    ledger_rows.sort(key=lambda x: (x["date"], x["category_name"]), reverse=True)

    # Ingest reports catalog
    catalog_path = REPORTS_DIR / "seabrokers_catalog.json"
    reports_out = []
    if catalog_path.exists():
        logging.info("Reading %s...", catalog_path)
        try:
            with open(catalog_path, "r", encoding="utf-8") as f:
                cat_raw = json.load(f)
            if isinstance(cat_raw, list):
                for item in cat_raw:
                    reports_out.append({
                        "title": item.get("title", ""),
                        "slug": item.get("slug", ""),
                        "date": item.get("date", ""),
                        "year": item.get("year", 0),
                        "month": item.get("month", 0),
                        "card_url": item.get("card_url", ""),
                        "pdf_url": item.get("pdf_url", ""),
                        "file_size_bytes": item.get("file_size_bytes", 0),
                        "file_size_mb": round(item.get("file_size_bytes", 0) / (1024 * 1024), 2)
                    })
                reports_out.sort(key=lambda x: x["date"], reverse=True)
        except Exception as e:
            logging.warning("Error reading seabrokers catalog: %s", e)

    output_payload = {
        "metadata": {
            "source": "Seabrokers Chartering (Seabreeze Market Intelligence)",
            "updated": latest_date_str,
            "latest_month": latest_month_name,
            "total_monthly_records": len(df),
            "total_reports": len(reports_out),
            "currency": "GBP (£)",
            "coverage": "May 2018 – August 2026 (97 Months Continuous)"
        },
        "kpis": {
            "large_ahts": categories_out.get("large_ahts", {}),
            "med_ahts": categories_out.get("med_ahts", {}),
            "large_psv": categories_out.get("large_psv", {}),
            "med_psv": categories_out.get("med_psv", {})
        },
        "categories": categories_out,
        "recent_ledger": ledger_rows[:120],  # Latest 120 rows for fast pagination/display
        "all_ledger": ledger_rows,
        "reports": reports_out
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, separators=(",", ":"))

    logging.info("Saved %s (%.1f KB).", OUTPUT_FILE, OUTPUT_FILE.stat().st_size / 1024)


if __name__ == "__main__":
    main()
