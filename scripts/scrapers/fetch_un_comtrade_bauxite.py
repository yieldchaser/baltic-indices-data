#!/usr/bin/env python3
"""
UN Comtrade Guinea-to-China Bauxite Trade Scraper
Fetches bilateral monthly bauxite export/import trade volumes (HS 260600) between Guinea (M49: 324) and China (M49: 156).
Uses UN Comtrade v1 Data API with COMTRADE_API_KEY (or public preview fallback).
Direct Portal: https://comtradedeveloper.un.org/ / https://comtradeplus.un.org/
"""

import os
import sys
import logging
from pathlib import Path
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT / "data" / "commodities"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = DATA_DIR / "un_comtrade_guinea_bauxite.csv"
ALT_OUT_FILE = DATA_DIR / "guinea_bauxite_exports.csv"

def fetch_comtrade_bauxite():
    logging.info("Compiling Guinea-to-China Bauxite seaborne export series (HS 260600)...")
    api_key = os.environ.get("COMTRADE_API_KEY", "").strip()

    records = []
    fetched_live = False

    if api_key:
        logging.info("COMTRADE_API_KEY detected. Querying official UN Comtrade v1 Data API...")
        try:
            # Query China imports of HS 260600 from Guinea (reporter: 156 China, partner: 324 Guinea)
            headers = {"Ocp-Apim-Subscription-Key": api_key, "Accept": "application/json"}
            url = "https://comtradeapi.un.org/data/v1/get/C/M/HS?reporterCode=156&partnerCode=324&cmdCode=260600&flowCode=M"
            resp = requests.get(url, headers=headers, timeout=20)
            if resp.status_code == 200:
                data = resp.json().get("data", [])
                if data:
                    logging.info("Fetched %d raw Comtrade trade records.", len(data))
                    for row in data:
                        period = str(row.get("period", ""))
                        if len(period) == 6:
                            dt_str = f"{period[:4]}-{period[4:6]}-01"
                            net_wgt_kg = float(row.get("netWgt") or row.get("qty") or 0)
                            cif_usd = float(row.get("primaryValue") or 0)
                            mt = round(net_wgt_kg / 1000.0, 1)
                            if mt > 0:
                                records.append({
                                    "date": dt_str,
                                    "period": period,
                                    "commodity": "Bauxite",
                                    "hs_code": "260600",
                                    "reporter": "China",
                                    "partner": "Guinea",
                                    "import_volume_mt": mt,
                                    "cif_usd": cif_usd,
                                    "avg_cif_usd_t": round(cif_usd / mt, 2) if mt > 0 else 0
                                })
                    if records:
                        fetched_live = True
        except Exception as e:
            logging.warning("UN Comtrade v1 API query failed (%s); trying public preview endpoint.", e)

    if not fetched_live:
        logging.info("Querying UN Comtrade public preview endpoint for monthly bauxite imports...")
        from datetime import datetime, timezone
        import time

        now = datetime.now(timezone.utc)
        # Query monthly data from 2023 to current month
        periods = []
        for y in range(2023, now.year + 1):
            max_m = now.month if y == now.year else 12
            for m in range(1, max_m + 1):
                periods.append(f"{y}{m:02d}")

        for period in periods:
            url = f"https://comtradeapi.un.org/public/v1/preview/C/M/HS?reporterCode=156&partnerCode=324&cmdCode=260600&flowCode=M&period={period}"
            for attempt in range(4):
                try:
                    resp = requests.get(url, timeout=15)
                    if resp.status_code == 200:
                        data = resp.json().get("data", [])
                        if data:
                            row = data[0]
                            net_wgt = float(row.get("netWgt") or row.get("qty") or 0)
                            val = float(row.get("primaryValue") or 0)
                            mt = round(net_wgt / 1000.0, 1)
                            if mt > 0:
                                records.append({
                                    "date": f"{period[:4]}-{period[4:6]}-01",
                                    "period": period,
                                    "commodity": "Bauxite",
                                    "hs_code": "260600",
                                    "reporter": "China",
                                    "partner": "Guinea",
                                    "import_volume_mt": mt,
                                    "cif_usd": val,
                                    "avg_cif_usd_t": round(val / mt, 2) if mt > 0 else 0.0,
                                })
                        break
                    elif resp.status_code == 429:
                        time.sleep(2.0 * (attempt + 1))
                    else:
                        break
                except Exception:
                    time.sleep(1.0)
            time.sleep(0.8)  # Polite spacing between requests

        if records:
            fetched_live = True

    if not fetched_live:
        logging.error("Could not fetch real UN Comtrade data and refusing to synthesize fake values.")
        if OUT_FILE.exists():
            return pd.read_csv(OUT_FILE)
        return pd.DataFrame()

    df = pd.DataFrame(records).sort_values("date")
    # Discard any previous synthetic records if present
    df.to_csv(OUT_FILE, index=False)
    df.to_csv(ALT_OUT_FILE, index=False)
    logging.info("Wrote %d REAL rows to %s and %s", len(df), OUT_FILE, ALT_OUT_FILE)
    return df

if __name__ == "__main__":
    fetch_comtrade_bauxite()

