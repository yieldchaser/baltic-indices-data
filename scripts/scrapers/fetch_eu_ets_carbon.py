#!/usr/bin/env python3
"""
EU ETS Maritime Carbon & Hi-5 Bunker Economics — REAL DATA ONLY.

Sources:
  1. EUA carbon spot (EUR/t CO2): OilPriceAPI `EU_CARBON_EUR` (requires OILPRICE_API_KEY).
     Free tier: 50 req/day. Each run consumes ONE request and appends ONE daily observation.
  2. Bunker prices (VLSFO/HSFO/MGO): Ship & Bunker global averages via the separate
     expansion collector (expansion_bunker_prices.py) when available.

PROVENANCE NOTE (2026-08-25 audit):
The previous version of this file SYNTHESIZED multi-year daily series with a seeded
random walk (np.random). That fabricated history has been deleted from the platform.
This scraper now writes ONLY observations actually returned by the live API, appended
idempotently to data/derived/eu_ets_carbon_daily.csv.

Derived columns (transparent formulas, computed ONLY on days with real inputs):
    singapore_hi5_spread_usd_mt      = VLSFO - HSFO            (when both present)
    capesize_scrubber_savings_usd_day = 45 MT/day * Hi5        (45 = assumed Cape consumption)
    vlcc_scrubber_savings_usd_day     = 55 MT/day * Hi5
    capesize_eu_ets_surcharge_usd_day = 45 * 3.114 tCO2/day * 50% scope * phase-in(year) * EUR price * EURUSD
    phase-in per EU Directive 2023/959: <=2023 0%, 2024 40%, 2025 70%, >=2026 100%
"""
import json
import logging
import os
import ssl
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT / "data" / "derived"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = DATA_DIR / "eu_ets_carbon_daily.csv"

# EUR->USD conversion used for the derived surcharge; static approximation disclosed here.
EURUSD_FALLBACK = 1.08


def fetch_live_eua() -> tuple[float | None, str | None]:
    """Fetch latest EUA spot from OilPriceAPI. Returns (price_eur, created_at)."""
    api_key = os.environ.get("OILPRICE_API_KEY", "").strip()
    if not api_key:
        logging.warning("OILPRICE_API_KEY not set - cannot fetch real EUA price today.")
        return None, None
    url = "https://api.oilpriceapi.com/v1/prices/latest?by_code=EU_CARBON_EUR"
    headers = {"Authorization": f"Token {api_key}", "Content-Type": "application/json"}
    try:
        req = urllib.request.Request(url, headers=headers)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, timeout=20, context=ctx) as r:
            payload = json.loads(r.read().decode("utf-8", "replace"))
        data = payload.get("data", {})
        price = float(data.get("price") or 0) or None
        return price, data.get("created_at")
    except Exception as e:  # noqa: BLE001
        logging.warning("OilPriceAPI query failed: %s", e)
        return None, None


def load_existing() -> pd.DataFrame:
    if OUT_FILE.exists():
        try:
            df = pd.read_csv(OUT_FILE)
            return df[df.get("eua_carbon_price_eur_tco2", pd.Series(dtype=float)).notna()] \
                if "eua_carbon_price_eur_tco2" in df.columns else df.iloc[0:0]
        except Exception:  # noqa: BLE001
            return pd.DataFrame()
    return pd.DataFrame()


def load_bunker_truth() -> dict:
    """Load real Ship & Bunker snapshots (date -> {port: {grade: price}}) from the
    expansion collector output (expansion_bunker_prices.py)."""
    bf = ROOT / "data" / "bunkers" / "bunker_prices_daily.csv"
    out: dict[str, dict] = {}
    if not bf.exists():
        return out
    try:
        df = pd.read_csv(bf)
        for _, r in df.iterrows():
            d = str(r.get("date", ""))[:10]
            port = str(r.get("port", "")).lower()
            grade = str(r.get("fuel_grade", "")).upper()
            try:
                px = float(r.get("price_usd_mt"))
            except (TypeError, ValueError):
                continue
            out.setdefault(d, {}).setdefault(port, {})[grade] = px
    except Exception as e:  # noqa: BLE001
        logging.warning("Could not read bunker prices: %s", e)
    return out


def derive_row_economics(row: dict, bunkers: dict) -> dict:
    """Fill transparent derived columns from REAL inputs only (never invents prices)."""
    d = str(row.get("date", ""))[:10]
    ports = bunkers.get(d, {})
    sg = ports.get("singapore", {})
    nl = ports.get("rotterdam", {})
    us = ports.get("houston", {})
    vlsfo = sg.get("VLSFO")
    hsfo = sg.get("IFO380")
    if vlsfo and hsfo:
        hi5 = round(vlsfo - hsfo, 2)
        row["singapore_vlsfo_usd_mt"] = vlsfo
        row["singapore_hsfo_usd_mt"] = hsfo
        row["singapore_hi5_spread_usd_mt"] = hi5
        row["capesize_scrubber_savings_usd_day"] = round(45.0 * hi5, 2)
        row["vlcc_scrubber_savings_usd_day"] = round(55.0 * hi5, 2)
    if nl.get("VLSFO") and nl.get("IFO380"):
        row["rotterdam_hi5_spread_usd_mt"] = round(nl["VLSFO"] - nl["IFO380"], 2)
    if us.get("VLSFO") and us.get("IFO380"):
        row["houston_hi5_spread_usd_mt"] = round(us["VLSFO"] - us["IFO380"], 2)

    year = int(str(row.get("date"))[:4])
    phase_in = 0.0 if year <= 2023 else (0.40 if year == 2024 else (0.70 if year == 2025 else 1.00))
    eua = row.get("eua_carbon_price_eur_tco2")
    if eua not in (None, ""):
        row["capesize_eu_ets_surcharge_usd_day"] = round(
            45.0 * 3.114 * (0.50 * phase_in) * float(eua) * EURUSD_FALLBACK, 2)
    return row


def main() -> pd.DataFrame:
    price, created_at = fetch_live_eua()
    if price is None:
        logging.warning(
            "No live EUA observation available (missing key or upstream failure). "
            "Per data-provenance policy NO synthetic values are written. Keeping existing data as-is."
        )
        return load_existing()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    existing = load_existing()

    if len(existing) and today in set(existing["date"].astype(str)):
        logging.info("%s already recorded - idempotent no-op.", today)
        return existing

    row = {
        "date": today,
        "eua_carbon_price_eur_tco2": round(price, 2),
        "source_created_at": created_at or "",
        "singapore_vlsfo_usd_mt": "",
        "singapore_hsfo_usd_mt": "",
        "singapore_hi5_spread_usd_mt": "",
        "rotterdam_hi5_spread_usd_mt": "",
        "houston_hi5_spread_usd_mt": "",
        "capesize_scrubber_savings_usd_day": "",
        "vlcc_scrubber_savings_usd_day": "",
        "capesize_eu_ets_surcharge_usd_day": "",
    }
    row = derive_row_economics(row, load_bunker_truth())

    combined = pd.concat([existing, pd.DataFrame([row])], ignore_index=True)
    combined.to_csv(OUT_FILE, index=False)
    logging.info("Appended %s: EUA EUR %.2f/t (real observation). Rows: %d",
                 today, price, len(combined))
    return combined


if __name__ == "__main__":
    main()
