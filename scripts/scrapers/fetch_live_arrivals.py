#!/usr/bin/env python3
"""
IMF PortWatch Live Vessel Arrival & Port Influx Scraper
======================================================
Captures real-time and historical daily vessel calls across 40 strategic global hubs
spanning four core maritime asset classes: Dry Bulk, Tankers, LPG, and LNG.

Source: IMF PortWatch ArcGIS REST API Gateway
  https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Ports_Data/FeatureServer/0/query

Outputs:
  data/congestion/port_calls_daily.csv - Comprehensive single-source daily observations (2019-2026)
  data/congestion/port_calls_daily_v2.csv - Mirror synchronized file
"""

import argparse
import concurrent.futures
import json
import logging
import ssl
import time
import urllib.parse
import urllib.request
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT / "data" / "congestion"
DATA_DIR.mkdir(parents=True, exist_ok=True)

LAYER_URL = (
    "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/"
    "arcgis/rest/services/Daily_Ports_Data/FeatureServer/0/query"
)

# 40-Port Universe Across 4 Asset Spheres with Metadata
TARGET_HUBS = {
    # -------------------------------------------------------------
    # 1. DRY BULK HUBS (Iron Ore, Bauxite, Coal, Grain, Minor Bulks)
    # -------------------------------------------------------------
    "port955": {
        "locode": "AUPHE",
        "portname": "Port Hedland",
        "country": "Australia",
        "iso3": "AUS",
        "primary_asset": "Dry Bulk",
        "segments": ["Dry Bulk"],
        "call_col": "portcalls_dry_bulk",
    },
    "port1328": {
        "locode": "BRTUB",
        "portname": "Tubarao",
        "country": "Brazil",
        "iso3": "BRA",
        "primary_asset": "Dry Bulk",
        "segments": ["Dry Bulk"],
        "call_col": "portcalls_dry_bulk",
    },
    "port506": {
        "locode": "BRPMD",
        "portname": "Itaqui / Ponta da Madeira",
        "country": "Brazil",
        "iso3": "BRA",
        "primary_asset": "Dry Bulk",
        "segments": ["Dry Bulk"],
        "call_col": "portcalls_dry_bulk",
    },
    "port536": {
        "locode": "GNKMR",
        "portname": "Kamsar",
        "country": "Guinea",
        "iso3": "GIN",
        "primary_asset": "Dry Bulk",
        "segments": ["Dry Bulk"],
        "call_col": "portcalls_dry_bulk",
    },
    "port276": {
        "locode": "AUDAM",
        "portname": "Dampier",
        "country": "Australia",
        "iso3": "AUS",
        "primary_asset": "Dry Bulk",
        "segments": ["Dry Bulk", "LNG"],
        "call_col": "portcalls_dry_bulk",
    },
    "port458": {
        "locode": "AUHPT",
        "portname": "Hay Point",
        "country": "Australia",
        "iso3": "AUS",
        "primary_asset": "Dry Bulk",
        "segments": ["Dry Bulk"],
        "call_col": "portcalls_dry_bulk",
    },
    "port816": {
        "locode": "AUNCL",
        "portname": "Newcastle",
        "country": "Australia",
        "iso3": "AUS",
        "primary_asset": "Dry Bulk",
        "segments": ["Dry Bulk"],
        "call_col": "portcalls_dry_bulk",
    },
    "port398": {
        "locode": "AUGLT",
        "portname": "Gladstone",
        "country": "Australia",
        "iso3": "AUS",
        "primary_asset": "Dry Bulk",
        "segments": ["Dry Bulk", "LNG"],
        "call_col": "portcalls_dry_bulk",
    },
    "port1133": {
        "locode": "ZASDB",
        "portname": "Saldanha Bay",
        "country": "South Africa",
        "iso3": "ZAF",
        "primary_asset": "Dry Bulk",
        "segments": ["Dry Bulk"],
        "call_col": "portcalls_dry_bulk",
    },
    "port1099": {
        "locode": "ZARCB",
        "portname": "Richards Bay",
        "country": "South Africa",
        "iso3": "ZAF",
        "primary_asset": "Dry Bulk",
        "segments": ["Dry Bulk"],
        "call_col": "portcalls_dry_bulk",
    },
    "port1160": {
        "locode": "BRSSZ",
        "portname": "Santos",
        "country": "Brazil",
        "iso3": "BRA",
        "primary_asset": "Dry Bulk",
        "segments": ["Dry Bulk"],
        "call_col": "portcalls_dry_bulk",
    },
    "port1069": {
        "locode": "CNQDG",
        "portname": "Qingdao Port",
        "country": "China",
        "iso3": "CHN",
        "primary_asset": "Dry Bulk",
        "segments": ["Dry Bulk", "Tankers"],
        "call_col": "portcalls_dry_bulk",
    },
    "port824": {
        "locode": "CNNGB",
        "portname": "Ningbo",
        "country": "China",
        "iso3": "CHN",
        "primary_asset": "Dry Bulk",
        "segments": ["Dry Bulk", "LPG"],
        "call_col": "portcalls_dry_bulk",
    },
    "port1105": {
        "locode": "CNRZH",
        "portname": "Rizhao",
        "country": "China",
        "iso3": "CHN",
        "primary_asset": "Dry Bulk",
        "segments": ["Dry Bulk"],
        "call_col": "portcalls_dry_bulk",
    },
    "port1266": {
        "locode": "CNJGT",
        "portname": "Tangshan (Jingtang)",
        "country": "China",
        "iso3": "CHN",
        "primary_asset": "Dry Bulk",
        "segments": ["Dry Bulk"],
        "call_col": "portcalls_dry_bulk",
    },
    "port1114": {
        "locode": "NLRTM",
        "portname": "Rotterdam",
        "country": "The Netherlands",
        "iso3": "NLD",
        "primary_asset": "Tankers",
        "segments": ["Dry Bulk", "Tankers"],
        "call_col": "portcalls_tanker",
    },
    # -------------------------------------------------------------
    # 2. TANKERS (Crude & Product Terminals, Refining & Trade Hubs)
    # -------------------------------------------------------------
    "port1091": {
        "locode": "SARRT",
        "portname": "Ras Tanura",
        "country": "Saudi Arabia",
        "iso3": "SAU",
        "primary_asset": "Tankers",
        "segments": ["Tankers"],
        "call_col": "portcalls_tanker",
    },
    "port155": {
        "locode": "NGBON",
        "portname": "Bonny",
        "country": "Nigeria",
        "iso3": "NGA",
        "primary_asset": "Tankers",
        "segments": ["Tankers", "LNG"],
        "call_col": "portcalls_tanker",
    },
    "port481": {
        "locode": "USHOU",
        "portname": "Houston (US-TX)",
        "country": "United States",
        "iso3": "USA",
        "primary_asset": "Tankers",
        "segments": ["Tankers", "LPG", "Dry Bulk"],
        "call_col": "portcalls_tanker",
    },
    "port264": {
        "locode": "USCRP",
        "portname": "Corpus Christi",
        "country": "United States",
        "iso3": "USA",
        "primary_asset": "Tankers",
        "segments": ["Tankers", "LNG"],
        "call_col": "portcalls_tanker",
    },
    "port1201": {
        "locode": "SGSIN",
        "portname": "Singapore",
        "country": "Singapore",
        "iso3": "SGP",
        "primary_asset": "Tankers",
        "segments": ["Tankers"],
        "call_col": "portcalls_tanker",
    },
    "port362": {
        "locode": "AEFJR",
        "portname": "Fujairah",
        "country": "United Arab Emirates",
        "iso3": "ARE",
        "primary_asset": "Tankers",
        "segments": ["Tankers"],
        "call_col": "portcalls_tanker",
    },
    "port1020": {
        "locode": "RUPRI",
        "portname": "Primorsk",
        "country": "Russian Federation",
        "iso3": "RUS",
        "primary_asset": "Tankers",
        "segments": ["Tankers"],
        "call_col": "portcalls_tanker",
    },
    "port570": {
        "locode": "SAYNB",
        "portname": "Yanbu (King Fahd Port)",
        "country": "Saudi Arabia",
        "iso3": "SAU",
        "primary_asset": "Tankers",
        "segments": ["Tankers"],
        "call_col": "portcalls_tanker",
    },
    "port1199": {
        "locode": "INSIK",
        "portname": "Sikka / Jamnagar",
        "country": "India",
        "iso3": "IND",
        "primary_asset": "Tankers",
        "segments": ["Tankers"],
        "call_col": "portcalls_tanker",
    },
    # -------------------------------------------------------------
    # 3. LPG HUBS (VLGC Loaders & Major Discharge Hubs)
    # -------------------------------------------------------------
    "port933": {
        "locode": "USPOA",
        "portname": "Port Arthur / Nederland",
        "country": "United States",
        "iso3": "USA",
        "primary_asset": "LPG",
        "segments": ["LPG", "Tankers"],
        "call_col": "portcalls_tanker",
    },
    "port134": {
        "locode": "USBPT",
        "portname": "Beaumont",
        "country": "United States",
        "iso3": "USA",
        "primary_asset": "LPG",
        "segments": ["LPG", "Tankers"],
        "call_col": "portcalls_tanker",
    },
    "port1090": {
        "locode": "QARLF",
        "portname": "Ras Laffan",
        "country": "Qatar",
        "iso3": "QAT",
        "primary_asset": "LNG",
        "segments": ["LNG", "LPG"],
        "call_col": "portcalls_tanker",
    },
    "port526": {
        "locode": "SAJUA",
        "portname": "Juaymah",
        "country": "Saudi Arabia",
        "iso3": "SAU",
        "primary_asset": "LPG",
        "segments": ["LPG"],
        "call_col": "portcalls_tanker",
    },
    "port743": {
        "locode": "KWMFA",
        "portname": "Mina Al Ahmadi",
        "country": "Kuwait",
        "iso3": "KWT",
        "primary_asset": "LPG",
        "segments": ["LPG", "Tankers"],
        "call_col": "portcalls_tanker",
    },
    "port239": {
        "locode": "JPCHB",
        "portname": "Chiba",
        "country": "Japan",
        "iso3": "JPN",
        "primary_asset": "LPG",
        "segments": ["LPG", "Tankers"],
        "call_col": "portcalls_tanker",
    },
    "port1417": {
        "locode": "JPYOK",
        "portname": "Yokohama",
        "country": "Japan",
        "iso3": "JPN",
        "primary_asset": "LPG",
        "segments": ["LPG", "Tankers"],
        "call_col": "portcalls_tanker",
    },
    "port1426": {
        "locode": "CNZHA",
        "portname": "Zhanjiang",
        "country": "China",
        "iso3": "CHN",
        "primary_asset": "LPG",
        "segments": ["LPG", "Dry Bulk"],
        "call_col": "portcalls_tanker",
    },
    "port1338": {
        "locode": "KRUSN",
        "portname": "Ulsan",
        "country": "Korea",
        "iso3": "KOR",
        "primary_asset": "LPG",
        "segments": ["LPG", "Tankers"],
        "call_col": "portcalls_tanker",
    },
    # -------------------------------------------------------------
    # 4. LNG HUBS (Major Liquefaction Exporters & Influx Terminals)
    # -------------------------------------------------------------
    "port2388": {
        "locode": "USSPG",
        "portname": "Sabine Pass",
        "country": "United States",
        "iso3": "USA",
        "primary_asset": "LNG",
        "segments": ["LNG"],
        "call_col": "portcalls_tanker",
    },
    "port629": {
        "locode": "USCMR",
        "portname": "Lake Charles / Cameron",
        "country": "United States",
        "iso3": "USA",
        "primary_asset": "LNG",
        "segments": ["LNG", "Tankers"],
        "call_col": "portcalls_tanker",
    },
    "port2379": {
        "locode": "USCVP",
        "portname": "Dominion Cove Point",
        "country": "United States",
        "iso3": "USA",
        "primary_asset": "LNG",
        "segments": ["LNG"],
        "call_col": "portcalls_tanker",
    },
    "port280": {
        "locode": "AUDRW",
        "portname": "Darwin",
        "country": "Australia",
        "iso3": "AUS",
        "primary_asset": "LNG",
        "segments": ["LNG"],
        "call_col": "portcalls_tanker",
    },
    "port149": {
        "locode": "MYBTU",
        "portname": "Bintulu",
        "country": "Malaysia",
        "iso3": "MYS",
        "primary_asset": "LNG",
        "segments": ["LNG"],
        "call_col": "portcalls_tanker",
    },
    "port70": {
        "locode": "DZAZW",
        "portname": "Arzew",
        "country": "Algeria",
        "iso3": "DZA",
        "primary_asset": "LNG",
        "segments": ["LNG", "Tankers"],
        "call_col": "portcalls_tanker",
    },
    "port449": {
        "locode": "NOHFT",
        "portname": "Hammerfest",
        "country": "Norway",
        "iso3": "NOR",
        "primary_asset": "LNG",
        "segments": ["LNG"],
        "call_col": "portcalls_tanker",
    },
}

FIELDS = (
    "date,year,month,day,portid,portname,country,ISO3,portcalls_container,"
    "portcalls_dry_bulk,portcalls_general_cargo,portcalls_roro,portcalls_tanker,"
    "portcalls_cargo,portcalls,import_container,import_dry_bulk,import_general_cargo,"
    "import_roro,import_tanker,import_cargo,import,export_container,export_dry_bulk,"
    "export_general_cargo,export_roro,export_tanker,export_cargo,export,ObjectId"
)

_CTX = ssl.create_default_context()
_CTX.check_hostname = False
_CTX.verify_mode = ssl.CERT_NONE


def _http_json(url: str, retries: int = 3) -> dict:
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "shipping-terminal/2.0"})
            with urllib.request.urlopen(req, timeout=45, context=_CTX) as r:
                return json.loads(r.read().decode("utf-8", "replace"))
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    logging.warning("HTTP query failed for %s: %s", url[:120], last_err)
    return {}


def fetch_port_history(port_id: str, limit_total: int | None = None) -> list[dict]:
    """Fetch historical and live records for a given portid."""
    rows: list[dict] = []
    offset = 0
    page_size = 1000
    while True:
        req_count = page_size if limit_total is None else min(page_size, limit_total - len(rows))
        if req_count <= 0:
            break
        params = urllib.parse.urlencode({
            "f": "json",
            "where": f"portid='{port_id}'",
            "outFields": FIELDS,
            "returnGeometry": "false",
            "orderByFields": "date ASC",
            "resultRecordCount": str(req_count),
            "resultOffset": str(offset),
        })
        url = f"{LAYER_URL}?{params}"
        d = _http_json(url)
        feats = d.get("features") or []
        if not feats:
            break
        rows.extend([f["attributes"] for f in feats])
        got = len(feats)
        offset += got
        if limit_total is not None and len(rows) >= limit_total:
            break
        if not d.get("exceededTransferLimit") and got < page_size:
            break
    return rows


def scrape_all_target_hubs(workers: int = 6) -> pd.DataFrame:
    """Fetch daily observations for all 40 target hubs in parallel."""
    all_rows = []
    logging.info("Starting ingest across %d target maritime hubs...", len(TARGET_HUBS))

    def _worker(pid):
        meta = TARGET_HUBS[pid]
        records = fetch_port_history(pid)
        logging.info("Fetched %s (%s | %s): %d records", meta["portname"], pid, meta["locode"], len(records))
        return records

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(_worker, pid): pid for pid in TARGET_HUBS}
        for future in concurrent.futures.as_completed(future_map):
            try:
                records = future.result()
                all_rows.extend(records)
            except Exception as e:
                pid = future_map[future]
                logging.error("Failed fetching %s: %s", pid, e)

    df = pd.DataFrame(all_rows)
    if df.empty:
        raise RuntimeError("No records fetched from PortWatch ArcGIS API!")
    return df


def upsert_port_calls(fresh_df: pd.DataFrame) -> pd.DataFrame:
    """Merge fresh observations into existing local port_calls_daily.csv."""
    master_path = DATA_DIR / "port_calls_daily.csv"
    if master_path.exists():
        logging.info("Reading existing master: %s", master_path)
        existing = pd.read_csv(master_path, dtype={"portid": str})
        # Concatenate and deduplicate by (portid, date)
        combined = pd.concat([existing, fresh_df], ignore_index=True)
    else:
        combined = fresh_df

    combined = combined.drop_duplicates(subset=["portid", "date"], keep="last")
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined = combined.dropna(subset=["date"]).sort_values(["portid", "date"])
    combined["date"] = combined["date"].dt.strftime("%Y-%m-%d")

    # Save to port_calls_daily.csv
    combined.to_csv(master_path, index=False)
    logging.info("Wrote %d total rows to %s (dates: %s to %s)",
                 len(combined), master_path, combined["date"].min(), combined["date"].max())

    # Also sync to port_calls_daily_v2.csv with standard format
    v2_path = DATA_DIR / "port_calls_daily_v2.csv"
    v2_df = combined[combined["portid"].isin(TARGET_HUBS)].copy()
    v2_df["hub_code"] = v2_df["portid"]
    rename_cols = {
        "portcalls": "daily_port_calls_total",
        "portcalls_dry_bulk": "daily_port_calls_dry_bulk",
        "portcalls_tanker": "daily_port_calls_tanker",
        "portcalls_container": "daily_port_calls_container",
    }
    v2_df = v2_df.rename(columns=rename_cols)
    for c in ("import_dry_bulk", "export_dry_bulk", "import_tanker", "export_tanker"):
        if c in v2_df.columns:
            v2_df[c + "_kt"] = (pd.to_numeric(v2_df[c], errors="coerce") / 1000.0).round(2)

    v2_cols = ["date", "portid", "portname", "country", "hub_code",
               "daily_port_calls_total", "daily_port_calls_dry_bulk",
               "daily_port_calls_tanker", "daily_port_calls_container",
               "import_dry_bulk_kt", "export_dry_bulk_kt",
               "import_tanker_kt", "export_tanker_kt"]
    v2_cols = [c for c in v2_cols if c in v2_df.columns]
    v2_df[v2_cols].to_csv(v2_path, index=False)
    logging.info("Synchronized %d rows to %s", len(v2_df), v2_path)

    return combined


def main():
    parser = argparse.ArgumentParser(description="Fetch live and historical vessel calls from IMF PortWatch.")
    parser.add_argument("--dry-run", action="store_true", help="Test fetch for sample ports only.")
    args = parser.parse_args()

    if args.dry_run:
        logging.info("DRY RUN MODE: Testing single-port fetch (Port Hedland port955)...")
        sample = fetch_port_history("port955", limit_total=20)
        print(f"Sample records fetched: {len(sample)}")
        if sample:
            print("First record:", sample[0])
            print("Latest record:", sample[-1])
        return

    fresh = scrape_all_target_hubs(workers=6)
    upsert_port_calls(fresh)
    logging.info("Ingestion completed successfully.")


if __name__ == "__main__":
    main()
