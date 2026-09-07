"""
fetch_fearnpulse_full.py
Exhaustive historical pull of ALL ~360 rate series from Fearnleys'
Hasura GraphQL backend (pbrokerapp.hasura.app).

Workflow:
  1. Dynamically queries the rate_meta catalog (ListAllSeries) to get
     every unique (rate_type, rate_subtype, route, rate_unit) combination.
  2. Loops through all series with polite pacing (0.35s) and exponential backoff.
  3. Pulls full historical time series back to 1970-01-01.
  4. Streams and saves all observations to fearnpulse_rates_full.csv.
"""

import csv
import os
import re
import sys
import time
from datetime import date

import requests

ENDPOINT = "https://pbrokerapp.hasura.app/v1/graphql"

CATALOG_QUERY = """
query ListAllSeries {
  rate_meta {
    info {
      rate_type
      rate_subtype
      route
      __typename
    }
    rate_unit
    __typename
  }
}
"""

RATES_QUERY = """
query GetRatesByMetaForRange($dateFrom: date!, $dateTo: date!, $rateType: String!, $rateSubtype: String!, $route: [String!]!, $rateUnit: String = "usd") {
  rate_meta(
    where: {info: {rate_type: {_eq: $rateType}, rate_subtype: {_eq: $rateSubtype}, route: {_in: $route}}, rate_unit: {_eq: $rateUnit}}
  ) {
    rates(where: {date: {_gte: $dateFrom, _lte: $dateTo}}, order_by: {date: desc}) {
      date
      rate
      __typename
    }
    __typename
  }
}
"""

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) FearnpulseHarvester/1.0",
}


def slugify(text: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", text).strip("_")
    return s.upper()


def get_catalog():
    payload = {
        "operationName": "ListAllSeries",
        "query": CATALOG_QUERY,
        "variables": {},
    }
    resp = requests.post(ENDPOINT, json=payload, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        raise RuntimeError(f"Catalog query failed: {body['errors']}")

    raw_meta = body.get("data", {}).get("rate_meta", [])
    seen = set()
    catalog = []
    for m in raw_meta:
        info = m.get("info") or {}
        rt = info.get("rate_type")
        rst = info.get("rate_subtype")
        route = info.get("route")
        unit = m.get("rate_unit")
        if not (rt and rst and route and unit):
            continue
        key = (rt, rst, route, unit)
        if key not in seen:
            seen.add(key)
            label = f"{slugify(rt)}_{slugify(rst)}_{slugify(route)}"
            catalog.append({
                "label": label,
                "rate_type": rt,
                "rate_subtype": rst,
                "route": route,
                "rate_unit": unit,
            })

    # Sort for deterministic processing
    catalog.sort(key=lambda x: (x["rate_type"], x["rate_subtype"], x["route"], x["rate_unit"]))
    return catalog


def fetch_series_with_retry(item, date_from="1970-01-01", date_to=None, max_retries=4):
    date_to = date_to or date.today().isoformat()
    payload = {
        "operationName": "GetRatesByMetaForRange",
        "query": RATES_QUERY,
        "variables": {
            "dateFrom": date_from,
            "dateTo": date_to,
            "rateType": item["rate_type"],
            "rateSubtype": item["rate_subtype"],
            "route": [item["route"]],
            "rateUnit": item["rate_unit"],
        },
    }

    for attempt in range(max_retries):
        try:
            resp = requests.post(ENDPOINT, json=payload, headers=HEADERS, timeout=35)
            if resp.status_code in (429, 500, 502, 503, 504):
                backoff = 2.0 * (2 ** attempt)
                time.sleep(backoff)
                continue
            resp.raise_for_status()
            body = resp.json()
            if "errors" in body:
                return []
            rate_meta = body.get("data", {}).get("rate_meta", [])
            rows = []
            for meta in rate_meta:
                for r in meta.get("rates", []):
                    rows.append({
                        "label": item["label"],
                        "rate_type": item["rate_type"],
                        "rate_subtype": item["rate_subtype"],
                        "route": item["route"],
                        "unit": item["rate_unit"],
                        "date": r["date"],
                        "rate": r["rate"],
                    })
            return rows
        except (requests.RequestException, Exception) as e:
            if attempt == max_retries - 1:
                print(f"    [WARN] Exceeded retries for {item['label']}: {e}", flush=True)
                return []
            time.sleep(2.0 * (attempt + 1))
    return []


def main():
    print("========================================================", flush=True)
    print("  Fearnpulse Complete Catalog Harvester (360 Series)   ", flush=True)
    print("========================================================", flush=True)

    catalog = get_catalog()
    total_series = len(catalog)
    print(f"Discovered {total_series} unique series across Fearnleys backend.\n", flush=True)

    out_csv = "fearnpulse_rates_full.csv"
    fieldnames = ["label", "rate_type", "rate_subtype", "route", "unit", "date", "rate"]

    all_rows = []
    populated_count = 0
    empty_count = 0
    start_time = time.time()

    for idx, item in enumerate(catalog, start=1):
        rows = fetch_series_with_retry(item, date_from="1970-01-01")
        if rows:
            all_rows.extend(rows)
            populated_count += 1
            earliest = rows[-1]["date"]
            status_tag = f"{len(rows):4d} rows ({earliest})"
        else:
            empty_count += 1
            status_tag = "   0 rows [EMPTY]"

        elapsed = time.time() - start_time
        avg_per_item = elapsed / idx
        remaining_items = total_series - idx
        eta_sec = remaining_items * avg_per_item
        eta_str = f"{int(eta_sec // 60)}m {int(eta_sec % 60):02d}s" if eta_sec >= 60 else f"{int(eta_sec)}s"

        pct = (idx / total_series) * 100.0
        print(f"[{idx:3d}/{total_series}] ({pct:5.1f}%) | {item['rate_type'][:10]:10} | {item['rate_subtype'][:12]:12} | {item['route'][:35]:35} -> {status_tag} | Cum: {len(all_rows):6d} | ETA: {eta_str}", flush=True)

        # Polite waitout
        time.sleep(0.35)

    print(f"\nWriting {len(all_rows)} rows to {out_csv}...", flush=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Wrote {len(all_rows)} rows to {out_csv}", flush=True)

    # Also mirror into data/derived/
    derived_dir = os.path.join("..", "data", "derived")
    if os.path.exists(derived_dir):
        derived_path = os.path.join(derived_dir, "fearnpulse_rates_full.csv")
        with open(derived_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"Mirrored to {derived_path}", flush=True)

    total_elapsed = time.time() - start_time
    print(f"\n========================================================", flush=True)
    print(f"  Harvesting Summary", flush=True)
    print(f"========================================================", flush=True)
    print(f"Total series queried : {total_series}", flush=True)
    print(f"Series with data     : {populated_count}", flush=True)
    print(f"Series empty (0 rows): {empty_count}", flush=True)
    print(f"Total records saved  : {len(all_rows)} rows", flush=True)
    print(f"Total elapsed time   : {total_elapsed / 60:.1f} minutes", flush=True)
    print("========================================================\n", flush=True)


if __name__ == "__main__":
    main()
