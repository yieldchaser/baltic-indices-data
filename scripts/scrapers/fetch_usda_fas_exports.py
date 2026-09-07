"""
USDA Foreign Agricultural Service (FAS) Export Sales Reporting Scraper.
Fetches weekly export sales, outstanding commitments, and accumulated exports
for major bulk dry commodities (Corn, Soybeans, Wheat) to key maritime destinations
(China, Japan, Mexico, EU, Egypt) via USDA AgTransport Socrata Open API.
"""

import os
import io
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
COMMODITIES_DIR = REPO_ROOT / "data" / "commodities"
DERIVED_DIR = REPO_ROOT / "data" / "derived"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126",
    "Accept": "text/csv,application/csv",
}

# 885i-uek7: Total Outstanding Export Sales by Week, Commodity, and Country
# pamd-wd5x: Year-to-Date Grain and Soybean Inspections by Top 20 Destinations
# Build C: fetch RECENT rows first. Socrata default $order=:id returns the
# OLDEST 10k rows; ordering by date DESC + offset pagination returns the most
# recent ~60k rows instead. MAX_ROWS caps total per dataset (never truncate
# history: new pages are upserted onto the existing file, deduped + sorted).
DATASETS = {
    "usda_fas_outstanding_export_sales.csv": {
        "id": "885i-uek7",
        "name": "USDA FAS Weekly Outstanding Export Sales by Commodity & Country",
        "dir": COMMODITIES_DIR,
        "limit": 10000,
        "order_by": "date DESC",
        "max_rows": 60000,
    },
    "usda_ytd_grain_inspections_top20.csv": {
        "id": "5sxb-qe7q",
        "name": "USDA Grain & Soybean Inspections by Top 20 Destinations",
        "dir": COMMODITIES_DIR,
        "limit": 10000,
        "order_by": "date DESC",
        "max_rows": 60000,
    },
}

def _fetch_socrata_page(dataset_id, limit, offset, order_by):
    """Fetch one Socrata CSV page with 429/5xx backoff. Returns DataFrame."""
    params = {
        "$limit": str(limit),
        "$offset": str(offset),
        "$order": order_by,
    }
    url = f"https://agtransport.usda.gov/resource/{dataset_id}.csv?{urllib.parse.urlencode(params)}"
    last_err = None
    for attempt in range(1, 4):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=60) as resp:
                content = resp.read()
            return pd.read_csv(io.BytesIO(content))
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            # Unknown order column (400) -> caller falls back to :id ordering.
            if e.code == 400:
                raise
            if e.code == 429 or e.code >= 500:
                time.sleep(min(2 ** attempt * 5, 30))
                continue
            raise
        except Exception as e:  # noqa: BLE001
            last_err = str(e)
            time.sleep(min(2 ** attempt * 5, 30))
    raise RuntimeError(f"Socrata {dataset_id} offset {offset} failed ({last_err})")


def fetch_fas_dataset(filename, info):
    dataset_id = info["id"]
    name = info["name"]
    target_dir = info["dir"]
    limit = info.get("limit", 10000)
    order_by = info.get("order_by", "date DESC")
    max_rows = info.get("max_rows", 60000)

    print(f"[+] Fetching {name} ({dataset_id}) [order={order_by}, page={limit}, cap={max_rows}]...")

    # $order=date DESC needs a real date column; fall back to :id only if the
    # dataset rejects the date ordering (HTTP 400), never silently.
    effective_order = order_by
    try:
        probe = _fetch_socrata_page(dataset_id, min(limit, 100), 0, effective_order)
    except Exception as e:
        if "400" in str(e) or "HTTP 400" in str(e):
            print(f"    [warn] order {effective_order!r} rejected; falling back to :id (oldest-first).")
            effective_order = ":id"
            probe = _fetch_socrata_page(dataset_id, min(limit, 100), 0, effective_order)
        else:
            raise
    if probe.empty:
        print("    [warn] probe page empty; keeping existing file as-is.")
        return 0

    pages = []
    # Paginate offsets 0,10000,20000,... up to max_rows to get recent rows,
    # not just the oldest 10k.
    offset = 0
    total = 0
    while total < max_rows:
        df_page = _fetch_socrata_page(dataset_id, limit, offset, effective_order)
        if df_page.empty:
            break
        pages.append(df_page)
        total += len(df_page)
        print(f"    [page] offset={offset} rows={len(df_page)} (total {total})")
        if len(df_page) < limit:
            break  # last page
        offset += limit
        time.sleep(1)  # polite pacing between pages
    if not pages:
        print("    [warn] no pages returned; keeping existing file as-is.")
        return 0

    df = pd.concat(pages, ignore_index=True)
    # Never delete rows: upsert onto existing file, dedup + sort only.
    # Header is preserved exactly (existing column order wins).
    target_dir.mkdir(parents=True, exist_ok=True)
    out_path = target_dir / filename
    if out_path.exists():
        try:
            df_old = pd.read_csv(out_path)
            old_cols = list(df_old.columns)
            # Align new pages to existing header (preserve header, no new cols
            # silently changing schema; extra cols appended only if truly new).
            for c in old_cols:
                if c not in df.columns:
                    df[c] = pd.NA
            df = pd.concat([df_old, df], ignore_index=True)
            df = df.drop_duplicates(keep="last")
            df = df[[c for c in old_cols] + [c for c in df.columns if c not in old_cols]]
        except Exception as e:  # noqa: BLE001
            print(f"    [warn] could not merge with existing {filename} ({e}); writing fresh fetch.")
    # Sort recent-first when a date column exists, else keep fetch order.
    date_col = next((c for c in ("date", "week_ending", "week", "year") if c in df.columns), None)
    if date_col:
        df["_sort_tmp"] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.sort_values(["_sort_tmp", date_col], ascending=[False, False], na_position="last").drop(columns=["_sort_tmp"]).reset_index(drop=True)
    df.to_csv(out_path, index=False)
    print(f"    [OK] Saved {len(df):,} rows and {len(df.columns)} columns to {out_path.name}")
    return len(df)

def main():
    print("=" * 80)
    print("  USDA FAS AGRICULTURAL EXPORT SALES INGESTION ENGINE")
    print("=" * 80)
    
    results = {}
    for filename, info in DATASETS.items():
        try:
            count = fetch_fas_dataset(filename, info)
            results[info["name"]] = count
        except Exception as e:
            print(f"    [!] Error fetching {info['name']}: {e}")
            results[info["name"]] = False
            
    print("\n" + "=" * 80)
    print("  FAS INGESTION RESULTS:")
    for name, count in results.items():
        status = f"{count:,} rows" if count else "FAILED"
        print(f"  • {name:60s} -> {status}")
    print("=" * 80)

if __name__ == "__main__":
    main()
