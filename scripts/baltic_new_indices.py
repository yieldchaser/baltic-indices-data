"""
Baltic Exchange — New Indices Scraper
======================================
Fetches BLNG, BLPG, FBX, BAI00 from the Baltic Exchange public ticker API
  https://blacksun-api.balticexchange.com/api/ticker
and appends them to historical CSVs.

The API returns current + previous values with exact ISO timestamps, so no
anchor-CSV date matching is needed — we use the indexDate directly.

Output schema (matches existing repo CSVs):
  Date (DD-MM-YYYY), Index, % Change

Usage:
  pip install requests
  python baltic_new_indices.py --repo /path/to/Shipping
"""

import csv
import argparse
from datetime import datetime
from pathlib import Path

import requests

API_URL = "https://blacksun-api.balticexchange.com/api/ticker"

# Indices to record, keyed by indexDataSetName from the API
NEW_INDICES = {
    "BLNG":  "blng_historical.csv",
    "BLPG":  "blpg_historical.csv",
    "FBX":   "fbx_historical.csv",
    "BAI00": "bai_historical.csv",
}

# ── API fetch ────────────────────────────────────────────────────────────────

def fetch_ticker() -> dict:
    """
    Call the Baltic Exchange ticker API.
    Returns {indexDataSetName: (value, date_str_DD-MM-YYYY)}.
    """
    resp = requests.get(API_URL, timeout=30, headers={"Accept": "application/json"})
    resp.raise_for_status()
    data = resp.json()

    result = {}
    for item in data:
        code    = (item.get("indexDataSetName") or "").strip()
        current = item.get("current") or {}
        value   = current.get("value")
        raw_dt  = current.get("indexDate")
        if code and value is not None and raw_dt:
            date_str = datetime.fromisoformat(raw_dt).strftime("%d-%m-%Y")
            result[code] = (float(value), date_str)

    return result

# ── CSV append ───────────────────────────────────────────────────────────────

def load_existing_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def compute_change(new_val: float, prev_val: float | None) -> str:
    if prev_val is None or prev_val == 0:
        return ""
    pct = (new_val - prev_val) / prev_val * 100
    return f"{pct:.2f}"

def append_to_csv(path: Path, date_str: str, code: str, value: float):
    """Append one row to a historical CSV. Skip if date already exists."""
    rows = load_existing_csv(path)

    existing_dates = {r.get("Date", r.get("date", "")) for r in rows}
    if date_str in existing_dates:
        print(f"[--] {code}: {date_str} already in {path.name} — skipped")
        return

    prev_val = None
    if rows:
        try:
            prev_val = float(str(rows[-1].get("Index", rows[-1].get("index", ""))).replace(",", ""))
        except (ValueError, TypeError):
            pass
    change = compute_change(value, prev_val)

    write_header = not path.exists() or path.stat().st_size == 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Date", "Index", "% Change"])
        if write_header:
            writer.writeheader()
        writer.writerow({"Date": date_str, "Index": value, "% Change": change})
    pct_str = f"({'+' if change and float(change) > 0 else ''}{change}%)" if change else ""
    print(f"[ok] {code}: {value:,.2f}  {pct_str}  -> {path.name}")

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=".", help="Path to Shipping repo root (default: .)")
    args = parser.parse_args()
    repo_root = Path(args.repo).resolve()

    print("=" * 60)
    print("  Baltic New Indices Scraper")
    print(f"  Repo: {repo_root}")
    print("=" * 60)

    print(f"\n[..] Fetching {API_URL}")
    try:
        ticker = fetch_ticker()
    except Exception as e:
        print(f"[x] API fetch failed: {e}")
        return

    print(f"[ok] Got {len(ticker)} indices from API\n")

    for code, filename in NEW_INDICES.items():
        if code not in ticker:
            print(f"[!]  {code} not found in API response — skipped")
            continue
        value, date_str = ticker[code]
        append_to_csv(repo_root / filename, date_str, code, value)

    print("\n[done]")
    print(f"Files written to: {repo_root}")


if __name__ == "__main__":
    main()
