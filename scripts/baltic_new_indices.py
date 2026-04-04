"""
Baltic Exchange — New Indices Scraper
======================================
Scrapes BLNG, BLPG, FBX, BAI00 from the Baltic Exchange homepage
and appends them to historical CSVs, using BDI + BCI as date anchors
to derive the correct trading date from existing historical data.

Date logic:
  - Scrape live BDI and BCI values from Baltic Exchange
  - Match BDI against bdiy_historical.csv
  - Cross-check BCI against cape_historical.csv
  - Use the matched date — inherits all holiday/weekend logic from
    your existing series automatically
  - If both match → confident date assignment
  - If only one matches → use it with a warning
  - If neither matches → new data point, use today + flag for review

Output schema (matches existing repo CSVs):
  Date (DD-MM-YYYY), Index, % Change

New files created (repo root):
  blng_historical.csv
  blpg_historical.csv
  fbx_historical.csv
  bai_historical.csv

Usage:
  pip install playwright
  playwright install chromium
  python baltic_new_indices.py --repo C:\\path\\to\\Shipping
"""

import csv
import argparse
import asyncio
from datetime import date, datetime
from pathlib import Path

from playwright.async_api import async_playwright

URL = "https://www.balticexchange.com/en/index.html"

# Indices we scrape but already have in the repo — used only for date matching
ANCHOR_INDICES = {"BDI", "BCI"}

# Indices we want to record — not freely available elsewhere
NEW_INDICES = {
    "BLNG":  "blng_historical.csv",
    "BLPG":  "blpg_historical.csv",
    "FBX":   "fbx_historical.csv",
    "BAI00": "bai_historical.csv",
}

# Existing CSVs used as date anchors (relative to repo root)
ANCHOR_FILES = {
    "BDI": "bdiy_historical.csv",
    "BCI": "cape_historical.csv",
}

# ── Playwright scrape ───────────────────────────────────────────────────────

async def scrape_live() -> dict:
    """Returns {code: value} for all ticker indices."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()
        print(f"[->] Loading {URL}")
        await page.goto(URL, wait_until="networkidle", timeout=60_000)
        await page.wait_for_timeout(3_000)

        tickets = await page.query_selector_all("#ticker .ticket")
        print(f"[ok] Found {len(tickets)} ticker items")

        result = {}
        for ticket in tickets:
            code  = (await (await ticket.query_selector(".index")).inner_text()).strip()
            value = (await (await ticket.query_selector(".value")).inner_text()).strip()
            result[code] = float(value.replace(",", ""))

        await browser.close()
    return result

# ── Date matching ───────────────────────────────────────────────────────────

def load_anchor_csv(path: Path) -> dict:
    """
    Load an existing historical CSV and return {value: date_str} mapping.
    CSV schema: Date (DD-MM-YYYY), Index, % Change
    """
    if not path.exists():
        print(f"[!]  Anchor file not found: {path}")
        return {}
    mapping = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                val  = float(str(row.get("Index", row.get("index", ""))).replace(",", ""))
                dstr = row.get("Date", row.get("date", "")).strip()
                mapping[val] = dstr
            except (ValueError, TypeError):
                continue
    return mapping

def find_date(live_values: dict, repo_root: Path) -> tuple[str, str]:
    """
    Match live BDI and BCI against existing CSVs.
    Returns (date_str_DD-MM-YYYY, confidence) where confidence is
    'confirmed' | 'partial' | 'new'
    """
    matches = {}

    for code, filename in ANCHOR_FILES.items():
        live_val = live_values.get(code)
        if live_val is None:
            continue
        anchor = load_anchor_csv(repo_root / filename)
        if live_val in anchor:
            matches[code] = anchor[live_val]
            print(f"[ok] {code} = {live_val:,.0f} matched to date {anchor[live_val]}")
        else:
            print(f"[!]  {code} = {live_val:,.0f} not found in {filename} — may be new data")

    dates_found = list(set(matches.values()))

    if len(dates_found) == 1:
        confidence = "confirmed" if len(matches) == 2 else "partial"
        return dates_found[0], confidence
    elif len(dates_found) > 1:
        # Mismatch between anchors — use BDI as primary
        print(f"[!]  Anchor mismatch: {matches} — using BDI date")
        return matches.get("BDI", dates_found[0]), "partial"
    else:
        # No match found — new data point not yet in repo
        today = date.today().strftime("%d-%m-%Y")
        print(f"[!]  No anchor match — using today {today} (flagged for review)")
        return today, "new"

# ── CSV append ──────────────────────────────────────────────────────────────

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

    # Dedup — skip if this date already recorded
    existing_dates = {r.get("Date", r.get("date", "")) for r in rows}
    if date_str in existing_dates:
        print(f"[--] {code}: {date_str} already in {path.name} — skipped")
        return

    # Compute % change from last row
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
        writer.writerow({
            "Date":     date_str,
            "Index":    value,
            "% Change": change,
        })
    print(f"[ok] {code}: {value:,.2f}  ({'+' if change and float(change)>0 else ''}{change}%)  -> {path.name}")

# ── Main ────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--repo",
        default=".",
        help="Path to your Shipping repo root (default: current directory)"
    )
    args = parser.parse_args()
    repo_root = Path(args.repo).resolve()

    print("=" * 60)
    print(f"  Baltic New Indices Scraper")
    print(f"  Repo: {repo_root}")
    print("=" * 60)

    # 1. Scrape live values
    live = await scrape_live()

    if not live:
        print("[x] Nothing scraped.")
        return

    print(f"\n[dbg] Live values: { {k: v for k, v in live.items()} }")

    # 2. Derive date from anchors
    print("\n[..] Matching date from anchor CSVs ...")
    date_str, confidence = find_date(live, repo_root)
    print(f"[ok] Date: {date_str}  (confidence: {confidence})\n")

    # 3. Append new indices to their CSVs
    print("[..] Appending new indices ...")
    for code, filename in NEW_INDICES.items():
        value = live.get(code)
        if value is None:
            print(f"[!]  {code} not found in scraped data — skipped")
            continue
        append_to_csv(repo_root / filename, date_str, code, value)

    print("\n[done]")
    print(f"Files written to: {repo_root}")


if __name__ == "__main__":
    asyncio.run(main())
