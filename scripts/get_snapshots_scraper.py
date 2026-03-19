"""
get_snapshots_scraper.py
──────────────────────────────────────────────────────────────────────────────
Fetches ETF flow + performance data from Trackinsight's internal API:
  POST https://www.trackinsight.com/search-api/snapshot/get_snapshots

Discovered via DevTools Network inspection. The endpoint accepts a JSON body:
  { "enterpriseId": null, "requests": [{ "fund": "3NGS", "startDate": "..." }] }

Each response item has: stamp (unix ms), USD:flow, nav, perf

Output CSV columns:
  date, usd_flow, nav, perf, cumulative_flow

Run:
    pip install requests pandas
    python get_snapshots_scraper.py --ticker 3NGS
    python get_snapshots_scraper.py --ticker 3NGS --start 2021-01-01 --end 2026-03-19

No login required if the endpoint is public. If you get 401/403, add:
    python get_snapshots_scraper.py --ticker 3NGS --cookies trackinsight_cookies.json
"""

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import pandas as pd


ENDPOINT   = "https://www.trackinsight.com/search-api/snapshot/get_snapshots"
COOKIE_FILE = Path("trackinsight_cookies.json")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Content-Type": "application/json",
    "Accept": "application/json, */*",
    "Origin": "https://www.trackinsight.com",
    "Referer": "https://www.trackinsight.com/en/fund/3NGS",
    "X-Requested-With": "XMLHttpRequest",
}


def build_session(cookies_path: Path | None) -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    if cookies_path and cookies_path.exists():
        cookies = json.loads(cookies_path.read_text())
        for c in cookies:
            s.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))
        print(f"[ok] Loaded {len(cookies)} cookies from {cookies_path}")
    else:
        print("[ok] No cookies — trying unauthenticated")
    return s


def fetch_snapshots(session: requests.Session, ticker: str,
                    start_date: str, end_date: str) -> list[dict]:
    """
    POST to get_snapshots with the exact payload shape observed in DevTools.
    Tries both with and without endDate since the observed payload was truncated.
    """
    payload = {
        "enterpriseId": None,
        "requests": [
            {
                "fund": ticker,
                "startDate": start_date,
                "endDate": end_date,
                "columns": ["stamp", "USD:flow", "nav", "perf"],
            }
        ]
    }

    print(f"\n[->] POST {ENDPOINT}")
    print(f"     payload: {json.dumps(payload)}")

    try:
        r = session.post(ENDPOINT, json=payload, timeout=30)
        print(f"     status: {r.status_code}")

        if r.status_code == 200:
            return r.json()

        # Try without endDate (maybe it's not supported)
        if r.status_code in (400, 422):
            print("     [!] Retrying without endDate ...")
            payload["requests"][0].pop("endDate", None)  # keep columns
            r = session.post(ENDPOINT, json=payload, timeout=30)
            print(f"     status: {r.status_code}")
            if r.status_code == 200:
                return r.json()

        print(f"     [!] Response body: {r.text[:300]}")
        return []

    except Exception as e:
        print(f"     [!] Request failed: {e}")
        return []


def parse_snapshots(data: list | dict, ticker: str) -> pd.DataFrame:
    """
    Parse the get_snapshots response.

    Actual response shape (confirmed):
      [ {
          "stamp":    {"scale": 1.1574e-08, "data": [18631, 18632, ...]},  # Unix day numbers
          "USD:flow": {"scale": 0.001,      "data": [3, 0, 1864, ...]},    # USD millions
          "nav":      {"scale": 100,        "data": [1096, 945, ...]},
          "perf":     {"scale": 1000000,    "data": [-50272, ...]},
        } ]

    stamp values are Unix DAY numbers (days since 1970-01-01).
    All other fields: actual_value = data[i] * scale.
    USD:flow unit: scale=0.001 on integer -> USD thousands (e.g. 1864*0.001 = $1,864)
    """
    # Response is a list with one item per requested fund
    if isinstance(data, list):
        if not data:
            return pd.DataFrame()
        item = data[0]  # first (and only) fund
    elif isinstance(data, dict):
        item = data
    else:
        print(f"[!] Unexpected type: {type(data)}")
        return pd.DataFrame()

    def unpack(field_data: dict) -> list[float]:
        """Multiply every value in data[] by scale."""
        if not isinstance(field_data, dict):
            return []
        scale = field_data.get("scale", 1)
        raw   = field_data.get("data", [])
        return [v / scale if v is not None else None for v in raw]

    # Stamps are Unix day numbers -> convert to dates
    stamp_field = item.get("stamp", {})
    day_numbers = stamp_field.get("data", [])
    dates = []
    for d in day_numbers:
        try:
            dates.append(
                datetime.fromtimestamp(int(d) * 86400, tz=timezone.utc)
                        .strftime("%Y-%m-%d")
            )
        except Exception:
            dates.append(None)

    flow_vals = unpack(item.get("USD:flow", {}))
    nav_vals  = unpack(item.get("nav",      {}))
    perf_vals = unpack(item.get("perf",     {}))

    # Pad shorter arrays with None
    n = len(dates)
    def pad(lst): return lst + [None] * (n - len(lst))
    flow_vals = pad(flow_vals)
    nav_vals  = pad(nav_vals)
    perf_vals = pad(perf_vals)

    df = pd.DataFrame({
        "date":          dates,
        "usd_flow":      flow_vals,   # USD: data/scale -> actual dollars (e.g. 1864/0.001 = $1,864,000)
        "nav":           nav_vals,
        "perf_pct":      perf_vals,
    })

    df = df.dropna(subset=["date"])
    df = df.drop_duplicates("date").sort_values("date").reset_index(drop=True)

    # Derived columns
    df["cumulative_flow"] = df["usd_flow"].cumsum()
    df["daily_inflow"]    = df["usd_flow"].clip(lower=0)
    df["daily_outflow"]   = df["usd_flow"].clip(upper=0)

    return df


def main():
    ap = argparse.ArgumentParser(description="Trackinsight get_snapshots flow scraper")
    ap.add_argument("--ticker",  default="3NGS")
    ap.add_argument("--start",   default="2021-01-01")
    ap.add_argument("--end",     default=datetime.today().strftime("%Y-%m-%d"))
    ap.add_argument("--cookies", default=None,
                    help="Path to cookies JSON (optional, try without first)")
    ap.add_argument("--outdir",  default=".")
    ap.add_argument("--raw",     action="store_true",
                    help="Also save raw JSON response for inspection")
    args = ap.parse_args()

    ticker   = args.ticker.upper()
    out_dir  = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cookies  = Path(args.cookies) if args.cookies else None

    session  = build_session(cookies)
    raw_data = fetch_snapshots(session, ticker, args.start, args.end)

    # Save raw if requested or if empty (helps debug)
    if args.raw or not raw_data:
        raw_out = out_dir / f"{ticker}_raw_snapshots.json"
        raw_out.write_text(json.dumps(raw_data, indent=2, default=str))
        print(f"\n[saved] Raw response -> {raw_out}")

    if not raw_data:
        print("\n[!] Empty response. Check raw JSON and share it.")
        print("    Also try adding --cookies trackinsight_cookies.json")
        return

    # Print raw structure hint
    if isinstance(raw_data, list) and raw_data:
        first = raw_data[0]
        print(f"\n[info] Response is a list of {len(raw_data)} items")
        print(f"[info] First item keys: {list(first.keys()) if isinstance(first, dict) else type(first)}")
        if isinstance(first, dict):
            for k, v in list(first.items())[:8]:
                print(f"         {k}: {str(v)[:80]}")
    elif isinstance(raw_data, dict):
        print(f"\n[info] Response is a dict with keys: {list(raw_data.keys())}")

    df = parse_snapshots(raw_data, ticker)

    if df.empty:
        print("\n[!] Could not parse response into DataFrame.")
        print("    The raw JSON has been saved — share it so we can adjust the parser.")
        # Save raw anyway
        raw_out = out_dir / f"{ticker}_raw_snapshots.json"
        raw_out.write_text(json.dumps(raw_data, indent=2, default=str))
        print(f"    -> {raw_out}")
        return

    print(f"\n[ok] Parsed {len(df)} rows")
    print(df.head(10).to_string())
    print(f"\n     date range: {df['date'].min()} -> {df['date'].max()}")
    if "usd_flow" in df.columns:
        total = df["usd_flow"].sum()
        print(f"     total net flow: ${total:,.0f}")

    out_csv = out_dir / f"{ticker}_flows_{args.start}_{args.end}.csv"
    df.to_csv(out_csv, index=False)
    print(f"\n[saved] {out_csv}")


if __name__ == "__main__":
    main()
