#!/usr/bin/env python3
"""
Forensic Raw Data Verifier for SGX Contracts
Pulls full contract time series directly from SGX API and performs exhaustive analysis:
1. Non-zero price validation across all historical trading days.
2. Volume and Open Interest progression from contract inception to delivery.
3. Prints exact raw JSON rows for earliest active days, middle days, and latest days.
"""

import sys
import json
import requests
import pandas as pd
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Referer': 'https://www.sgx.com/',
    'Origin': 'https://www.sgx.com',
}

CONTRACTS_TO_AUDIT = [
    ("FEFU26", "SGX 62% Iron Ore (Sep 2026 Delivery)"),
    ("FEFZ26", "SGX 62% Iron Ore (Dec 2026 Delivery)"),
    ("FEFZ23", "SGX 62% Iron Ore (Dec 2023 Delivery - Expired)"),
    ("CWFU26", "SGX Capesize 5TC (Sep 2026 Delivery)"),
    ("CWFV26", "SGX Capesize 5TC (Oct 2026 Delivery)"),
    ("CWFZ23", "SGX Capesize 5TC (Dec 2023 Delivery - Expired)"),
    ("PWFU26", "SGX Panamax 5TC (Sep 2026 Delivery)"),
    ("SWFU26", "SGX Supramax 10TC (Sep 2026 Delivery)"),
]

def audit_contract(ticker, desc):
    print("=" * 95)
    print(f"AUDITING CONTRACT: {ticker} ({desc})")
    print("=" * 95)

    url = f"https://api.sgx.com/derivatives/v1.0/history/symbol/{ticker}?days=5y&category=futures&params=base-date,daily-settlement-price-abs,total-volume,open-interest"
    
    r = requests.get(url, headers=HEADERS, timeout=15)
    print(f"HTTP Status: {r.status_code} | Content-Type: {r.headers.get('Content-Type')} | Content Length: {len(r.content)} bytes")
    
    if r.status_code != 200:
        print(f"FAILED: {r.text[:200]}")
        return

    js = r.json()
    meta = js.get('meta', {})
    data = js.get('data', [])

    print(f"API Meta: code={meta.get('code')}, message={meta.get('message')}")
    print(f"Total raw daily records returned: {len(data)}")

    if not data:
        print("EMPTY DATA ARRAY!")
        return

    df = pd.DataFrame(data)
    df.columns = ['price', 'volume', 'open_interest', 'base_date']
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
    df['open_interest'] = pd.to_numeric(df['open_interest'], errors='coerce').fillna(0)

    # Filter non-zero prices vs zero prices
    nonzero_df = df[df['price'] > 0].copy()
    zero_df = df[df['price'] <= 0].copy()

    print(f"\n--- DATA INTEGRITY & PRICE ACTIVITY BREAKDOWN ---")
    print(f"Total Trading Days in Series: {len(df)}")
    print(f"Active Days with Real Non-Zero Settlement Price: {len(nonzero_df)} ({len(nonzero_df)/len(df)*100:.1f}%)")
    print(f"Days with Price == 0.0 (Pre-listing / Dormant): {len(zero_df)} ({len(zero_df)/len(df)*100:.1f}%)")
    
    if len(nonzero_df) > 0:
        p = nonzero_df['price']
        print(f"Price Statistics: Min=${p.min():.2f} | 25%=${p.quantile(0.25):.2f} | Median=${p.median():.2f} | Mean=${p.mean():.2f} | 75%=${p.quantile(0.75):.2f} | Max=${p.max():.2f}")
        print(f"Open Interest: Current={nonzero_df['open_interest'].iloc[-1]:,.0f} lots | Peak={nonzero_df['open_interest'].max():,.0f} lots")
        print(f"Daily Volume: Current={nonzero_df['volume'].iloc[-1]:,.0f} lots | Peak={nonzero_df['volume'].max():,.0f} lots | Total Cleared={nonzero_df['volume'].sum():,.0f} lots")

        print(f"\n[SAMPLE 1: EARLIEST 5 ACTIVE TRADING DAYS WITH REAL PRICES]")
        print(f"{'Date':<10} | {'Settlement Price':<18} | {'Daily Volume':<14} | {'Open Interest':<14}")
        print("-" * 65)
        for _, r in nonzero_df.head(5).iterrows():
            print(f"{r['base_date']:<10} | ${r['price']:>12.2f}      | {r['volume']:>10,.0f}   | {r['open_interest']:>10,.0f}")

        print(f"\n[SAMPLE 2: MIDDLE 5 TRADING DAYS]")
        print(f"{'Date':<10} | {'Settlement Price':<18} | {'Daily Volume':<14} | {'Open Interest':<14}")
        print("-" * 65)
        mid_idx = len(nonzero_df) // 2
        for _, r in nonzero_df.iloc[mid_idx-2:mid_idx+3].iterrows():
            print(f"{r['base_date']:<10} | ${r['price']:>12.2f}      | {r['volume']:>10,.0f}   | {r['open_interest']:>10,.0f}")

        print(f"\n[SAMPLE 3: MOST RECENT 5 TRADING DAYS]")
        print(f"{'Date':<10} | {'Settlement Price':<18} | {'Daily Volume':<14} | {'Open Interest':<14}")
        print("-" * 65)
        for _, r in nonzero_df.tail(5).iterrows():
            print(f"{r['base_date']:<10} | ${r['price']:>12.2f}      | {r['volume']:>10,.0f}   | {r['open_interest']:>10,.0f}")
    print("\n")

if __name__ == "__main__":
    for ticker, desc in CONTRACTS_TO_AUDIT:
        audit_contract(ticker, desc)
