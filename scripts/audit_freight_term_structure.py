#!/usr/bin/env python3
"""
Deep Forensic Audit of SGX Dry Bulk Freight FFA Derivatives:
- Capesize 5TC (CWF)
- Panamax 4TC/5TC (PWF)
- Supramax 10TC (SWF)
- Handysize 7TC (HWF)

1. Active Contract Historical Depth: Tests daily bars available per contract.
2. Expired Contracts Retention (2018-2025): Verifies multi-year historical retention for expired FFA contracts.
3. Complete Forward Curves & Term Structures: Maps all active forward delivery tenors out to 2028.
"""

import sys
import json
import time
import requests
import pandas as pd
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Referer': 'https://www.sgx.com/',
    'Origin': 'https://www.sgx.com',
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CME_MONTHS = {
    'F': (1, 'Jan'), 'G': (2, 'Feb'), 'H': (3, 'Mar'), 'J': (4, 'Apr'),
    'K': (5, 'May'), 'M': (6, 'Jun'), 'N': (7, 'Jul'), 'Q': (8, 'Aug'),
    'U': (9, 'Sep'), 'V': (10, 'Oct'), 'X': (11, 'Nov'), 'Z': (12, 'Dec')
}

FREIGHT_PRODUCTS = [
    ("CWF", "Capesize 5TC FFA Futures ($/day)"),
    ("PWF", "Panamax 4TC/5TC FFA Futures ($/day)"),
    ("SWF", "Supramax 10TC FFA Futures ($/day)"),
    ("HWF", "Handysize 7TC FFA Futures ($/day)"),
]

def audit_active_freight_depth():
    print("=" * 95)
    print("PART 1: ACTIVE FREIGHT CONTRACT HISTORICAL DEPTH (DAILY BARS PER CONTRACT)")
    print("=" * 95)

    test_contracts = [
        ("CWFU26", "Capesize Sep 2026"),
        ("CWFV26", "Capesize Oct 2026"),
        ("CWFZ26", "Capesize Dec 2026"),
        ("CWFZ27", "Capesize Dec 2027"),
        ("PWFU26", "Panamax Sep 2026"),
        ("PWFV26", "Panamax Oct 2026"),
        ("PWFZ26", "Panamax Dec 2026"),
        ("SWFU26", "Supramax Sep 2026"),
        ("SWFV26", "Supramax Oct 2026"),
        ("SWFZ26", "Supramax Dec 2026"),
        ("HWFU26", "Handysize Sep 2026"),
        ("HWFV26", "Handysize Oct 2026"),
    ]

    for ticker, desc in test_contracts:
        url = f"https://api.sgx.com/derivatives/v1.0/history/symbol/{ticker}?days=5y&category=futures&params=base-date,daily-settlement-price-abs,total-volume,open-interest"
        try:
            r = SESSION.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json().get('data', [])
                if data:
                    dates = [d.get('base-date') for d in data if d.get('base-date')]
                    prices = [float(d.get('daily-settlement-price-abs', 0)) for d in data if d.get('daily-settlement-price-abs') is not None]
                    ois = [float(d.get('open-interest', 0)) for d in data]

                    first_date = dates[0] if dates else 'N/A'
                    last_date = dates[-1] if dates else 'N/A'
                    latest_p = prices[-1] if prices else 0
                    latest_oi = ois[-1] if ois else 0

                    print(f"✔ {ticker:<8} ({desc:<22}) -> {len(data):>4} Daily Bars | {first_date} to {last_date} | Settle: ${latest_p:>8,.0f}/day | OI: {latest_oi:>6,.0f} lots")
                else:
                    print(f"✖ {ticker:<8} -> 0 records")
            else:
                print(f"✖ {ticker:<8} -> HTTP {r.status_code}")
        except Exception as e:
            print(f"Error {ticker}: {e}")
        time.sleep(0.04)

def audit_expired_freight_contracts():
    print("\n" + "=" * 95)
    print("PART 2: EXPIRED FREIGHT FFA CONTRACTS RETENTION (2018 TO 2025)")
    print("=" * 95)

    expired_freight = [
        # Capesize
        ("CWFZ25", "Capesize Dec 2025 (Expired)"),
        ("CWFZ24", "Capesize Dec 2024 (Expired)"),
        ("CWFZ23", "Capesize Dec 2023 (Expired)"),
        ("CWFZ22", "Capesize Dec 2022 (Expired)"),
        ("CWFZ21", "Capesize Dec 2021 (Expired)"),
        # Panamax
        ("PWFZ25", "Panamax Dec 2025 (Expired)"),
        ("PWFZ24", "Panamax Dec 2024 (Expired)"),
        ("PWFZ23", "Panamax Dec 2023 (Expired)"),
        ("PWFZ22", "Panamax Dec 2022 (Expired)"),
        ("PWFZ21", "Panamax Dec 2021 (Expired)"),
        # Supramax
        ("SWFZ25", "Supramax Dec 2025 (Expired)"),
        ("SWFZ24", "Supramax Dec 2024 (Expired)"),
        ("SWFZ23", "Supramax Dec 2023 (Expired)"),
        ("SWFZ22", "Supramax Dec 2022 (Expired)"),
        ("SWFZ21", "Supramax Dec 2021 (Expired)"),
        # Handysize
        ("HWFZ25", "Handysize Dec 2025 (Expired)"),
        ("HWFZ24", "Handysize Dec 2024 (Expired)"),
        ("HWFZ23", "Handysize Dec 2023 (Expired)"),
        ("HWFZ22", "Handysize Dec 2022 (Expired)"),
    ]

    for ticker, desc in expired_freight:
        url = f"https://api.sgx.com/derivatives/v1.0/history/symbol/{ticker}?days=5y&category=futures&params=base-date,daily-settlement-price-abs,total-volume,open-interest"
        try:
            r = SESSION.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json().get('data', [])
                if data:
                    dates = [d.get('base-date') for d in data if d.get('base-date')]
                    prices = [float(d.get('daily-settlement-price-abs', 0)) for d in data if d.get('daily-settlement-price-abs') is not None]
                    print(f"✔ [EXPIRED RETAINED] {ticker:<8} ({desc:<26}) -> {len(data):>4} Daily Bars | {dates[0]} to {dates[-1]} | Final Settle: ${prices[-1]:>8,.0f}/day")
                else:
                    print(f"✖ [NO DATA]          {ticker:<8} ({desc:<26}) -> 0 records")
            else:
                print(f"✖ [ERROR]            {ticker:<8} -> HTTP {r.status_code}")
        except Exception as e:
            print(f"Error {ticker}: {e}")
        time.sleep(0.04)

def map_all_freight_forward_curves():
    print("\n" + "=" * 95)
    print("PART 3: MAPPING THE COMPLETE ACTIVE FORWARD CURVES (CAPE, PANAMAX, SUPRAMAX, HANDY)")
    print("=" * 95)

    now = datetime.now()
    cur_year = now.year
    cur_month = now.month

    curve_tenors = []
    for y in range(cur_year, cur_year + 3):
        y2 = str(y)[-2:]
        for m_code, (m_num, m_name) in CME_MONTHS.items():
            if y == cur_year and m_num < cur_month:
                continue
            curve_tenors.append((m_code, y2, y, m_num, m_name))

    for p_code, p_name in FREIGHT_PRODUCTS:
        print(f"\n📈 FORWARD CURVE: {p_name.upper()}")
        print("-" * 95)
        print(f"{'Tenor':<12} | {'Ticker':<9} | {'Settlement ($/day)':<22} | {'Open Interest':<16} | {'Daily Vol':<12} | {'Curve Spread'}")
        print("-" * 95)

        curve_data = []
        for m_code, y2, y, m_num, m_name in curve_tenors:
            ticker = f"{p_code}{m_code}{y2}"
            url = f"https://api.sgx.com/derivatives/v1.0/history/symbol/{ticker}?days=5d&category=futures&params=base-date,daily-settlement-price-abs,total-volume,open-interest"
            try:
                r = SESSION.get(url, timeout=5)
                if r.status_code == 200:
                    data = r.json().get('data', [])
                    if data:
                        valid_rows = [row for row in data if row.get('daily-settlement-price-abs', 0) > 0 or row.get('open-interest', 0) > 0]
                        if valid_rows:
                            last = valid_rows[-1]
                            curve_data.append({
                                "ticker": ticker,
                                "tenor": f"{m_name}-{y}",
                                "settlement": last.get('daily-settlement-price-abs', 0),
                                "volume": last.get('total-volume', 0),
                                "open_interest": last.get('open-interest', 0),
                            })
            except Exception:
                pass
            time.sleep(0.03)

        prompt_price = curve_data[0]['settlement'] if curve_data else 0
        total_oi = sum(r['open_interest'] for r in curve_data)
        total_vol = sum(r['volume'] for r in curve_data)

        for i, row in enumerate(curve_data):
            settle = row['settlement']
            oi = row['open_interest']
            vol = row['volume']
            diff = settle - prompt_price
            pct = (diff / prompt_price) * 100 if prompt_price else 0
            spread_str = f"{diff:>+8,.0f} ({pct:>+5.1f}%)" if i > 0 else "PROMPT BASELINE"
            print(f"{row['tenor']:<12} | {row['ticker']:<9} | ${settle:>10,.0f} /day       | {oi:>8,.0f} lots     | {vol:>6,.0f}       | {spread_str}")

        print(f"Total Curve Open Interest: {total_oi:,.0f} lots | Daily Volume: {total_vol:,.0f} lots | Active Monthly Tenors: {len(curve_data)}")

if __name__ == "__main__":
    audit_active_freight_depth()
    audit_expired_freight_contracts()
    map_all_freight_forward_curves()
