#!/usr/bin/env python3
"""
SGX API Deep Discovery & Probing Script
Probes Singapore Exchange (SGX) Derivatives public endpoints.
"""

import sys
import json
import time
import requests
from datetime import datetime, date

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://www.sgx.com/',
    'Origin': 'https://www.sgx.com',
    'Accept': 'application/json, text/plain, */*'
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

def probe_metadata_endpoints():
    print("=" * 80)
    print("1. PROBING SGX DERIVATIVES CATALOG & METADATA ENDPOINTS")
    print("=" * 80)
    
    test_urls = [
        "https://api.sgx.com/derivatives/v1.0/products?kind=futures",
        "https://api.sgx.com/derivatives/v1.0/products?kind=options",
        "https://api.sgx.com/derivatives/v1.0/categories?kind=futures",
        "https://api.sgx.com/derivatives/v1.0/contract-specs?kind=futures",
        "https://api.sgx.com/derivatives/v1.0/delayed-prices/futures",
    ]
    
    for url in test_urls:
        try:
            r = SESSION.get(url, timeout=10)
            print(f"URL: {url}")
            print(f"Status: HTTP {r.status_code} | Content-Type: {r.headers.get('Content-Type', '')} | Size: {len(r.content)} bytes")
            if r.status_code == 200:
                try:
                    js = r.json()
                    sample = json.dumps(js, indent=2)[:500]
                    print(f"JSON Sample:\n{sample}\n")
                except Exception:
                    print(f"Text Sample: {r.text[:200]}\n")
            else:
                print(f"Response: {r.text[:150]}\n")
        except Exception as e:
            print(f"Error for {url}: {e}\n")

# CME month codes
CME_MONTHS = {
    'F': 'Jan', 'G': 'Feb', 'H': 'Mar', 'J': 'Apr',
    'K': 'May', 'M': 'Jun', 'N': 'Jul', 'Q': 'Aug',
    'U': 'Sep', 'V': 'Oct', 'X': 'Nov', 'Z': 'Dec'
}

CANDIDATE_PRODUCTS = {
    "Freight (Dry Bulk & Tankers)": [
        ("CWF", "Capesize Freight Futures (5TC)"),
        ("PWF", "Panamax Freight Futures (4TC/5TC)"),
        ("SWF", "Supramax Freight Futures (10TC)"),
        ("HWF", "Handysize Freight Futures (7TC)"),
        ("TC2", "Tanker TC2 FFA Futures"),
        ("TD3", "Tanker TD3C FFA Futures"),
    ],
    "Iron Ore & Steel Complex": [
        ("FEF", "SGX TSI Iron Ore CFR China (62% Fe Fines) Futures"),
        ("FEM", "SGX Fastmarkets Iron Ore CFR China (65% Fe Fines) Futures"),
        ("FOL", "SGX TSI Iron Ore CFR China (62% Fe) Lump Premium Futures"),
        ("FES", "SGX Platts Iron Ore CFR China (58% Fe Fines) Futures"),
        ("HRB", "SGX Steel Rebar CFR China Futures"),
        ("HRC", "SGX Steel HRC FOB China Futures"),
        ("FE",  "SGX Iron Ore Futures (Alternative Code)"),
        ("IO",  "SGX Iron Ore Fines"),
    ],
    "Energy & Marine Fuels": [
        ("MOF", "SGX Platts Marine Fuel 0.5% FOB Singapore (VLSFO) Futures"),
        ("FOF", "SGX Platts Fuel Oil 380cst FOB Singapore (HSFO) Futures"),
        ("FO1", "SGX Platts Fuel Oil 180cst FOB Singapore Futures"),
        ("DBF", "SGX Platts Dubai Crude Oil Index Futures"),
        ("LGF", "SGX LNG Platts JKM Futures"),
        ("EFW", "SGX Singapore Electricity Futures"),
    ],
    "Petrochemicals": [
        ("MEG", "SGX ICIS CFR China Monoethylene Glycol Futures"),
        ("PXF", "SGX ICIS CFR Taiwan/China Paraxylene Futures"),
        ("BZF", "SGX ICIS FOB Korea Benzene Futures"),
        ("MTF", "SGX ICIS CFR China Methanol Futures"),
    ],
    "Agriculture & Rubber": [
        ("TF",  "SGX SICOM TSR20 Rubber Futures"),
        ("RSS", "SGX SICOM RSS3 Rubber Futures"),
        ("CPO", "SGX Crude Palm Oil Futures"),
    ],
    "Macro Equity & FX Derivatives": [
        ("CN",  "FTSE China A50 Index Futures"),
        ("IN",  "GIFT Nifty 50 Index Futures"),
        ("NK",  "Nikkei 225 Index Futures"),
        ("SG",  "MSCI Singapore Index Futures"),
        ("TW",  "FTSE Taiwan Index Futures"),
        ("UC",  "USD/CNH FX Futures"),
        ("IU",  "INR/USD FX Futures"),
        ("KRW", "KRW/USD FX Futures"),
    ]
}

def probe_product_contracts():
    print("=" * 80)
    print("2. PROBING SGX PRODUCT DERIVATIVES & REAL-TIME / HISTORICAL FIELDS")
    print("=" * 80)

    now = datetime.now()
    cur_year_str = str(now.year)[-2:]
    next_year_str = str(now.year + 1)[-2:]
    
    month_codes = list(CME_MONTHS.keys())
    cur_month_idx = now.month - 1
    sample_months = [
        month_codes[cur_month_idx % 12],
        month_codes[(cur_month_idx + 1) % 12],
        month_codes[(cur_month_idx + 2) % 12],
        month_codes[(cur_month_idx + 3) % 12],
        'U', 'V', 'X', 'Z'
    ]

    discovered_products = []

    for cat_name, prod_list in CANDIDATE_PRODUCTS.items():
        print(f"\n[CATEGORY: {cat_name.upper()}]")
        print("-" * 80)
        
        for p_code, p_name in prod_list:
            found_active = False
            best_sample = None
            active_ticker_sample = None

            for y_str in [cur_year_str, next_year_str]:
                if found_active:
                    break
                for m_code in sample_months:
                    ticker = f"{p_code}{m_code}{y_str}"
                    url = (
                        f"https://api.sgx.com/derivatives/v1.0/history/symbol/{ticker}"
                        f"?days=5d&category=futures"
                        f"&params=base-date%2Ctotal-volume%2Cdaily-settlement-price-abs"
                        f"%2Copen-interest%2Chigh%2Clow%2Cfirst%2Clast%2Cchange-abs"
                    )
                    try:
                        r = SESSION.get(url, timeout=10)
                        if r.status_code == 200:
                            data = r.json().get('data', [])
                            if data and len(data) > 0:
                                found_active = True
                                active_ticker_sample = ticker
                                best_sample = data[0]
                                break
                    except Exception:
                        pass
                    time.sleep(0.04)

            if found_active:
                price = best_sample.get('daily-settlement-price-abs')
                vol = best_sample.get('total-volume', 0)
                oi = best_sample.get('open-interest', 0)
                b_date = best_sample.get('base-date')
                fields = list(best_sample.keys())
                print(f"[ACTIVE] {p_code:<5} | {p_name:<55} | Sample: {active_ticker_sample} | Date: {b_date} | Settle: {price} | Vol: {vol} | OI: {oi}")
                discovered_products.append({
                    "category": cat_name,
                    "code": p_code,
                    "name": p_name,
                    "sample_ticker": active_ticker_sample,
                    "latest_settlement": price,
                    "volume": vol,
                    "open_interest": oi,
                    "available_fields": fields,
                    "raw_sample": best_sample
                })
            else:
                print(f"[INACTIVE / CODE NOT MATCHED] {p_code:<5} | {p_name}")

    print("\n" + "=" * 80)
    print("3. COMPLETE FIELD SCHEMA RETURNED BY SGX DERIVATIVES API")
    print("=" * 80)
    if discovered_products:
        sample_prod = discovered_products[0]
        print(f"Sample Ticker: {sample_prod['sample_ticker']}")
        print("Raw JSON Data Payload Structure:")
        print(json.dumps(sample_prod['raw_sample'], indent=4))
        
    print("\n" + "=" * 80)
    print(f"DISCOVERY SUMMARY: Found {len(discovered_products)} active tradable derivative product lines on SGX API")
    print("=" * 80)

if __name__ == "__main__":
    probe_metadata_endpoints()
    probe_product_contracts()
