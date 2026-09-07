#!/usr/bin/env python3
"""
Exhaustive Forensic Probe of Singapore Exchange (SGX) API Architecture & Endpoints
1. Downloads and reverse-engineers SGX web pages and frontend JavaScript bundles.
2. Extracts all internal API endpoints on api.sgx.com and www.sgx.com.
3. Tests all discovered endpoints across Derivatives, Securities, Commodities, FX, Indices, and Fixed Income.
4. Produces an exhaustive inventory of everything SGX publishes.
"""

import sys
import os
import re
import json
import time
from urllib.parse import urljoin
from datetime import datetime, date
import requests

sys.stdout.reconfigure(encoding='utf-8')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Referer': 'https://www.sgx.com/',
    'Origin': 'https://www.sgx.com',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

def crawl_sgx_frontend_endpoints():
    print("=" * 85)
    print("STEP 1: REVERSE-ENGINEERING SGX FRONTEND JS BUNDLES FOR API ROUTES")
    print("=" * 85)

    pages = [
        "https://www.sgx.com/derivatives/delayed-prices-futures",
        "https://www.sgx.com/derivatives/products",
        "https://www.sgx.com/securities/delayed-prices-stocks",
        "https://www.sgx.com/indices/products",
        "https://www.sgx.com/commodities",
    ]

    discovered_js_files = set()
    for p_url in pages:
        try:
            r = SESSION.get(p_url, timeout=12)
            if r.status_code == 200:
                js_links = re.findall(r'src=["\']([^"\']+\.js(?:\?[^"\']*)?)["\']', r.text)
                for link in js_links:
                    full_js = urljoin(p_url, link)
                    if 'sgx.com' in full_js:
                        discovered_js_files.add(full_js)
        except Exception as e:
            print(f"Error crawling {p_url}: {e}")

    print(f"Found {len(discovered_js_files)} SGX frontend JavaScript bundle files.")
    
    api_patterns = set()
    # Scan JS bundles for api.sgx.com, /api/, and endpoint paths
    for js_url in list(discovered_js_files)[:15]:
        try:
            r = SESSION.get(js_url, timeout=12)
            if r.status_code == 200:
                # Find all api endpoints
                matches = re.findall(r'https?://api\.sgx\.com/[a-zA-Z0-9_\-\./?=&%]+', r.text)
                for m in matches:
                    api_patterns.add(m)
                matches_rel = re.findall(r'["\'](/api/[a-zA-Z0-9_\-\./?=&%]+)["\']', r.text)
                for m in matches_rel:
                    api_patterns.add("https://www.sgx.com" + m)
                matches_v1 = re.findall(r'["\'](/derivatives/v1\.[0-9]/[a-zA-Z0-9_\-\./?=&%]+)["\']', r.text)
                for m in matches_v1:
                    api_patterns.add("https://api.sgx.com" + m)
                matches_sec = re.findall(r'["\'](/securities/v1\.[0-9]/[a-zA-Z0-9_\-\./?=&%]+)["\']', r.text)
                for m in matches_sec:
                    api_patterns.add("https://api.sgx.com" + m)
        except Exception as e:
            pass

    print(f"Discovered {len(api_patterns)} unique API route patterns from frontend code:")
    for ep in sorted(api_patterns):
        print(f" • {ep}")

    return api_patterns

def probe_all_endpoints(discovered_endpoints):
    print("\n" + "=" * 85)
    print("STEP 2: SYSTEMATICALLY PROBING DISCOVERED & KNOWN SGX API ENDPOINTS")
    print("=" * 85)

    base_probes = [
        # Derivatives History & Pricing
        "https://api.sgx.com/derivatives/v1.0/history/symbol/FEFV26?days=5d&category=futures&params=base-date,daily-settlement-price-abs,total-volume,open-interest",
        "https://api.sgx.com/derivatives/v1.0/history/symbol/CWFU26?days=5d&category=futures&params=base-date,daily-settlement-price-abs,total-volume,open-interest",
        "https://api.sgx.com/derivatives/v1.0/history/symbol/CNU26?days=5d&category=futures&params=base-date,daily-settlement-price-abs,total-volume,open-interest",
        "https://api.sgx.com/derivatives/v1.0/history/symbol/UCU26?days=5d&category=futures&params=base-date,daily-settlement-price-abs,total-volume,open-interest",
        "https://api.sgx.com/derivatives/v1.0/history/symbol/WMPV26?days=5d&category=futures&params=base-date,daily-settlement-price-abs,total-volume,open-interest",
        "https://api.sgx.com/derivatives/v1.0/history/symbol/TFU26?days=5d&category=futures&params=base-date,daily-settlement-price-abs,total-volume,open-interest",
        "https://api.sgx.com/derivatives/v1.0/history/symbol/PXFU26?days=5d&category=futures&params=base-date,daily-settlement-price-abs,total-volume,open-interest",
        "https://api.sgx.com/derivatives/v1.0/history/symbol/BZFU26?days=5d&category=futures&params=base-date,daily-settlement-price-abs,total-volume,open-interest",
        "https://api.sgx.com/derivatives/v1.0/history/symbol/GOFV26?days=5d&category=futures&params=base-date,daily-settlement-price-abs,total-volume,open-interest",
        
        # Derivatives Delayed Prices & Realtime Lists
        "https://api.sgx.com/derivatives/v1.0/delayed-prices/futures?category=commodities",
        "https://api.sgx.com/derivatives/v1.0/delayed-prices/futures?category=equities",
        "https://api.sgx.com/derivatives/v1.0/delayed-prices/futures?category=fx",
        
        # Securities & Equities / Stocks
        "https://api.sgx.com/securities/v1.1?pagestart=0&pagesize=10",
        "https://api.sgx.com/securities/v1.1/stocks?pagestart=0&pagesize=10",
        "https://api.sgx.com/securities/v1.1/instruments?pagestart=0&pagesize=10",
        
        # Indices & Benchmarks
        "https://api.sgx.com/indices/v1.0/historical?index=STI",
        "https://api.sgx.com/indices/v1.0/list",
        "https://www.sgx.com/api/indices/summary",
        
        # Company Announcements & Corporate Actions
        "https://api.sgx.com/announcements/v1.0",
        "https://www.sgx.com/api/corporate-actions",
        
        # Market Reports & Daily Clearing Files
        "https://www.sgx.com/api/derivatives/daily-bulletin",
        "https://www.sgx.com/api/derivatives/settlement-prices",
    ]

    all_to_probe = list(dict.fromkeys(base_probes + list(discovered_endpoints)))
    results = []

    for url in all_to_probe:
        try:
            r = SESSION.get(url, timeout=10)
            status = r.status_code
            content_type = r.headers.get('Content-Type', '')
            is_json = 'application/json' in content_type
            size = len(r.content)
            
            data_preview = None
            field_keys = []
            if status == 200 and is_json:
                try:
                    js = r.json()
                    data_preview = js
                    if isinstance(js, dict):
                        field_keys = list(js.keys())
                        if 'data' in js and isinstance(js['data'], list) and len(js['data']) > 0:
                            if isinstance(js['data'][0], dict):
                                field_keys += ["data[0]." + k for k in js['data'][0].keys()]
                    elif isinstance(js, list) and len(js) > 0:
                        if isinstance(js[0], dict):
                            field_keys = list(js[0].keys())
                except Exception:
                    pass

            results.append({
                "url": url,
                "status": status,
                "content_type": content_type,
                "size": size,
                "fields": field_keys,
                "preview": data_preview
            })

            status_str = f"HTTP {status}"
            if status == 200 and is_json:
                status_str += f" [JSON - {size} bytes]"
            elif status == 200:
                status_str += f" [HTML/Text - {size} bytes]"
            
            print(f"[{status_str:<22}] {url}")
            if field_keys:
                print(f"   ↳ Schema Fields: {', '.join(field_keys[:8])}")
        except Exception as e:
            print(f"[ERROR] {url}: {e}")
        time.sleep(0.05)

    return results

def deep_probe_all_derivative_products():
    print("\n" + "=" * 85)
    print("STEP 3: DEEP SCAN OF COMMODITY, FREIGHT, FX, ENERGY & FINANCIAL CONTRACTS")
    print("=" * 85)

    # Master list of all known SGX derivative product codes
    all_codes = [
        # Dry Bulk Freight
        ("CWF", "Capesize 5TC Freight Futures"),
        ("PWF", "Panamax 4TC/5TC Freight Futures"),
        ("SWF", "Supramax 10TC Freight Futures"),
        ("HWF", "Handysize 7TC Freight Futures"),
        # Iron Ore & Steel
        ("FEF", "TSI Iron Ore 62% Fe CFR China Futures"),
        ("FEM", "Fastmarkets Iron Ore 65% Fe CFR China Futures"),
        ("FOL", "TSI Iron Ore 62% Lump Premium Futures"),
        ("FES", "Platts Iron Ore 58% Fe Fines Futures"),
        ("HRB", "Steel Rebar CFR China Futures"),
        ("HRC", "Steel HRC FOB China Futures"),
        # Energy & Gasoil
        ("GOF", "Gasoil FOB Singapore Futures"),
        ("MOF", "Marine Fuel 0.5% FOB Singapore Futures"),
        ("FOF", "Fuel Oil 380cst FOB Singapore Futures"),
        ("DBF", "Dubai Crude Oil Futures"),
        ("LGF", "LNG Platts JKM Futures"),
        # Petrochemicals
        ("PXF", "Paraxylene CFR Taiwan/China Futures"),
        ("BZF", "Benzene FOB Korea Futures"),
        ("MTF", "Methanol CFR China Futures"),
        # Agriculture & Softs
        ("TF",  "SICOM TSR20 Rubber Futures"),
        ("RSS", "SICOM RSS3 Rubber Futures"),
        ("WMP", "Whole Milk Powder Futures"),
        ("SMP", "Skim Milk Powder Futures"),
        ("AMF", "Anhydrous Milk Fat Futures"),
        ("BTR", "Butter Futures"),
        # Equity Indexes
        ("CN",  "FTSE China A50 Index Futures"),
        ("NK",  "Nikkei 225 Index Futures"),
        ("SGP", "MSCI Singapore Index Futures"),
        ("TWN", "FTSE Taiwan Index Futures"),
        # FX Currency Futures
        ("UC",  "USD/CNH FX Futures (Offshore Chinese Yuan)"),
        ("IU",  "INR/USD FX Futures (Indian Rupee)"),
        ("KRW", "KRW/USD FX Futures (Korean Won)"),
    ]

    months = ['U', 'V', 'X', 'Z', 'F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q']
    now = datetime.now()
    cur_yr = str(now.year)[-2:]
    next_yr = str(now.year + 1)[-2:]

    live_contracts = []
    print(f"{'Code':<6} | {'Contract Name':<45} | {'Active Contract':<15} | {'Settlement':<12} | {'OI (Lots)':<10} | {'Volume':<8} | {'Date':<10}")
    print("-" * 115)

    for code, name in all_codes:
        found = False
        for yr in [cur_yr, next_yr]:
            if found: break
            for m in months:
                ticker = f"{code}{m}{yr}"
                url = f"https://api.sgx.com/derivatives/v1.0/history/symbol/{ticker}?days=5d&category=futures&params=base-date,daily-settlement-price-abs,total-volume,open-interest"
                try:
                    r = SESSION.get(url, timeout=5)
                    if r.status_code == 200:
                        data = r.json().get('data', [])
                        if data and len(data) > 0:
                            valid_rows = [row for row in data if row.get('daily-settlement-price-abs', 0) > 0 or row.get('open-interest', 0) > 0]
                            if valid_rows:
                                last = valid_rows[-1]
                                settle = last.get('daily-settlement-price-abs', 0)
                                oi = last.get('open-interest', 0)
                                vol = last.get('total-volume', 0)
                                bdate = last.get('base-date', '')
                                print(f"{code:<6} | {name:<45} | {ticker:<15} | {settle:>12.2f} | {oi:>10.0f} | {vol:>8.0f} | {bdate:<10}")
                                live_contracts.append({
                                    "code": code,
                                    "name": name,
                                    "ticker": ticker,
                                    "settlement": settle,
                                    "open_interest": oi,
                                    "volume": vol,
                                    "date": bdate
                                })
                                found = True
                                break
                except Exception:
                    pass
                time.sleep(0.03)

    return live_contracts

def save_report(results, live_contracts):
    report_path = "data/sgx_exhaustive_probe_report.json"
    data_to_save = {
        "timestamp": datetime.now().isoformat(),
        "total_endpoints_tested": len(results),
        "total_active_product_lines": len(live_contracts),
        "active_products": live_contracts,
        "endpoint_results": [
            {
                "url": r["url"],
                "status": r["status"],
                "content_type": r["content_type"],
                "fields": r["fields"],
                "sample": r["preview"]
            }
            for r in results if r["status"] == 200
        ]
    }
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(data_to_save, fh, indent=2)
    print("\n" + "=" * 85)
    print(f"EXHAUSTIVE PROBE REPORT SAVED: {report_path}")
    print("=" * 85)

if __name__ == "__main__":
    discovered = crawl_sgx_frontend_endpoints()
    results = probe_all_endpoints(discovered)
    live = deep_probe_all_derivative_products()
    save_report(results, live)
