#!/usr/bin/env python3
"""
SGX Derivatives Full Product Universe Discovery & Analysis Tool
Systematically sweeps Singapore Exchange (SGX) product codes across:
- Dry Bulk & Tanker Freight (FFAs)
- Iron Ore, Steel, Metals & Minerals
- Bunker Fuels, Crude Oil & LNG
- Petrochemicals & Polymers
- Rubber, Agriculture & Soft Commodities
- FX Futures & Macro Crosses
- Equity Index Derivatives (China A50, Nifty, Nikkei, etc.)
"""

import sys
import json
import time
import requests
from datetime import datetime

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://www.sgx.com/',
    'Origin': 'https://www.sgx.com',
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# CME Month Codes
CME_MONTHS = {
    'F': 'Jan', 'G': 'Feb', 'H': 'Mar', 'J': 'Apr',
    'K': 'May', 'M': 'Jun', 'N': 'Jul', 'Q': 'Aug',
    'U': 'Sep', 'V': 'Oct', 'X': 'Nov', 'Z': 'Dec'
}

# Candidate Codes Matrix
PRODUCT_CATALOG = {
    "Freight & Shipping FFAs": [
        ("CWF", "Capesize Freight 5TC Futures", "$/day"),
        ("PWF", "Panamax Freight 4TC/5TC Futures", "$/day"),
        ("SWF", "Supramax Freight 10TC Futures", "$/day"),
        ("HWF", "Handysize Freight 7TC Futures", "$/day"),
        ("TC2", "Tanker TC2 FFA (Continent to USAC 37k)", "WS / $/t"),
        ("TC5", "Tanker TC5 FFA (Middle East to Japan 55k)", "WS / $/t"),
        ("TD3", "Tanker TD3C FFA (MEG to China VLCC 270k)", "WS / $/t"),
        ("TC12", "Tanker TC12 FFA (West Coast India to Japan)", "WS / $/t"),
        ("TC15", "Tanker TC15 FFA (Med to Far East)", "$/day"),
        ("TC17", "Tanker TC17 FFA", "WS"),
        ("TD20", "Tanker TD20 FFA (WAF to UKC Suezmax 130k)", "WS"),
    ],
    "Iron Ore, Steel & Ferrous Metals": [
        ("FEF", "SGX TSI Iron Ore CFR China (62% Fe Fines) Futures", "$/dmt"),
        ("FEM", "SGX Fastmarkets Iron Ore CFR China (65% Fe Fines) Futures", "$/dmt"),
        ("FOL", "SGX TSI Iron Ore CFR China (62% Fe) Lump Premium Futures", "$/dmt"),
        ("FES", "SGX Platts Iron Ore CFR China (58% Fe Fines) Futures", "$/dmt"),
        ("HRB", "SGX Steel Rebar CFR China Futures", "$/MT"),
        ("HRC", "SGX Steel Hot Rolled Coil (HRC) FOB China Futures", "$/MT"),
        ("SCR", "SGX Scrap Metal Futures", "$/MT"),
        ("IO",  "SGX Iron Ore Fines", "$/dmt"),
        ("FE",  "SGX Iron Ore Index", "$/dmt"),
        ("IOC", "SGX Iron Ore Options Underlying", "$/dmt"),
    ],
    "Bunker Fuels, Crude & Energy": [
        ("MOF", "SGX Platts Marine Fuel 0.5% FOB Singapore (VLSFO) Futures", "$/MT"),
        ("FOF", "SGX Platts Fuel Oil 380cst FOB Singapore (HSFO) Futures", "$/MT"),
        ("FO1", "SGX Platts Fuel Oil 180cst FOB Singapore Futures", "$/MT"),
        ("MO1", "SGX Marine Fuel 0.5% vs 380cst Hi-5 Fuel Spread", "$/MT"),
        ("DBF", "SGX Platts Dubai Crude Oil Index Futures", "$/bbl"),
        ("LGF", "SGX LNG Platts JKM (Japan Korea Marker) Futures", "$/MMBtu"),
        ("EFW", "SGX Singapore Baseload Electricity Futures", "$/MWh"),
        ("EFP", "SGX Electricity Peak Futures", "$/MWh"),
    ],
    "Petrochemicals & Chemicals": [
        ("MEG", "SGX ICIS CFR China Monoethylene Glycol Futures", "$/MT"),
        ("PXF", "SGX ICIS CFR Taiwan/China Paraxylene Futures", "$/MT"),
        ("BZF", "SGX ICIS FOB Korea Benzene Futures", "$/MT"),
        ("MTF", "SGX ICIS CFR China Methanol Futures", "$/MT"),
        ("PTA", "SGX Purified Terephthalic Acid Futures", "$/MT"),
        ("LDF", "SGX Linear Low Density Polyethylene (LLDPE) Futures", "$/MT"),
        ("PPF", "SGX Polypropylene Futures", "$/MT"),
    ],
    "Rubber & Agriculture": [
        ("TF",  "SGX SICOM TSR20 Technically Specified Rubber Futures", "US cents/kg"),
        ("RSS", "SGX SICOM RSS3 Ribbed Smoked Sheet Rubber Futures", "US cents/kg"),
        ("CPO", "SGX Crude Palm Oil Futures", "$/MT"),
        ("DAP", "SGX Diammonium Phosphate Fertilizer Futures", "$/MT"),
        ("URE", "SGX Urea Fertilizer Futures", "$/MT"),
    ],
    "Equity Indexes & Macro Financials": [
        ("CN",  "FTSE China A50 Index Futures", "Points"),
        ("IN",  "GIFT Nifty 50 Index Futures", "Points"),
        ("NK",  "Nikkei 225 Index Futures", "Points"),
        ("SG",  "MSCI Singapore Index Futures", "Points"),
        ("TW",  "FTSE Taiwan Index Futures", "Points"),
        ("VID", "SGX USD/CNH FX Futures", "CNH per USD"),
        ("UC",  "SGX USD/CNH FX Futures (UC)", "CNH per USD"),
        ("IU",  "SGX INR/USD FX Futures", "USD per INR"),
        ("KRW", "SGX KRW/USD FX Futures", "USD per KRW"),
        ("SGD", "SGX SGD/USD FX Futures", "USD per SGD"),
    ]
}

def probe_catalog():
    print("=" * 85)
    print("SGX DERIVATIVES UNIVERSE: ACTIVE PRODUCTS DISCOVERY")
    print("=" * 85)

    now = datetime.now()
    cur_year_str = str(now.year)[-2:]
    next_year_str = str(now.year + 1)[-2:]
    
    # Months to probe: Sep 2026, Oct 2026, Nov 2026, Dec 2026, Jan 2027, Dec 2027
    test_contracts = [
        ('U', cur_year_str),  # Sep 2026
        ('V', cur_year_str),  # Oct 2026
        ('X', cur_year_str),  # Nov 2026
        ('Z', cur_year_str),  # Dec 2026
        ('F', next_year_str), # Jan 2027
        ('Z', next_year_str), # Dec 2027
    ]

    active_matrix = []

    for category, products in PRODUCT_CATALOG.items():
        print(f"\n--- {category.upper()} ---")
        for p_code, p_name, unit in products:
            hit = False
            best_ticker = None
            best_data = None
            
            for m_code, y_str in test_contracts:
                ticker = f"{p_code}{m_code}{y_str}"
                url = (
                    f"https://api.sgx.com/derivatives/v1.0/history/symbol/{ticker}"
                    f"?days=5d&category=futures"
                    f"&params=base-date%2Ctotal-volume%2Cdaily-settlement-price-abs%2Copen-interest"
                )
                try:
                    r = SESSION.get(url, timeout=10)
                    if r.status_code == 200 and r.json().get('meta', {}).get('code') == '200':
                        data = r.json().get('data', [])
                        if data and len(data) > 0:
                            # Filter out dummy 0 price if other days have real settlement
                            valid_days = [d for d in data if d.get('daily-settlement-price-abs', 0) > 0]
                            if valid_days or data[0].get('open-interest', 0) > 0:
                                hit = True
                                best_ticker = ticker
                                best_data = valid_days[-1] if valid_days else data[-1]
                                break
                except Exception:
                    pass
                time.sleep(0.04)

            if hit and best_data:
                settle = best_data.get('daily-settlement-price-abs', 0)
                vol = best_data.get('total-volume', 0)
                oi = best_data.get('open-interest', 0)
                bdate = best_data.get('base-date', '')
                print(f"✔ [ACTIVE] {p_code:<6} | {p_name:<50} | Ticker: {best_ticker:<9} | Settle: {settle:>10.2f} {unit:<9} | Vol: {vol:>6.0f} | OI: {oi:>8.0f} | Date: {bdate}")
                active_matrix.append({
                    "category": category,
                    "code": p_code,
                    "name": p_name,
                    "unit": unit,
                    "sample_ticker": best_ticker,
                    "settle": settle,
                    "vol": vol,
                    "oi": oi,
                    "date": bdate
                })
            else:
                print(f"✖ [INACTIVE/OTHER] {p_code:<6} | {p_name}")

    print("\n" + "=" * 85)
    print("SUMMARY OF DISCOVERED SGX ACTIVE COMMODITY & FINANCIAL DERIVATIVE PRODUCTS")
    print("=" * 85)
    print(f"Total active products identified: {len(active_matrix)}")
    for item in active_matrix:
        print(f" • [{item['category']}] {item['code']} ({item['name']}): Latest Settle {item['settle']:.2f} {item['unit']} (OI: {item['oi']:,.0f} lots)")

if __name__ == "__main__":
    probe_catalog()
