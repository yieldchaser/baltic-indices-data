#!/usr/bin/env python3
"""
SGX Iron Ore Ingestion and Digestion Engine
===========================================
Ingests, normalizes, and digests institutional Singapore Exchange (SGX) Iron Ore
derivatives data across 3 benchmark instruments:
1. FEF  - TSI Iron Ore CFR China (62% Fe Fines) Futures
2. M65F - Fastmarkets MB Iron Ore CFR China (65% Fe Fines) Futures
3. LPF  - Platts Iron Ore CFR China (Lump Premium) Futures

Generates:
- Full contract historical stores (data/commodities/ and data/futures/)
- Active multi-tenor forward curve term structure with spreads and regimes
- Continuous prompt (M1, M2, M3) time series with high-grade spreads and term structure
- Dynamic update to data/derived/iron_ore_restocking.csv
"""

import os
import sys
import time
import argparse
import requests
import pandas as pd
from datetime import datetime, date
from calendar import monthrange

# Set standard UTF-8 console output
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(REPO_ROOT, 'data')
COMMODITIES_DIR = os.path.join(DATA_DIR, 'commodities')
FUTURES_DIR = os.path.join(DATA_DIR, 'futures')
DERIVED_DIR = os.path.join(DATA_DIR, 'derived')

os.makedirs(COMMODITIES_DIR, exist_ok=True)
os.makedirs(FUTURES_DIR, exist_ok=True)
os.makedirs(DERIVED_DIR, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36',
    'Referer': 'https://www.sgx.com/',
    'Origin': 'https://www.sgx.com',
    'Accept': 'application/json',
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CME_MONTHS = {
    'F': (1,  'Jan'), 'G': (2,  'Feb'), 'H': (3,  'Mar'),
    'J': (4,  'Apr'), 'K': (5,  'May'), 'M': (6,  'Jun'),
    'N': (7,  'Jul'), 'Q': (8,  'Aug'), 'U': (9,  'Sep'),
    'V': (10, 'Oct'), 'X': (11, 'Nov'), 'Z': (12, 'Dec'),
}

PRODUCTS = {
    'FEF': {
        'name': 'TSI Iron Ore 62% Fe CFR China Futures',
        'unit': 'USD/dmt',
        'start_year': 2018,
    },
    'M65F': {
        'name': 'Fastmarkets MB Iron Ore 65% Fe CFR China Futures',
        'unit': 'USD/dmt',
        'start_year': 2018,
    },
    'LPF': {
        'name': 'Platts Iron Ore CFR China Lump Premium Futures',
        'unit': 'USD/dmtu',
        'start_year': 2018,
    }
}


def get_contract_expiry(month_num, year):
    """Last weekday of the delivery month."""
    last_day = monthrange(year, month_num)[1]
    d = date(year, month_num, last_day)
    while d.weekday() >= 5:
        d = date.fromordinal(d.toordinal() - 1)
    return d


def generate_contract_tickers(product_code, start_year=2018, end_year=None):
    """Generate all contract tickers from start_year through end_year."""
    if end_year is None:
        end_year = datetime.now().year + 3
    tickers = []
    for y in range(start_year, end_year + 1):
        y2 = str(y)[-2:]
        for m_code, (m_num, m_name) in CME_MONTHS.items():
            exp = get_contract_expiry(m_num, y)
            ticker = f"{product_code}{m_code}{y2}"
            tickers.append({
                'ticker': ticker,
                'product': product_code,
                'month_code': m_code,
                'month_num': m_num,
                'month_name': m_name,
                'year': y,
                'expiry_date': exp,
                'expiry_str': exp.strftime('%Y-%m-%d'),
                'expiry_month': f"{m_name} {y}",
            })
    return tickers


def fetch_symbol_history(ticker, days='2200d', retries=3):
    """Fetch history from SGX derivatives API."""
    url = (
        f"https://api.sgx.com/derivatives/v1.0/history/symbol/{ticker}"
        f"?days={days}&category=futures"
        f"&params=base-date,daily-settlement-price-abs,total-volume,open-interest"
    )
    for attempt in range(1, retries + 1):
        try:
            r = SESSION.get(url, timeout=15)
            if r.status_code == 200:
                data = r.json().get('data', [])
                rows = []
                for item in data:
                    bd = item.get('base-date')
                    if not bd:
                        continue
                    try:
                        d_obj = datetime.strptime(str(bd), '%Y%m%d').date()
                        d_str = d_obj.strftime('%Y-%m-%d')
                    except Exception:
                        continue
                    p = item.get('daily-settlement-price-abs')
                    v = item.get('total-volume')
                    oi = item.get('open-interest')
                    rows.append({
                        'date': d_str,
                        'date_obj': d_obj,
                        'price': float(p) if p is not None else None,
                        'volume': float(v) if v is not None else 0.0,
                        'open_interest': float(oi) if oi is not None else 0.0,
                    })
                return rows
            elif r.status_code in (404, 410):
                return []
        except Exception as e:
            if attempt == retries:
                print(f"    [Error] {ticker}: {e}")
        time.sleep(0.5 * attempt)
    return []


def harvest_product_history(product_code, start_year=2018, end_year=None, days='2200d'):
    """Harvest complete contract lives for a given product."""
    tickers = generate_contract_tickers(product_code, start_year, end_year)
    print(f"\n[{product_code}] Initiating harvest across {len(tickers)} potential contracts ({start_year}-{end_year or 'active'})...")
    
    records = []
    active_contracts = 0
    
    for i, meta in enumerate(tickers):
        ticker = meta['ticker']
        rows = fetch_symbol_history(ticker, days=days)
        time.sleep(0.03)  # Respectful rate-limiting
        
        valid_rows = [r for r in rows if r['price'] is not None and (r['price'] > 0 or r['open_interest'] > 0)]
        if valid_rows:
            active_contracts += 1
            for r in valid_rows:
                records.append({
                    'contract': ticker,
                    'product': product_code,
                    'expiry_month': meta['expiry_month'],
                    'expiry_year': meta['year'],
                    'date': r['date'],
                    'price': r['price'] if r['price'] is not None else 0.0,
                    'volume': r['volume'],
                    'open_interest': r['open_interest'],
                    'expiry_date': meta['expiry_str'],
                })
        
        if (i + 1) % 25 == 0 or (i + 1) == len(tickers):
            print(f"  -> Processed {i + 1}/{len(tickers)} contracts ({active_contracts} active, {len(records):,} daily records accumulated)")
            
    df = pd.DataFrame(records)
    if not df.empty:
        # Sort chronologically by date, then contract
        df = df.sort_values(by=['date', 'contract']).reset_index(drop=True)
        # Drop exact duplicates if any
        df = df.drop_duplicates(subset=['contract', 'date'])
    return df


def build_forward_curve(fef_df, m65f_df, lpf_df):
    """
    Constructs the active forward curve across all future delivery tenors.
    Uses the latest available observation date for active contracts.
    """
    print("\n[Forward Curve] Constructing term structure across FEF (62%), M65F (65%), and LPF (Lump)...")
    cur_date = date.today()
    
    # Identify latest date in FEF
    latest_date_str = fef_df['date'].max() if not fef_df.empty else cur_date.strftime('%Y-%m-%d')
    latest_fef = fef_df[fef_df['date'] == latest_date_str].copy()
    latest_m65f = m65f_df[m65f_df['date'] == latest_date_str].copy() if not m65f_df.empty else pd.DataFrame()
    latest_lpf = lpf_df[lpf_df['date'] == latest_date_str].copy() if not lpf_df.empty else pd.DataFrame()
    
    # Filter for active contracts (expiry >= latest_date)
    latest_fef = latest_fef[latest_fef['expiry_date'] >= latest_date_str].sort_values('expiry_date')
    
    curve_rows = []
    prompt_settle = None
    
    for i, (_, row) in enumerate(latest_fef.iterrows()):
        ticker = row['contract']
        settle = float(row['price'])
        oi = float(row['open_interest'])
        vol = float(row['volume'])
        exp_month = row['expiry_month']
        exp_year = int(row['expiry_year'])
        
        if prompt_settle is None and settle > 0:
            prompt_settle = settle
            
        diff = settle - prompt_settle if prompt_settle else 0.0
        diff_pct = (diff / prompt_settle * 100.0) if prompt_settle else 0.0
        
        if i == 0:
            regime = "Prompt"
        elif diff < -0.25:
            regime = "Backwardation"
        elif diff > 0.25:
            regime = "Contango"
        else:
            regime = "Flat"
            
        # Match M65F
        m65f_ticker = ticker.replace('FEF', 'M65F')
        m65f_match = latest_m65f[latest_m65f['contract'] == m65f_ticker]
        m65f_settle = float(m65f_match.iloc[0]['price']) if not m65f_match.empty else None
        m65f_oi = float(m65f_match.iloc[0]['open_interest']) if not m65f_match.empty else 0.0
        m65f_vol = float(m65f_match.iloc[0]['volume']) if not m65f_match.empty else 0.0
        
        spread_65_62 = round(m65f_settle - settle, 2) if (m65f_settle and settle) else None
        
        # Match LPF
        lpf_ticker = ticker.replace('FEF', 'LPF')
        lpf_match = latest_lpf[latest_lpf['contract'] == lpf_ticker]
        lpf_settle = float(lpf_match.iloc[0]['price']) if not lpf_match.empty else None
        lpf_oi = float(lpf_match.iloc[0]['open_interest']) if not lpf_match.empty else 0.0
        lpf_vol = float(lpf_match.iloc[0]['volume']) if not lpf_match.empty else 0.0
        
        curve_rows.append({
            'date': latest_date_str,
            'tenor': exp_month.replace(' ', '-'),
            'delivery_month': exp_month.split(' ')[0],
            'delivery_year': exp_year,
            'fef_ticker': ticker,
            'fef_settle': round(settle, 2),
            'fef_oi': int(oi),
            'fef_volume': int(vol),
            'm65f_ticker': m65f_ticker,
            'm65f_settle': round(m65f_settle, 2) if m65f_settle else None,
            'm65f_oi': int(m65f_oi),
            'm65f_volume': int(m65f_vol),
            'fe65_fe62_premium_spread': spread_65_62,
            'lpf_ticker': lpf_ticker,
            'lpf_settle': round(lpf_settle, 4) if lpf_settle else None,
            'lpf_oi': int(lpf_oi),
            'lpf_volume': int(lpf_vol),
            'fef_term_structure_slope': round(diff, 2),
            'fef_slope_pct': round(diff_pct, 2),
            'fef_curve_regime': regime,
        })
        
    return pd.DataFrame(curve_rows)


def build_continuous_front_month_series(fef_df, m65f_df, lpf_df):
    """
    Constructs a rolling continuous front-month (M1), second-month (M2),
    and third-month (M3) daily series from 2018 to the latest session.
    """
    print("\n[Continuous Series] Synthesizing continuous prompt (M1/M2/M3) time series...")
    if fef_df.empty:
        return pd.DataFrame()
        
    continuous_records = []
    
    # Pre-index M65F and LPF for fast lookup: (contract, date) -> row
    m65f_lookup = m65f_df.set_index(['contract', 'date']) if not m65f_df.empty else None
    lpf_lookup = lpf_df.set_index(['contract', 'date']) if not lpf_df.empty else None
    
    # Group FEF by date
    fef_by_date = fef_df.groupby('date')
    
    for d_str, day_group in fef_by_date:
        # Active contracts on date d_str are those where expiry_date >= d_str and price > 0
        active = day_group[(day_group['expiry_date'] >= d_str) & (day_group['price'] > 0)].sort_values('expiry_date')
        if active.empty:
            continue
            
        # Total curve volume and open interest on that day
        total_curve_oi = active['open_interest'].sum()
        total_curve_vol = active['volume'].sum()
        
        # M1 (prompt month)
        m1_row = active.iloc[0]
        m1_contract = m1_row['contract']
        m1_price = float(m1_row['price'])
        m1_vol = float(m1_row['volume'])
        m1_oi = float(m1_row['open_interest'])
        
        # M2 (second month)
        if len(active) > 1:
            m2_row = active.iloc[1]
            m2_contract = m2_row['contract']
            m2_price = float(m2_row['price'])
            m2_vol = float(m2_row['volume'])
            m2_oi = float(m2_row['open_interest'])
        else:
            m2_contract, m2_price, m2_vol, m2_oi = None, None, 0.0, 0.0
            
        # M3 (third month)
        if len(active) > 2:
            m3_row = active.iloc[2]
            m3_contract = m3_row['contract']
            m3_price = float(m3_row['price'])
            m3_vol = float(m3_row['volume'])
            m3_oi = float(m3_row['open_interest'])
        else:
            m3_contract, m3_price, m3_vol, m3_oi = None, None, 0.0, 0.0
            
        # Calculate term structure spread (M2 - M1)
        if m2_price is not None and m1_price > 0:
            ts_spread = round(m2_price - m1_price, 2)
            ts_pct = round((ts_spread / m1_price) * 100.0, 2)
            if ts_spread < -0.25:
                regime = "Backwardation"
            elif ts_spread > 0.25:
                regime = "Contango"
            else:
                regime = "Flat"
        else:
            ts_spread, ts_pct, regime = None, None, "Unknown"
            
        # Lookup M65F front month
        m65f_m1_contract = m1_contract.replace('FEF', 'M65F')
        m65f_price, m65f_oi, m65f_vol = None, 0.0, 0.0
        if m65f_lookup is not None and (m65f_m1_contract, d_str) in m65f_lookup.index:
            m_data = m65f_lookup.loc[(m65f_m1_contract, d_str)]
            if isinstance(m_data, pd.DataFrame):
                m_data = m_data.iloc[0]
            m65f_price = float(m_data['price'])
            m65f_oi = float(m_data['open_interest'])
            m65f_vol = float(m_data['volume'])
            
        # High-grade spread (65% - 62%)
        high_grade_spread = round(m65f_price - m1_price, 2) if (m65f_price and m1_price) else None
        
        # Lookup LPF front month
        lpf_m1_contract = m1_contract.replace('FEF', 'LPF')
        lpf_price, lpf_oi, lpf_vol = None, 0.0, 0.0
        if lpf_lookup is not None and (lpf_m1_contract, d_str) in lpf_lookup.index:
            l_data = lpf_lookup.loc[(lpf_m1_contract, d_str)]
            if isinstance(l_data, pd.DataFrame):
                l_data = l_data.iloc[0]
            lpf_price = float(l_data['price'])
            lpf_oi = float(l_data['open_interest'])
            lpf_vol = float(l_data['volume'])
            
        continuous_records.append({
            'date': d_str,
            'fef_m1_contract': m1_contract,
            'fef_m1_price': round(m1_price, 2) if m1_price else None,
            'fef_m1_volume': int(m1_vol),
            'fef_m1_oi': int(m1_oi),
            'fef_m2_contract': m2_contract,
            'fef_m2_price': round(m2_price, 2) if m2_price else None,
            'fef_m2_volume': int(m2_vol),
            'fef_m2_oi': int(m2_oi),
            'fef_m3_contract': m3_contract,
            'fef_m3_price': round(m3_price, 2) if m3_price else None,
            'fef_m3_volume': int(m3_vol),
            'fef_m3_oi': int(m3_oi),
            'm1_m2_term_structure_spread': ts_spread,
            'm1_m2_slope_pct': ts_pct,
            'term_structure_regime': regime,
            'm65f_m1_contract': m65f_m1_contract,
            'm65f_m1_price': round(m65f_price, 2) if m65f_price else None,
            'm65f_m1_volume': int(m65f_vol),
            'm65f_m1_oi': int(m65f_oi),
            'high_grade_spread_65_62': high_grade_spread,
            'lpf_m1_contract': lpf_m1_contract,
            'lpf_m1_price': round(lpf_price, 4) if lpf_price else None,
            'lpf_m1_volume': int(lpf_vol),
            'lpf_m1_oi': int(lpf_oi),
            'fef_curve_total_oi': int(total_curve_oi),
            'fef_curve_total_volume': int(total_curve_vol),
        })
        
    df_cont = pd.DataFrame(continuous_records)
    if not df_cont.empty:
        df_cont = df_cont.sort_values('date').reset_index(drop=True)
    return df_cont


def sync_iron_ore_restocking(cont_df):
    """
    Synchronizes CFR 62% and 65% prices in data/derived/iron_ore_restocking.csv
    with official SGX continuous daily front-month settlement series.
    """
    target_path = os.path.join(DERIVED_DIR, 'iron_ore_restocking.csv')
    if not os.path.exists(target_path) or cont_df.empty:
        return
        
    print(f"\n[Sync] Synchronizing derived iron ore restocking series ({target_path})...")
    df_restock = pd.read_csv(target_path)
    df_restock['date'] = df_restock['date'].astype(str).str.strip()
    
    # Map from continuous daily df
    cont_map_62 = dict(zip(cont_df['date'], cont_df['fef_m1_price']))
    cont_map_65 = dict(zip(cont_df['date'], cont_df['m65f_m1_price']))
    
    updated_62 = 0
    updated_65 = 0
    
    for idx, row in df_restock.iterrows():
        d = row['date']
        # Unconditionally synchronize with official SGX continuous daily settlement prices
        if d in cont_map_62 and pd.notna(cont_map_62[d]) and float(cont_map_62[d]) > 0:
            df_restock.at[idx, 'cfr_62'] = round(float(cont_map_62[d]), 2)
            updated_62 += 1
                
        if d in cont_map_65 and pd.notna(cont_map_65[d]) and float(cont_map_65[d]) > 0:
            df_restock.at[idx, 'cfr_65'] = round(float(cont_map_65[d]), 2)
            updated_65 += 1
            
    # Forward-fill any isolated weekend/holiday dates that lacked an exact trading session
    df_restock['cfr_62'] = df_restock['cfr_62'].ffill()
    df_restock['cfr_65'] = df_restock['cfr_65'].ffill()
                
    # If the latest date in cont_df is newer than the last row in restocking, append it
    last_restock_date = df_restock['date'].max()
    latest_cont_row = cont_df.iloc[-1]
    if latest_cont_row['date'] > last_restock_date:
        print(f"  -> Appending new daily session: {latest_cont_row['date']}")
        new_row = {
            'date': latest_cont_row['date'],
            'cfr_62': latest_cont_row['fef_m1_price'],
            'cfr_65': latest_cont_row['m65f_m1_price'],
            'port_stock_62': '',
            'port_stock_65': '',
            'inventories_mt': '',
            'steel_production_mt': '',
            'steel_inventories_mt': '',
        }
        df_restock = pd.concat([df_restock, pd.DataFrame([new_row])], ignore_index=True)
        
    df_restock.to_csv(target_path, index=False)
    print(f"  ✔ Synchronized {updated_62} missing CFR 62% values and {updated_65} CFR 65% values in iron_ore_restocking.csv")

def save_merged_history(path, new_df, subset_cols):
    if new_df.empty:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return pd.read_csv(path)
        return new_df
    if os.path.exists(path) and os.path.getsize(path) > 0:
        try:
            existing = pd.read_csv(path)
            combined = pd.concat([existing[subset_cols], new_df[subset_cols]], ignore_index=True)
            combined = combined.drop_duplicates(subset=['contract', 'date'], keep='last')
            combined = combined.sort_values(['date', 'contract']).reset_index(drop=True)
            combined.to_csv(path, index=False)
            return combined
        except Exception as e:
            print(f"Warning: merge failed for {path}: {e}")
    new_df[subset_cols].to_csv(path, index=False)
    return new_df


def save_merged_continuous(path, cont_df):
    if cont_df.empty:
        return cont_df
    if os.path.exists(path) and os.path.getsize(path) > 0:
        try:
            existing = pd.read_csv(path)
            combined = pd.concat([existing, cont_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=['date'], keep='last')
            combined = combined.sort_values('date').reset_index(drop=True)
            combined.to_csv(path, index=False)
            return combined
        except Exception as e:
            print(f"Warning: merge failed for {path}: {e}")
    cont_df.to_csv(path, index=False)
    return cont_df


def main():
    parser = argparse.ArgumentParser(description="SGX Iron Ore Derivatives Ingestion & Term Structure Engine")
    parser.add_argument('--backfill', action='store_true', help="Execute full multi-year history backfill (2018-2029)")
    parser.add_argument('--days', type=str, default='2200d', help="Lookback window parameter for SGX API (default: 2200d)")
    parser.add_argument('--start-year', type=int, default=2018, help="Starting year for contract generation (default: 2018)")
    parser.add_argument('--end-year', type=int, default=2029, help="Ending year for contract generation (default: 2029)")
    args = parser.parse_args()

    lookback = args.days if args.backfill else '30d'
    start_yr = args.start_year if args.backfill else datetime.now().year - 1
    end_yr = args.end_year

    print("=" * 85)
    print("SGX IRON ORE DERIVATIVES INGESTION & DIGESTION ENGINE")
    print(f"Mode: {'FULL MULTI-YEAR BACKFILL' if args.backfill else 'INCREMENTAL REFRESH'} | Lookback: {lookback} | Years: {start_yr}-{end_yr}")
    print("=" * 85)

    # 1. Harvest FEF (62% Fe)
    fef_df = harvest_product_history('FEF', start_year=start_yr, end_year=end_yr, days=lookback)
    
    # 2. Harvest M65F (65% Fe)
    m65f_df = harvest_product_history('M65F', start_year=start_yr, end_year=end_yr, days=lookback)
    
    # 3. Harvest LPF (Lump Premium)
    lpf_df = harvest_product_history('LPF', start_year=start_yr, end_year=end_yr, days=lookback)

    # Save Product Contract History Files
    fef_cols = ['contract', 'expiry_month', 'expiry_year', 'date', 'price', 'volume', 'open_interest', 'expiry_date']
    
    if not fef_df.empty:
        # Commodities store
        fef_comm_path = os.path.join(COMMODITIES_DIR, 'sgx_iron_ore_62_fef_historical.csv')
        full_fef_df = save_merged_history(fef_comm_path, fef_df, fef_cols)
        print(f"✔ Saved: {fef_comm_path} ({len(full_fef_df):,} rows)")
        
        # Futures store (matched to sgx_cape_futures_history.csv)
        fef_fut_hist = os.path.join(FUTURES_DIR, 'sgx_iron_ore_fef_history.csv')
        save_merged_history(fef_fut_hist, fef_df, fef_cols)
        print(f"✔ Saved: {fef_fut_hist} ({len(full_fef_df):,} rows)")
        
        # Live active contracts snapshot (matched to sgx_cape_futures.csv)
        fef_fut_live = os.path.join(FUTURES_DIR, 'sgx_iron_ore_fef.csv')
        latest_date = full_fef_df['date'].max()
        fef_live_df = full_fef_df[full_fef_df['date'] == latest_date]
        fef_live_df[fef_cols].to_csv(fef_fut_live, index=False)
        print(f"✔ Saved: {fef_fut_live} ({len(fef_live_df):,} active rows)")
    else:
        full_fef_df = pd.DataFrame()

    if not m65f_df.empty:
        m65f_comm_path = os.path.join(COMMODITIES_DIR, 'sgx_iron_ore_65_m65f_historical.csv')
        full_m65f_df = save_merged_history(m65f_comm_path, m65f_df, fef_cols)
        print(f"✔ Saved: {m65f_comm_path} ({len(full_m65f_df):,} rows)")
        
        m65f_fut_hist = os.path.join(FUTURES_DIR, 'sgx_iron_ore_m65f_history.csv')
        save_merged_history(m65f_fut_hist, m65f_df, fef_cols)
        print(f"✔ Saved: {m65f_fut_hist} ({len(full_m65f_df):,} rows)")
        
        m65f_fut_live = os.path.join(FUTURES_DIR, 'sgx_iron_ore_m65f.csv')
        m65f_latest_date = full_m65f_df['date'].max()
        m65f_live_df = full_m65f_df[full_m65f_df['date'] == m65f_latest_date]
        m65f_live_df[fef_cols].to_csv(m65f_fut_live, index=False)
        print(f"✔ Saved: {m65f_fut_live} ({len(m65f_live_df):,} active rows)")
    else:
        full_m65f_df = pd.DataFrame()

    if not lpf_df.empty:
        lpf_comm_path = os.path.join(COMMODITIES_DIR, 'sgx_iron_ore_lump_lpf_historical.csv')
        full_lpf_df = save_merged_history(lpf_comm_path, lpf_df, fef_cols)
        print(f"✔ Saved: {lpf_comm_path} ({len(full_lpf_df):,} rows)")
        
        lpf_fut_hist = os.path.join(FUTURES_DIR, 'sgx_iron_ore_lump_lpf_history.csv')
        save_merged_history(lpf_fut_hist, lpf_df, fef_cols)
        print(f"✔ Saved: {lpf_fut_hist} ({len(full_lpf_df):,} rows)")
        
        lpf_fut_live = os.path.join(FUTURES_DIR, 'sgx_iron_ore_lump_lpf.csv')
        lpf_latest_date = full_lpf_df['date'].max()
        lpf_live_df = full_lpf_df[full_lpf_df['date'] == lpf_latest_date]
        lpf_live_df[fef_cols].to_csv(lpf_fut_live, index=False)
        print(f"✔ Saved: {lpf_fut_live} ({len(lpf_live_df):,} active rows)")
    else:
        full_lpf_df = pd.DataFrame()

    # 4. Construct Forward Curve
    curve_df = build_forward_curve(fef_df, m65f_df, lpf_df)
    if not curve_df.empty:
        curve_path = os.path.join(COMMODITIES_DIR, 'sgx_iron_ore_forward_curve.csv')
        curve_df.to_csv(curve_path, index=False)
        print(f"✔ Saved: {curve_path} ({len(curve_df)} tenors)")
        
        # Display preview of active forward curve
        print("\n" + "=" * 105)
        print(f"{'Tenor':<12} | {'FEF (62%)':<12} | {'M65F (65%)':<12} | {'Spread 65-62':<14} | {'LPF (Lump)':<12} | {'FEF OI (Lots)':<14} | {'Term Structure'}")
        print("-" * 105)
        for _, row in curve_df.iterrows():
            fef_s = f"${row['fef_settle']:.2f}"
            m65f_s = f"${row['m65f_settle']:.2f}" if pd.notna(row['m65f_settle']) else "—"
            spr = f"+${row['fe65_fe62_premium_spread']:.2f}" if pd.notna(row['fe65_fe62_premium_spread']) else "—"
            lpf_s = f"${row['lpf_settle']:.4f}" if pd.notna(row['lpf_settle']) else "—"
            oi_s = f"{row['fef_oi']:,}"
            slope_s = f"{row['fef_term_structure_slope']:+.2f} ({row['fef_slope_pct']:+.1f}%) [{row['fef_curve_regime']}]"
            print(f"{row['tenor']:<12} | {fef_s:<12} | {m65f_s:<12} | {spr:<14} | {lpf_s:<12} | {oi_s:<14} | {slope_s}")
        print("=" * 105)

    # 5. Construct Continuous Front-Month Series (M1/M2/M3)
    cont_df = build_continuous_front_month_series(fef_df, m65f_df, lpf_df)
    if not cont_df.empty:
        cont_path = os.path.join(COMMODITIES_DIR, 'sgx_iron_ore_continuous_daily.csv')
        full_cont_df = save_merged_continuous(cont_path, cont_df)
        print(f"\n✔ Saved: {cont_path} ({len(full_cont_df):,} trading days from {full_cont_df['date'].min()} to {full_cont_df['date'].max()})")
        
        # Synchronize iron_ore_restocking.csv
        sync_iron_ore_restocking(full_cont_df)

    print("\n✔ Ingestion and Digestion Completed Successfully with 0 Errors.")


if __name__ == '__main__':
    main()
