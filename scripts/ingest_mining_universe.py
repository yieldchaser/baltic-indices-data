"""
ingest_mining_universe.py
Production Ingestion Pipeline for Big Mining & Bulk Commodity Giants.
Maps and stores SEC EDGAR filings, XBRL financials, insider transactions,
and foreign financial metrics for the world's primary seaborne dry bulk cargo producers.
"""

import os
import sys
import time
import pandas as pd
from edgar import set_identity, Company
import yfinance as yf

# SEC User-Agent Identity
set_identity("Prateek Upadhyay upadhyayprateek574@gmail.com")

DATA_DIR = "data/equities"
CATALOG_PATH = os.path.join(DATA_DIR, "maritime_universe_catalog.csv")

MINING_UNIVERSE = [
    # SEC-Registered Mining & Agri-Bulk Giants
    {"user_ticker": "VALE", "target_symbol": "VALE", "sector": "Mining / Iron Ore", "company_name": "Vale S.A.", "is_sec": True},
    {"user_ticker": "RIO", "target_symbol": "RIO", "sector": "Mining / Diversified & Iron Ore", "company_name": "Rio Tinto plc", "is_sec": True},
    {"user_ticker": "BHP", "target_symbol": "BHP", "sector": "Mining / Diversified & Iron Ore", "company_name": "BHP Group Limited", "is_sec": True},
    {"user_ticker": "TECK", "target_symbol": "TECK", "sector": "Mining / Met Coal & Copper", "company_name": "Teck Resources Limited", "is_sec": True},
    {"user_ticker": "SID", "target_symbol": "SID", "sector": "Mining & Steel / Iron Ore", "company_name": "Companhia Siderurgica Nacional", "is_sec": True},
    {"user_ticker": "FCX", "target_symbol": "FCX", "sector": "Mining / Copper & Gold", "company_name": "Freeport-McMoRan Inc.", "is_sec": True},
    {"user_ticker": "AA", "target_symbol": "AA", "sector": "Mining & Smelting / Bauxite", "company_name": "Alcoa Corporation", "is_sec": True},
    {"user_ticker": "MT", "target_symbol": "MT", "sector": "Mining & Steel / Capesize Charterer", "company_name": "ArcelorMittal", "is_sec": True},
    {"user_ticker": "CLF", "target_symbol": "CLF", "sector": "Mining & Steel / Great Lakes Bulk", "company_name": "Cleveland-Cliffs Inc.", "is_sec": True},
    {"user_ticker": "BTU", "target_symbol": "BTU", "sector": "Mining / Coal Exporter", "company_name": "Peabody Energy Corporation", "is_sec": True},
    {"user_ticker": "HCC", "target_symbol": "HCC", "sector": "Mining / Met Coal Exporter", "company_name": "Warrior Met Coal, Inc.", "is_sec": True},
    {"user_ticker": "AMR", "target_symbol": "AMR", "sector": "Mining / Met Coal Exporter", "company_name": "Alpha Metallurgical Resources, Inc.", "is_sec": True},
    {"user_ticker": "ARCH", "target_symbol": "0001037676", "sector": "Mining / Met Coal Exporter", "company_name": "Arch Resources, Inc.", "is_sec": True},
    {"user_ticker": "ADM", "target_symbol": "ADM", "sector": "Agri-Bulk / Grain Charterer", "company_name": "Archer-Daniels-Midland Company", "is_sec": True},
    {"user_ticker": "BG", "target_symbol": "BG", "sector": "Agri-Bulk / Grain Charterer", "company_name": "Bunge Global SA", "is_sec": True},
    # Foreign-Listed Mining Giants
    {"user_ticker": "FMG", "target_symbol": "FMG.AX", "sector": "Mining / Iron Ore", "company_name": "Fortescue Ltd", "is_sec": False},
    {"user_ticker": "GLEN", "target_symbol": "GLEN.L", "sector": "Mining & Trading / Coal & Freight", "company_name": "Glencore plc", "is_sec": False},
    {"user_ticker": "AAL", "target_symbol": "AAL.L", "sector": "Mining / Iron Ore & Met Coal", "company_name": "Anglo American plc", "is_sec": False},
    {"user_ticker": "S32", "target_symbol": "S32.AX", "sector": "Mining / Manganese, Alumina & Coal", "company_name": "South32 Limited", "is_sec": False},
    {"user_ticker": "WHC", "target_symbol": "WHC.AX", "sector": "Mining / Coal Exporter", "company_name": "Whitehaven Coal Limited", "is_sec": False},
    {"user_ticker": "YAL", "target_symbol": "YAL.AX", "sector": "Mining / Coal Exporter", "company_name": "Yancoal Australia Limited", "is_sec": False}
]

def update_catalog():
    print("=== 1. Updating Master Universe Catalog ===")
    df_cat = pd.read_csv(CATALOG_PATH)
    existing_tickers = set(df_cat['user_ticker'].dropna().tolist())
    
    new_rows = []
    for item in MINING_UNIVERSE:
        if item['user_ticker'] not in existing_tickers:
            new_rows.append(item)
            existing_tickers.add(item['user_ticker'])
            
    if new_rows:
        df_new = pd.DataFrame(new_rows)
        df_combined = pd.concat([df_cat, df_new], ignore_index=True)
        df_combined.to_csv(CATALOG_PATH, index=False)
        print(f"[+] Added {len(new_rows)} mining & cargo giants to {CATALOG_PATH} (Total: {len(df_combined)})")
    else:
        print("[i] All mining giants already in catalog.")

def ingest_sec_miners():
    print("\n=== 2. Ingesting SEC EDGAR Filings & Financials for Mining Giants ===")
    sec_miners = [m for m in MINING_UNIVERSE if m['is_sec']]
    
    path_cat_parquet = os.path.join(DATA_DIR, "sec_master_filing_catalog.parquet")
    path_fin_parquet = os.path.join(DATA_DIR, "sec_xbrl_financials.parquet")
    path_f4_parquet = os.path.join(DATA_DIR, "sec_form4_insider_trades.parquet")
    path_ex99_parquet = os.path.join(DATA_DIR, "sec_exhibit99_announcements.parquet")
    
    df_cat_exist = pd.read_parquet(path_cat_parquet) if os.path.exists(path_cat_parquet) else pd.DataFrame()
    df_fin_exist = pd.read_parquet(path_fin_parquet) if os.path.exists(path_fin_parquet) else pd.DataFrame()
    df_f4_exist = pd.read_parquet(path_f4_parquet) if os.path.exists(path_f4_parquet) else pd.DataFrame()
    df_ex99_exist = pd.read_parquet(path_ex99_parquet) if os.path.exists(path_ex99_parquet) else pd.DataFrame()
    
    new_filings = []
    new_financials = []
    new_form4 = []
    new_ex99 = []
    
    for idx, item in enumerate(sec_miners):
        u_ticker = item['user_ticker']
        sym = item['target_symbol']
        name = item['company_name']
        sector = item['sector']
        print(f"[{idx+1}/{len(sec_miners)}] Harvesting SEC Data for {u_ticker} ({sym}) - {name}...")
        
        try:
            c = Company(sym)
        except Exception as e:
            print(f"  [!] Failed Company({sym}): {e}")
            continue
            
        # 1. Historical Filing Catalog
        try:
            filings = c.get_filings()
            print(f"  -> Found {len(filings)} historical filings")
            for f in filings:
                new_filings.append({
                    "user_ticker": u_ticker,
                    "sec_symbol": sym,
                    "cik": c.cik,
                    "company_name": c.name,
                    "sector": sector,
                    "form": f.form,
                    "filing_date": str(f.filing_date),
                    "accession_no": f.accession_no,
                    "document_url": f.homepage_url if hasattr(f, "homepage_url") else ""
                })
        except Exception as e:
            print(f"  [!] Error fetching filing catalog: {e}")
            
        # 2. Form 4 Insider Trades (latest 15)
        try:
            f4 = c.get_filings(form="4").latest(15)
            if f4:
                f4_list = [f4] if hasattr(f4, "form") else f4
                for f in f4_list:
                    try:
                        obj = f.obj()
                        new_form4.append({
                            "user_ticker": u_ticker,
                            "sec_symbol": sym,
                            "company_name": c.name,
                            "filing_date": str(f.filing_date),
                            "accession_no": f.accession_no,
                            "reporting_owner": getattr(obj, "reporting_owner", "N/A"),
                            "form": "4"
                        })
                    except:
                        pass
        except:
            pass

        # 3. Form 6-K / 8-K Commercial Announcements
        try:
            cf = c.get_filings(form=["6-K", "8-K"]).latest(8)
            if cf:
                cf_list = [cf] if hasattr(cf, "form") else cf
                for f in cf_list:
                    try:
                        ann_desc = ""
                        if hasattr(f, "attachments"):
                            for att in f.attachments:
                                if "99" in str(att.document_type) or "EX-99" in str(att.document):
                                    ann_desc = att.description or att.document
                                    break
                        new_ex99.append({
                            "user_ticker": u_ticker,
                            "sec_symbol": sym,
                            "company_name": c.name,
                            "form": f.form,
                            "filing_date": str(f.filing_date),
                            "accession_no": f.accession_no,
                            "announcement_desc": ann_desc or f.form
                        })
                    except:
                        pass
        except:
            pass

        # 4. Structured XBRL Financials
        try:
            fin = c.get_financials()
            if fin:
                for stmt_type, getter in [("income_statement", fin.income_statement), 
                                          ("balance_sheet", fin.balance_sheet), 
                                          ("cash_flow", fin.cash_flow)]:
                    try:
                        s_df = getter()
                        if s_df is not None and hasattr(s_df, "to_dataframe"):
                            s_data = s_df.to_dataframe().reset_index()
                            s_data["user_ticker"] = u_ticker
                            s_data["sec_symbol"] = sym
                            s_data["statement"] = stmt_type
                            new_financials.append(s_data)
                    except:
                        pass
        except:
            pass

        time.sleep(0.2)
        
    # Append & deduplicate Master Filings
    if new_filings:
        df_new_f = pd.DataFrame(new_filings)
        df_cat_comb = pd.concat([df_cat_exist, df_new_f], ignore_index=True).drop_duplicates(subset=["accession_no", "user_ticker"])
        df_cat_comb.to_parquet(path_cat_parquet, index=False)
        df_cat_comb.to_csv(os.path.join(DATA_DIR, "sec_master_filing_catalog.csv"), index=False)
        print(f"[+] Updated SEC Filings Catalog: {len(df_cat_comb)} total records (+{len(df_new_f)} new)")

    # Append & deduplicate Form 4
    if new_form4:
        df_new_f4 = pd.DataFrame(new_form4)
        df_f4_comb = pd.concat([df_f4_exist, df_new_f4], ignore_index=True).drop_duplicates(subset=["accession_no", "user_ticker"])
        df_f4_comb.to_parquet(path_f4_parquet, index=False)
        df_f4_comb.to_csv(os.path.join(DATA_DIR, "sec_form4_insider_trades.csv"), index=False)
        print(f"[+] Updated Form 4 Insider Trades: {len(df_f4_comb)} total records (+{len(df_new_f4)} new)")

    # Append & deduplicate Exhibits
    if new_ex99:
        df_new_ex = pd.DataFrame(new_ex99)
        df_ex_comb = pd.concat([df_ex99_exist, df_new_ex], ignore_index=True).drop_duplicates(subset=["accession_no", "user_ticker"])
        df_ex_comb.to_parquet(path_ex99_parquet, index=False)
        df_ex_comb.to_csv(os.path.join(DATA_DIR, "sec_exhibit99_announcements.csv"), index=False)
        print(f"[+] Updated Exhibit 99 Announcements: {len(df_ex_comb)} total records (+{len(df_new_ex)} new)")

    # Append Structured Financials
    if new_financials:
        df_new_fin = pd.concat(new_financials, ignore_index=True)
        df_fin_comb = pd.concat([df_fin_exist, df_new_fin], ignore_index=True)
        df_fin_comb.to_parquet(path_fin_parquet, index=False)
        df_fin_comb.to_csv(os.path.join(DATA_DIR, "sec_xbrl_financials.csv"), index=False)
        print(f"[+] Updated XBRL Financial Statements: {len(df_fin_comb)} total rows (+{len(df_new_fin)} new)")

def ingest_foreign_miners():
    print("\n=== 3. Ingesting Foreign Market Metrics & Financials for Mining Giants ===")
    foreign_miners = [m for m in MINING_UNIVERSE if not m['is_sec']]
    
    path_met_parquet = os.path.join(DATA_DIR, "foreign_maritime_metrics.parquet")
    path_fin_parquet = os.path.join(DATA_DIR, "foreign_maritime_financials.parquet")
    
    df_met_exist = pd.read_parquet(path_met_parquet) if os.path.exists(path_met_parquet) else pd.DataFrame()
    df_fin_exist = pd.read_parquet(path_fin_parquet) if os.path.exists(path_fin_parquet) else pd.DataFrame()
    
    new_metrics = []
    new_financials = []
    
    for idx, item in enumerate(foreign_miners):
        u_ticker = item['user_ticker']
        sym = item['target_symbol']
        name = item['company_name']
        sector = item['sector']
        print(f"[{idx+1}/{len(foreign_miners)}] Fetching {u_ticker} ({sym}) - {name}...")
        
        try:
            t = yf.Ticker(sym)
            info = t.info or {}
            
            new_metrics.append({
                "user_ticker": u_ticker,
                "symbol": sym,
                "name": info.get("shortName", name),
                "sector": sector,
                "currency": info.get("currency", "N/A"),
                "market_cap": info.get("marketCap"),
                "enterprise_value": info.get("enterpriseValue"),
                "trailing_pe": info.get("trailingPE"),
                "forward_pe": info.get("forwardPE"),
                "price_to_book": info.get("priceToBook"),
                "dividend_yield": info.get("dividendYield"),
                "52w_high": info.get("fiftyTwoWeekHigh"),
                "52w_low": info.get("fiftyTwoWeekLow"),
                "shares_outstanding": info.get("sharesOutstanding"),
                "exchange": info.get("exchange", "N/A")
            })
            
            # Financial statements
            for stmt_name, stmt_obj in [("income_statement", t.financials), 
                                        ("balance_sheet", t.balance_sheet), 
                                        ("cash_flow", t.cashflow)]:
                try:
                    if stmt_obj is not None and not stmt_obj.empty:
                        tidy = stmt_obj.reset_index().rename(columns={"index": "metric"})
                        melted = tidy.melt(id_vars=["metric"], var_name="period_date", value_name="value")
                        melted["user_ticker"] = u_ticker
                        melted["symbol"] = sym
                        melted["statement"] = stmt_name
                        new_financials.append(melted)
                except:
                    pass
        except Exception as e:
            print(f"  [!] Failed for {sym}: {e}")
            
        time.sleep(0.2)
        
    if new_metrics:
        df_new_met = pd.DataFrame(new_metrics)
        df_met_comb = pd.concat([df_met_exist, df_new_met], ignore_index=True).drop_duplicates(subset=["user_ticker", "symbol"], keep="last")
        df_met_comb.to_parquet(path_met_parquet, index=False)
        df_met_comb.to_csv(os.path.join(DATA_DIR, "foreign_maritime_metrics.csv"), index=False)
        print(f"[+] Updated Foreign Metrics: {len(df_met_comb)} total records (+{len(df_new_met)} new)")

    if new_financials:
        df_new_fin = pd.concat(new_financials, ignore_index=True)
        df_fin_comb = pd.concat([df_fin_exist, df_new_fin], ignore_index=True)
        df_fin_comb.to_parquet(path_fin_parquet, index=False)
        df_fin_comb.to_csv(os.path.join(DATA_DIR, "foreign_maritime_financials.csv"), index=False)
        print(f"[+] Updated Foreign Financials: {len(df_fin_comb)} total rows (+{len(df_new_fin)} new)")

if __name__ == "__main__":
    update_catalog()
    ingest_sec_miners()
    ingest_foreign_miners()
    print("\n=== ALL MINING GIANTS INGESTION COMPLETED SUCCESSFULLY ===")
