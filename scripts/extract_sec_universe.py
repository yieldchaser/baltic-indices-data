"""
extract_sec_universe.py
Production SEC EDGAR Extraction Pipeline for Maritime Equities & Shipping ETFs.
Harvests:
1. Master Historical Filing Catalog (all forms back to inception)
2. XBRL Financial Statements (Income Statement, Balance Sheet, Cash Flow)
3. Form 4 Insider Trades (Tycoon & Executive Skin-in-the-Game)
4. Form 6-K & 8-K Exhibit 99 Commercial & Fleet Announcements
"""

import os
import sys
import time
import pandas as pd
from datetime import datetime
from edgar import set_identity, Company

# Set compliant SEC User-Agent identity
set_identity("Prateek Upadhyay upadhyayprateek574@gmail.com")

CATALOG_PATH = "data/equities/maritime_universe_catalog.csv"
OUT_DIR = "data/equities"
os.makedirs(OUT_DIR, exist_ok=True)

df_cat = pd.read_csv(CATALOG_PATH)
sec_list = df_cat[df_cat['is_sec'] == True].dropna(subset=['target_symbol']).copy()
print(f"Total SEC Companies to Ingest: {len(sec_list)}")

master_filings = []
financial_records = []
insider_trades = []
commercial_announcements = []

for idx, (_, row) in enumerate(sec_list.iterrows()):
    u_ticker = row['user_ticker']
    sym = row['target_symbol']
    name = row['company_name']
    sector = row['sector']
    
    print(f"[{idx+1}/{len(sec_list)}] Harvesting SEC Data for {u_ticker} ({sym}) - {name}...")
    try:
        c = Company(sym)
    except Exception as e:
        print(f"  [!] Failed to initialize Company({sym}): {e}")
        continue

    # 1. Ingest Master Filing Catalog (Historical Index)
    try:
        filings = c.get_filings()
        print(f"  -> Found {len(filings)} total historical filings")
        for f in filings:
            master_filings.append({
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
        print(f"  [!] Error fetching filing catalog for {sym}: {e}")

    # 2. Ingest Form 4 Insider Trades
    try:
        form4_filings = c.get_filings(form="4").latest(15)
        if form4_filings:
            # Handle if single filing or list
            f4_list = [form4_filings] if hasattr(form4_filings, "form") else form4_filings
            for f in f4_list:
                try:
                    obj = f.obj()
                    insider_trades.append({
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
    except Exception as e:
        pass

    # 3. Ingest Form 6-K / 8-K Commercial Announcements (Exhibit 99)
    try:
        curr_filings = c.get_filings(form=["6-K", "8-K"]).latest(8)
        if curr_filings:
            cf_list = [curr_filings] if hasattr(curr_filings, "form") else curr_filings
            for f in cf_list:
                try:
                    ann_desc = ""
                    if hasattr(f, "attachments"):
                        for att in f.attachments:
                            if "99" in str(att.document_type) or "EX-99" in str(att.document):
                                ann_desc = att.description or att.document
                                break
                    commercial_announcements.append({
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
    except Exception as e:
        pass

    # 4. Ingest Structured XBRL Financial Statements
    try:
        financials = c.get_financials()
        if financials:
            # Income Statement
            try:
                inc = financials.income_statement()
                if inc is not None and hasattr(inc, "to_dataframe"):
                    df_inc = inc.to_dataframe().reset_index()
                    df_inc["user_ticker"] = u_ticker
                    df_inc["sec_symbol"] = sym
                    df_inc["statement"] = "income_statement"
                    financial_records.append(df_inc)
            except:
                pass
            # Balance Sheet
            try:
                bs = financials.balance_sheet()
                if bs is not None and hasattr(bs, "to_dataframe"):
                    df_bs = bs.to_dataframe().reset_index()
                    df_bs["user_ticker"] = u_ticker
                    df_bs["sec_symbol"] = sym
                    df_bs["statement"] = "balance_sheet"
                    financial_records.append(df_bs)
            except:
                pass
            # Cash Flow
            try:
                cf = financials.cash_flow()
                if cf is not None and hasattr(cf, "to_dataframe"):
                    df_cf = cf.to_dataframe().reset_index()
                    df_cf["user_ticker"] = u_ticker
                    df_cf["sec_symbol"] = sym
                    df_cf["statement"] = "cash_flow"
                    financial_records.append(df_cf)
            except:
                pass
    except Exception as e:
        pass

    # Polite pacing
    time.sleep(0.2)

# Save Master Filing Catalog
if master_filings:
    df_mf = pd.DataFrame(master_filings)
    df_mf.to_parquet(os.path.join(OUT_DIR, "sec_master_filing_catalog.parquet"), index=False)
    df_mf.to_csv(os.path.join(OUT_DIR, "sec_master_filing_catalog.csv"), index=False)
    print(f"[+] Saved {len(df_mf)} filing catalog records to {OUT_DIR}/sec_master_filing_catalog.parquet")

# Save Insider Trades
if insider_trades:
    df_it = pd.DataFrame(insider_trades)
    df_it.to_parquet(os.path.join(OUT_DIR, "sec_form4_insider_trades.parquet"), index=False)
    df_it.to_csv(os.path.join(OUT_DIR, "sec_form4_insider_trades.csv"), index=False)
    print(f"[+] Saved {len(df_it)} Form 4 insider records to {OUT_DIR}/sec_form4_insider_trades.parquet")

# Save Commercial Announcements
if commercial_announcements:
    df_ca = pd.DataFrame(commercial_announcements)
    df_ca.to_parquet(os.path.join(OUT_DIR, "sec_exhibit99_announcements.parquet"), index=False)
    df_ca.to_csv(os.path.join(OUT_DIR, "sec_exhibit99_announcements.csv"), index=False)
    print(f"[+] Saved {len(df_ca)} commercial announcement records to {OUT_DIR}/sec_exhibit99_announcements.parquet")

# Save Structured Financials
if financial_records:
    df_all_fin = pd.concat(financial_records, ignore_index=True)
    df_all_fin.to_parquet(os.path.join(OUT_DIR, "sec_xbrl_financials.parquet"), index=False)
    df_all_fin.to_csv(os.path.join(OUT_DIR, "sec_xbrl_financials.csv"), index=False)
    print(f"[+] Saved {len(df_all_fin)} structured XBRL financial rows to {OUT_DIR}/sec_xbrl_financials.parquet")

print("=== SEC EXTRACTION COMPLETED ===")
