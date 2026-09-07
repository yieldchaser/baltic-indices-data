"""
sec_edgar_pipeline.py
Production SEC EDGAR Filing Pipeline for Maritime Equities & Shipping ETFs.
Powered by EdgarTools (https://github.com/dgunning/edgartools).

User Identity: Prateek Upadhyay (upadhyayprateek574@gmail.com)

Supports:
- Breakwave ETFs: BDRY (Dry Bulk), BWET (Tankers), BOAT / SEA
- Major Dry Bulk: SBLK, GNK, EGLE, DSX, PANL, CTRM
- Major Crude & Product Tankers: FRO, STNG, INSW, TRMD, DHT, TNK, NAT, HSHP
- Container & Gas Carriers: ZIM, DAC, FLNG, GLNG
- Automated Form 10-K / 10-Q / 20-F / 6-K filing downloads
- Financial Statements extraction (Income Statement, Balance Sheet, Cash Flows)
- Form 4 Insider Ownership & Transactions
- Material 8-K / 6-K Contract & Earnings Announcements
"""

import argparse
import os
import sys
import json
import pandas as pd
from datetime import datetime

try:
    from edgar import set_identity, Company, get_filings, Filing
except ImportError:
    print("Error: edgartools not installed. Run: pip install edgartools")
    sys.exit(1)

# Default SEC identity for compliance
DEFAULT_IDENTITY = "Prateek Upadhyay upadhyayprateek574@gmail.com"

# Predefined shipping universe
SHIPPING_UNIVERSE = {
    "ETFS": ["BDRY", "BWET"],
    "DRY_BULK": ["SBLK", "GNK", "DSX", "PANL", "CTRM"],
    "TANKERS": ["FRO", "STNG", "INSW", "TRMD", "DHT", "TNK", "NAT", "HSHP"],
    "CONTAINER_GAS": ["ZIM", "DAC", "FLNG", "GLNG"]
}

def init_edgar(identity: str = DEFAULT_IDENTITY):
    """Set identity for SEC EDGAR rate limit compliance."""
    set_identity(identity)
    return identity

def get_company_filings(ticker: str, forms=None, limit: int = 10):
    """Retrieve recent filings for a specific company or ETF ticker."""
    init_edgar()
    try:
        company = Company(ticker)
    except Exception as e:
        print(f"[!] Error locating company for ticker '{ticker}': {e}")
        return None
    
    if forms:
        filings = company.get_filings(form=forms)
    else:
        filings = company.get_filings()
    
    if limit and len(filings) > limit:
        return filings.latest(limit)
    return filings

def get_financial_statements(ticker: str, statement_type: str = "income_statement"):
    """
    Extract standardized financial statements from XBRL.
    statement_type options: 'income_statement', 'balance_sheet', 'cash_flow'
    """
    init_edgar()
    company = Company(ticker)
    try:
        financials = company.get_financials()
        if not financials:
            print(f"[-] No XBRL financials available for {ticker}")
            return None
        
        if statement_type == "income_statement":
            return financials.income_statement()
        elif statement_type == "balance_sheet":
            return financials.balance_sheet()
        elif statement_type == "cash_flow":
            return financials.cash_flow()
        else:
            return financials
    except Exception as e:
        print(f"[!] Error parsing financials for {ticker}: {e}")
        return None

def get_insider_trades(ticker: str, limit: int = 10):
    """Retrieve Form 4 insider transactions for corporate insiders."""
    init_edgar()
    company = Company(ticker)
    try:
        form4_filings = company.get_filings(form="4").latest(limit)
        trades = []
        for f in form4_filings:
            obj = f.obj()
            trades.append({
                "filing_date": f.filing_date,
                "accession_no": f.accession_no,
                "reporting_owner": getattr(obj, "reporting_owner", "N/A"),
                "form": "4"
            })
        return trades
    except Exception as e:
        print(f"[!] Error retrieving insider trades for {ticker}: {e}")
        return None

def sync_shipping_etfs(output_dir: str = "data/etf/sec_filings"):
    """
    Sync recent SEC 10-Q, 8-K, and Annual filings for BDRY & BWET.
    Saves metadata catalog to output_dir.
    """
    init_edgar()
    os.makedirs(output_dir, exist_ok=True)
    catalog = []
    
    for ticker in SHIPPING_UNIVERSE["ETFS"]:
        print(f"[*] Syncing SEC filings for {ticker}...")
        try:
            company = Company(ticker)
            filings = company.get_filings(form=["10-K", "10-Q", "8-K", "424B3"]).latest(15)
            for f in filings:
                catalog.append({
                    "ticker": ticker,
                    "company_name": company.name,
                    "cik": company.cik,
                    "form": f.form,
                    "filing_date": str(f.filing_date),
                    "accession_no": f.accession_no,
                    "document_url": f.homepage_url if hasattr(f, "homepage_url") else ""
                })
        except Exception as e:
            print(f"[!] Error fetching {ticker}: {e}")
            
    df = pd.DataFrame(catalog)
    out_csv = os.path.join(output_dir, "shipping_etf_sec_filings.csv")
    df.to_csv(out_csv, index=False)
    print(f"[+] Saved {len(df)} filings to {out_csv}")
    return df

def scan_shipping_universe(output_file: str = "data/indices/sec_shipping_universe_status.csv"):
    """Scan the entire shipping equity and ETF universe for latest filings."""
    init_edgar()
    results = []
    
    for category, tickers in SHIPPING_UNIVERSE.items():
        for ticker in tickers:
            try:
                company = Company(ticker)
                filing = company.get_filings().latest()
                latest_form = filing.form if filing else "N/A"
                latest_date = str(filing.filing_date) if filing else "N/A"
                results.append({
                    "category": category,
                    "ticker": ticker,
                    "cik": company.cik,
                    "name": company.name,
                    "latest_form": latest_form,
                    "latest_filing_date": latest_date
                })
                print(f"  [OK] {ticker:6} | CIK: {str(company.cik):8} | Form: {latest_form:6} | Date: {latest_date} | {company.name[:30]}")
            except Exception as e:
                print(f"  [FAIL] {ticker}: {e}")
                results.append({
                    "category": category,
                    "ticker": ticker,
                    "cik": "N/A",
                    "name": "N/A",
                    "latest_form": "ERROR",
                    "latest_filing_date": str(e)
                })
                
    df = pd.DataFrame(results)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"[+] Shipping universe SEC status saved to {output_file}")
    return df

def main():
    parser = argparse.ArgumentParser(description="SEC EDGAR Maritime Shipping Pipeline")
    parser.add_argument("--ticker", type=str, help="Stock or ETF ticker (e.g. BDRY, FRO, ZIM)")
    parser.add_argument("--form", type=str, nargs="+", help="Filing form filter (e.g. 10-Q 10-K 8-K 20-F 6-K 4)")
    parser.add_argument("--limit", type=int, default=5, help="Number of filings to return")
    parser.add_argument("--financials", action="store_true", help="Fetch XBRL financial statement")
    parser.add_argument("--statement", type=str, default="income_statement", choices=["income_statement", "balance_sheet", "cash_flow"])
    parser.add_argument("--sync-etfs", action="store_true", help="Sync BDRY and BWET ETF SEC filings")
    parser.add_argument("--scan-universe", action="store_true", help="Scan full maritime universe (Dry Bulk, Tankers, Containers)")
    parser.add_argument("--identity", type=str, default=DEFAULT_IDENTITY, help="SEC identity email string")
    
    args = parser.parse_args()
    init_edgar(args.identity)
    
    if args.sync_etfs:
        sync_shipping_etfs()
    elif args.scan_universe:
        scan_shipping_universe()
    elif args.ticker:
        if args.financials:
            print(f"[*] Fetching {args.statement} for {args.ticker}...")
            stmt = get_financial_statements(args.ticker, args.statement)
            if stmt is not None:
                print(stmt)
        else:
            print(f"[*] Fetching filings for {args.ticker} (Forms: {args.form or 'All'}, Limit: {args.limit})...")
            filings = get_company_filings(args.ticker, forms=args.form, limit=args.limit)
            if filings:
                for idx, f in enumerate(filings):
                    print(f"  [{idx+1}] Form: {f.form:8} | Date: {f.filing_date} | Accession: {f.accession_no}")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
