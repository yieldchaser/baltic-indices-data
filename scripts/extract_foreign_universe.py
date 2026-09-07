import os
import time
import pandas as pd
import yfinance as yf

CATALOG_PATH = "data/equities/maritime_universe_catalog.csv"
OUT_DIR = "data/equities"
os.makedirs(OUT_DIR, exist_ok=True)

df_cat = pd.read_csv(CATALOG_PATH)
foreign_list = df_cat[df_cat['is_sec'] == False].dropna(subset=['target_symbol']).copy()
print(f"Total Foreign Companies to Ingest: {len(foreign_list)}")

all_metrics = []
all_financials = []

for idx, (_, row) in enumerate(foreign_list.iterrows()):
    u_ticker = row['user_ticker']
    sym = row['target_symbol']
    name = row['company_name']
    sector = row['sector']
    
    print(f"[{idx+1}/{len(foreign_list)}] Fetching {u_ticker} ({sym}) - {name}...")
    try:
        t = yf.Ticker(sym)
        info = t.info or {}
        
        # 1. Capture key metrics
        all_metrics.append({
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
        
        # 2. Capture financials (Income Statement)
        try:
            fin = t.financials
            if fin is not None and not fin.empty:
                fin_tidy = fin.reset_index().rename(columns={"index": "metric"})
                fin_melted = fin_tidy.melt(id_vars=["metric"], var_name="period_date", value_name="value")
                fin_melted["user_ticker"] = u_ticker
                fin_melted["symbol"] = sym
                fin_melted["statement"] = "income_statement"
                all_financials.append(fin_melted)
        except Exception:
            pass

        # 3. Capture Balance Sheet
        try:
            bs = t.balance_sheet
            if bs is not None and not bs.empty:
                bs_tidy = bs.reset_index().rename(columns={"index": "metric"})
                bs_melted = bs_tidy.melt(id_vars=["metric"], var_name="period_date", value_name="value")
                bs_melted["user_ticker"] = u_ticker
                bs_melted["symbol"] = sym
                bs_melted["statement"] = "balance_sheet"
                all_financials.append(bs_melted)
        except Exception:
            pass

        # 4. Capture Cash Flow
        try:
            cf = t.cashflow
            if cf is not None and not cf.empty:
                cf_tidy = cf.reset_index().rename(columns={"index": "metric"})
                cf_melted = cf_tidy.melt(id_vars=["metric"], var_name="period_date", value_name="value")
                cf_melted["user_ticker"] = u_ticker
                cf_melted["symbol"] = sym
                cf_melted["statement"] = "cash_flow"
                all_financials.append(cf_melted)
        except Exception:
            pass

    except Exception as e:
        print(f"  [!] Failed for {sym}: {e}")
        all_metrics.append({
            "user_ticker": u_ticker,
            "symbol": sym,
            "name": name,
            "sector": sector,
            "currency": "ERROR",
            "market_cap": None
        })

    # Polite pacing
    time.sleep(0.15)

# Save metrics
df_metrics = pd.DataFrame(all_metrics)
df_metrics.to_csv(os.path.join(OUT_DIR, "foreign_maritime_metrics.csv"), index=False)
df_metrics.to_parquet(os.path.join(OUT_DIR, "foreign_maritime_metrics.parquet"), index=False)
print(f"[+] Successfully saved foreign metrics to {OUT_DIR}/foreign_maritime_metrics.parquet ({len(df_metrics)} records)")

# Save financials
if all_financials:
    df_fin_all = pd.concat(all_financials, ignore_index=True)
    df_fin_all.to_csv(os.path.join(OUT_DIR, "foreign_maritime_financials.csv"), index=False)
    df_fin_all.to_parquet(os.path.join(OUT_DIR, "foreign_maritime_financials.parquet"), index=False)
    print(f"[+] Successfully saved foreign financials to {OUT_DIR}/foreign_maritime_financials.parquet ({len(df_fin_all)} rows)")
