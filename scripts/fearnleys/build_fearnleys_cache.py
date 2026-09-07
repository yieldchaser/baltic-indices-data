"""
build_fearnleys_cache.py
Extracts, aggregates, and precomputes institutional analytics from Fearnleys Hasura datasets:
  - 56-Year Time Charter Benchmarks (1970–2026) across Dry Bulk & Tankers
  - 50-Year Secondhand Vessel Asset Valuations (1976–2026) vs Newbuilding Prices
  - Commercial Fixture Analytics & Top Charterers League Table (537k fixtures)
  - Secondhand Vessel Sale & Purchase (S&P) Deal Stream (2,592 deals)
  - Qualitative Broker Desk Sentiment Feed (11.7k notes)

Outputs a compact, highly optimized JSON file:
  data/derived/fearnleys_summary.json (~250 KB)
Ensuring sub-50ms browser rendering without loading multi-megabyte CSVs.
"""

import json
import os
import sys
import time
from datetime import datetime
import numpy as np
import pandas as pd

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DERIVED_DIR = os.path.join(BASE_DIR, "data", "derived")
OUTPUT_JSON = os.path.join(DERIVED_DIR, "fearnleys_summary.json")

RATES_CSV = os.path.join(DERIVED_DIR, "fearnpulse_rates_full.csv")
FIXTURES_PARQUET = os.path.join(DERIVED_DIR, "fearnleys_fixtures_full.parquet")
FIXTURES_CSV = os.path.join(DERIVED_DIR, "fearnleys_fixtures_full.csv")
SNP_CSV = os.path.join(DERIVED_DIR, "fearnleys_snp_transactions.csv")
COMMENTS_CSV = os.path.join(DERIVED_DIR, "fearnleys_broker_comments.csv")


def sanitize_float(val, decimals=2):
    if pd.isna(val) or np.isinf(val):
        return None
    return round(float(val), decimals)


def build_tce_benchmarks(df_rates):
    """Aggregates 56 years of 1Y TC rates (1970-2026) across key segments."""
    print("  Aggregating 56-Year 1Y TC benchmark series...", flush=True)

    tce_specs = [
        {"key": "capesize", "label": "Capesize 1Y", "type": "BULK", "subtype": "TC", "routes": ["Capesize (182 000 dwt)", "Capesize (180 000 dwt)"]},
        {"key": "panamax", "label": "Panamax 1Y", "type": "BULK", "subtype": "TC", "routes": ["Panamax (75 000 dwt)"]},
        {"key": "supramax", "label": "Supramax 1Y", "type": "BULK", "subtype": "TC", "routes": ["Supramax (58 000 dwt)"]},
        {"key": "handysize", "label": "Handysize 1Y", "type": "BULK", "subtype": "TC", "routes": ["Handysize (38 000 dwt)"]},
        {"key": "kamsarmax", "label": "Kamsarmax 1Y", "type": "BULK", "subtype": "TC", "routes": ["Kamsarmax (82 000 dwt)"]},
        {"key": "vlcc", "label": "VLCC 1Y", "type": "TANK", "subtype": "1 Year T/C", "routes": ["VLCC"]},
        {"key": "suezmax", "label": "Suezmax 1Y", "type": "TANK", "subtype": "1 Year T/C", "routes": ["Suezmax"]},
        {"key": "aframax", "label": "Aframax 1Y", "type": "TANK", "subtype": "1 Year T/C", "routes": ["Aframax"]},
    ]

    series_dfs = {}
    stats_dict = {}

    for spec in tce_specs:
        mask = (
            (df_rates["rate_type"] == spec["type"])
            & (df_rates["rate_subtype"] == spec["subtype"])
            & (df_rates["route"].isin(spec["routes"]))
        )
        sub = df_rates[mask].copy()
        if sub.empty:
            continue

        sub["date"] = pd.to_datetime(sub["date"], errors="coerce")
        sub = sub.dropna(subset=["date", "rate"])
        sub = sub.sort_values("date").drop_duplicates(subset=["date"], keep="last")

        rates = sub["rate"].values
        current_rate = float(rates[-1]) if len(rates) > 0 else 0
        all_time_high = float(rates.max()) if len(rates) > 0 else 0
        all_time_low = float(rates.min()) if len(rates) > 0 else 0

        # Calculate historical percentile of current rate across full 56Y history
        if len(rates) > 0 and all_time_high > all_time_low:
            pct_56y = float((rates < current_rate).mean() * 100.0)
        else:
            pct_56y = 50.0

        # Calculate 5Y average (last 260 weekly prints)
        last_5y = rates[-260:] if len(rates) >= 260 else rates
        avg_5y = float(last_5y.mean()) if len(last_5y) > 0 else current_rate

        stats_dict[spec["key"]] = {
            "label": spec["label"],
            "current": sanitize_float(current_rate, 0),
            "ath": sanitize_float(all_time_high, 0),
            "ath_date": str(sub.loc[sub["rate"].idxmax(), "date"].date()) if len(sub) > 0 else None,
            "atl": sanitize_float(all_time_low, 0),
            "atl_date": str(sub.loc[sub["rate"].idxmin(), "date"].date()) if len(sub) > 0 else None,
            "pct_56y": sanitize_float(pct_56y, 1),
            "avg_5y": sanitize_float(avg_5y, 0),
            "start_year": int(sub["date"].min().year),
            "count": len(sub),
        }

        # Resample to monthly close to keep JSON lightweight
        sub.set_index("date", inplace=True)
        monthly = sub["rate"].resample("ME").last().dropna()
        series_dfs[spec["key"]] = monthly

    # Merge monthly series
    monthly_merged = pd.DataFrame(series_dfs).sort_index()

    timeline = []
    for dt, row in monthly_merged.iterrows():
        entry = {"date": dt.strftime("%Y-%m")}
        has_val = False
        for k in series_dfs.keys():
            val = row.get(k)
            if pd.notna(val):
                entry[k] = int(round(val))
                has_val = True
            else:
                entry[k] = None
        if has_val:
            timeline.append(entry)

    return {"stats": stats_dict, "monthly_series": timeline}


def build_asset_valuations(df_rates):
    """Aggregates 50 years of secondhand asset valuations & newbuilding prices (1976-2026)."""
    print("  Aggregating 50-Year Secondhand Asset Valuations vs Newbuilds...", flush=True)

    segments = ["Capesize", "Kamsarmax", "Ultramax", "Handysize", "VLCC", "Suezmax", "Aframax / LR2", "MR"]

    # Map segment to Newbuild route
    nb_route_map = {
        "Capesize": "Newcastlemax",
        "Kamsarmax": "Kamsarmax",
        "Ultramax": "Ultramax",
        "Handysize": None,
        "VLCC": "VLCC",
        "Suezmax": "Suezmax",
        "Aframax / LR2": "Aframax",
        "MR": "Product",
    }

    results = {}

    for seg in segments:
        seg_key = seg.lower().replace(" / lr2", "").replace(" ", "_")

        tiers = {}
        # 5-Year Old
        sub_5y = df_rates[
            (df_rates["rate_type"] == "S&P")
            & (df_rates["rate_subtype"].isin(["DRY-5", "WET-5"]))
            & (df_rates["route"] == seg)
        ].copy()

        # 10-Year Old
        sub_10y = df_rates[
            (df_rates["rate_type"] == "S&P")
            & (df_rates["rate_subtype"].isin(["DRY-10", "WET-10"]))
            & (df_rates["route"] == seg)
        ].copy()

        # Resale
        sub_resale = df_rates[
            (df_rates["rate_type"] == "S&P")
            & (df_rates["rate_subtype"].isin(["DRY-RESALE", "WET-RESALE"]))
            & (df_rates["route"] == seg)
        ].copy()

        # Newbuilding
        nb_route = nb_route_map.get(seg)
        sub_nb = df_rates[
            (df_rates["rate_type"] == "NEWBUILDING")
            & (df_rates["rate_subtype"] == "PRICES")
            & (df_rates["route"] == nb_route)
        ].copy() if nb_route else pd.DataFrame()

        tier_map = {"5y": sub_5y, "10y": sub_10y, "resale": sub_resale, "newbuilding": sub_nb}
        monthly_tiers = {}
        curr_stats = {}

        for tname, tdf in tier_map.items():
            if tdf.empty:
                continue
            tdf["date"] = pd.to_datetime(tdf["date"], errors="coerce")
            tdf = tdf.dropna(subset=["date", "rate"]).sort_values("date")
            rates = tdf["rate"].values
            curr_val = float(rates[-1]) if len(rates) > 0 else 0
            ath = float(rates.max()) if len(rates) > 0 else 0
            atl = float(rates.min()) if len(rates) > 0 else 0
            pct_hist = float((rates < curr_val).mean() * 100.0) if len(rates) > 0 and ath > atl else 50.0

            curr_stats[tname] = {
                "current": sanitize_float(curr_val, 1),
                "ath": sanitize_float(ath, 1),
                "atl": sanitize_float(atl, 1),
                "pct_hist": sanitize_float(pct_hist, 1),
                "count": len(rates),
                "start_year": int(tdf["date"].min().year),
            }

            tdf.set_index("date", inplace=True)
            m = tdf["rate"].resample("QE").last().dropna()  # quarterly resample for asset values
            monthly_tiers[tname] = m

        if monthly_tiers:
            merged_seg = pd.DataFrame(monthly_tiers).sort_index()
            history = []
            for dt, row in merged_seg.iterrows():
                h_item = {"date": f"{dt.year}-Q{dt.quarter}" if hasattr(dt, 'quarter') else dt.strftime("%Y-%m")}
                h_item["date_str"] = dt.strftime("%Y-%m")
                for col in ["resale", "5y", "10y", "newbuilding"]:
                    val = row.get(col)
                    h_item[col] = sanitize_float(val, 1) if pd.notna(val) else None
                history.append(h_item)

            results[seg_key] = {
                "label": seg,
                "stats": curr_stats,
                "history": history,
            }

    return results


def build_fixture_analytics():
    """Aggregates 537,164 fixtures into sector throughput, top charterers, and recent stream."""
    print("  Aggregating Commercial Fixture Analytics & Charterer League Table...", flush=True)

    if os.path.exists(FIXTURES_PARQUET):
        df = pd.read_parquet(FIXTURES_PARQUET)
    elif os.path.exists(FIXTURES_CSV):
        df = pd.read_csv(FIXTURES_CSV, low_memory=False)
    else:
        print("    [WARN] No fixtures file found!")
        return {}

    total_fixtures = len(df)
    dept_counts = df["department"].value_counts().to_dict()

    # Normalize dates
    df["dt"] = pd.to_datetime(df["date"], errors="coerce")
    valid_dt = df.dropna(subset=["dt"])

    # Annual fixture count & sector distribution (1980-2026)
    valid_dt["year"] = valid_dt["dt"].dt.year
    annual_df = valid_dt[(valid_dt["year"] >= 1980) & (valid_dt["year"] <= 2026)]
    annual_counts = annual_df.groupby(["year", "department"]).size().unstack(fill_value=0)

    annual_volume = []
    for yr, row in annual_counts.iterrows():
        annual_volume.append({
            "year": int(yr),
            "bulk": int(row.get("BULK", 0)),
            "tank": int(row.get("TANK", 0) + row.get("TANKPRO", 0)),
            "gas": int(row.get("LNG", 0) + row.get("LPG", 0)),
            "total": int(row.sum()),
        })

    # Top 25 Charterers League Table
    # Filter out empty or 'TBN' or 'Undisclosed'
    clean_chart = df[df["charterer"].notna() & (df["charterer"].str.strip() != "") & (~df["charterer"].str.upper().isin(["TBN", "UNKNOWN", "UNDISCLOSED", "PRIVATE"]))].copy()
    clean_chart["charterer_clean"] = clean_chart["charterer"].str.strip().str.upper()

    top_ch = clean_chart["charterer_clean"].value_counts().head(30)
    league_table = []

    for rank, (charterer, count) in enumerate(top_ch.items(), start=1):
        c_rows = clean_chart[clean_chart["charterer_clean"] == charterer]
        top_seg = c_rows["segment"].value_counts().index[0] if not c_rows["segment"].dropna().empty else "Various"
        top_route = c_rows["route"].value_counts().index[0] if not c_rows["route"].dropna().empty else "Global"
        
        # Calculate market share %
        share_pct = (count / total_fixtures) * 100.0

        league_table.append({
            "rank": rank,
            "charterer": charterer,
            "fixtures": int(count),
            "market_share_pct": sanitize_float(share_pct, 2),
            "primary_segment": str(top_seg),
            "primary_route": str(top_route)[:45],
            "dept": str(c_rows["department"].value_counts().index[0]) if not c_rows["department"].dropna().empty else "BULK",
        })

    # Recent 100 commercial fixtures
    recent = valid_dt.sort_values("dt", ascending=False).head(100)
    recent_fixtures = []
    for _, r in recent.iterrows():
        recent_fixtures.append({
            "date": str(r["dt"].date()) if pd.notna(r["dt"]) else str(r.get("date")),
            "vessel": str(r.get("vessel") or "TBN").strip().upper(),
            "segment": str(r.get("segment") or "Various").strip(),
            "charterer": str(r.get("charterer") or "Undisclosed").strip().upper(),
            "route": str(r.get("route") or "N/A").strip(),
            "rate": str(r.get("rate") or "-").strip(),
            "period": str(r.get("period") or "").strip(),
            "load_port": str(r.get("load_port") or "").strip(),
            "discharge_port": str(r.get("discharge_port") or "").strip(),
            "comment": str(r.get("comment") or "")[:120].strip(),
        })

    return {
        "total_fixtures": total_fixtures,
        "department_totals": {str(k): int(v) for k, v in dept_counts.items()},
        "annual_volume": annual_volume,
        "top_charterers": league_table[:25],
        "recent_fixtures": recent_fixtures,
    }


def build_snp_deals():
    """Extracts 2,592 S&P transaction deals, computing deal aggregates and recent stream."""
    print("  Processing Secondhand S&P Deals Ledger...", flush=True)

    if not os.path.exists(SNP_CSV):
        print("    [WARN] No S&P CSV found!")
        return {}

    df = pd.read_csv(SNP_CSV)
    total_deals = len(df)
    
    # Clean prices
    df["price_num"] = pd.to_numeric(df["price"], errors="coerce")
    df["dwt_num"] = pd.to_numeric(df["dwt"], errors="coerce")
    df["built_num"] = pd.to_numeric(df["built"], errors="coerce")

    # Segment breakdowns
    seg_counts = df["segment"].value_counts().to_dict()
    total_volume_usd_m = df["price_num"].sum()

    # Recent 150 deals
    df["created_dt"] = pd.to_datetime(df["created_at"], errors="coerce")
    sorted_deals = df.sort_values("created_dt", ascending=False).head(150)

    deal_stream = []
    for _, r in sorted_deals.iterrows():
        price = sanitize_float(r["price_num"], 1)
        dwt = int(r["dwt_num"]) if pd.notna(r["dwt_num"]) and r["dwt_num"] > 0 else None
        
        # Compute implied $/DWT
        usd_per_dwt = None
        if price and dwt and dwt > 0:
            usd_per_dwt = sanitize_float((price * 1_000_000) / dwt, 0)

        buyer_raw = r.get("buyer")
        buyer_clean = str(buyer_raw).strip() if pd.notna(buyer_raw) and str(buyer_raw).strip().lower() != "nan" else "Undisclosed"

        deal_stream.append({
            "id": str(r["id"]) if pd.notna(r["id"]) else "",
            "date": str(r["created_dt"].date()) if pd.notna(r["created_dt"]) else "",
            "vessel": str(r.get("vessel") or "TBN").strip().upper(),
            "built": int(r["built_num"]) if pd.notna(r["built_num"]) and r["built_num"] > 1900 else None,
            "yard": str(r.get("yard") or "N/A").strip(),
            "dwt": dwt,
            "segment": str(r.get("segment") or "bulk").strip().lower(),
            "price_usd_m": price,
            "usd_per_dwt": usd_per_dwt,
            "buyer": buyer_clean,
            "comment": str(r.get("comment") or "")[:120].strip(),
        })

    return {
        "total_deals": total_deals,
        "total_volume_usd_m": sanitize_float(total_volume_usd_m, 1),
        "segments": {str(k): int(v) for k, v in seg_counts.items()},
        "recent_deals": deal_stream,
    }


def build_broker_sentiment():
    """Extracts recent broker commentary notes."""
    print("  Processing Broker Commentary Feed...", flush=True)

    if not os.path.exists(COMMENTS_CSV):
        return {}

    df = pd.read_csv(COMMENTS_CSV)
    total_comments = len(df)

    df["dt"] = pd.to_datetime(df["date"], errors="coerce")
    recent = df.sort_values("dt", ascending=False).head(40)

    comments = []
    for _, r in recent.iterrows():
        comments.append({
            "date": str(r["dt"].date()) if pd.notna(r["dt"]) else "",
            "type": str(r.get("comment_type") or "Market").strip(),
            "subtype": str(r.get("comment_subtype") or "").strip(),
            "name": str(r.get("comment_name") or "Weekly Color").strip(),
            "text": str(r.get("text") or "")[:280].strip(),
        })

    return {
        "total_comments": total_comments,
        "recent_comments": comments,
    }


def main():
    print("================================================================", flush=True)
    print("  FEARNLEYS INSTITUTIONAL SUMMARY CACHE BUILDER                 ", flush=True)
    print("================================================================", flush=True)

    start_time = time.time()

    print("Loading FearnPulse rates full CSV...", flush=True)
    if not os.path.exists(RATES_CSV):
        print(f"Error: {RATES_CSV} not found!", flush=True)
        sys.exit(1)

    df_rates = pd.read_csv(RATES_CSV, low_memory=False)

    tce_data = build_tce_benchmarks(df_rates)
    asset_data = build_asset_valuations(df_rates)
    fixtures_data = build_fixture_analytics()
    snp_data = build_snp_deals()
    sentiment_data = build_broker_sentiment()

    summary_payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "provenance": {
            "source": "Fearnleys Hasura GraphQL Backend (pbrokerapp.hasura.app)",
            "rate_records": len(df_rates),
            "fixture_records": fixtures_data.get("total_fixtures", 0),
            "snp_records": snp_data.get("total_deals", 0),
            "comment_records": sentiment_data.get("total_comments", 0),
        },
        "tce_benchmarks_56y": tce_data,
        "asset_valuations_50y": asset_data,
        "fixtures_analytics": fixtures_data,
        "snp_deals": snp_data,
        "broker_sentiment": sentiment_data,
    }

    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(summary_payload, f, ensure_ascii=False, indent=2)

    elapsed = time.time() - start_time
    file_size_kb = os.path.getsize(OUTPUT_JSON) / 1024.0

    print(f"\nSuccessfully generated {OUTPUT_JSON}")
    print(f"File size: {file_size_kb:.1f} KB")
    print(f"Execution time: {elapsed:.2f} seconds")
    print("================================================================\n", flush=True)


if __name__ == "__main__":
    main()
