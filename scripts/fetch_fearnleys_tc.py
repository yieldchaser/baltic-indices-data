"""Fetch Fearnleys TC rates from Hasura GraphQL API and save to CSV."""
import requests
import pandas as pd
import os
import sys

URL = "https://pbrokerapp.hasura.app/v1/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://fearnpulse.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126",
}

QUERY = """
query Q($routes:[String!],$rateTypes:[String!],$rateSubtypes:[String!],$dateFrom:date,$dateTo:date){
  rate_meta(where:{info:{route:{_in:$routes},rate_type:{_in:$rateTypes},rate_subtype:{_in:$rateSubtypes}},rate_unit:{_eq:"usd"}}){
    rates(where:{date:{_gte:$dateFrom,_lte:$dateTo}},order_by:{date:asc}){date rate}
    info{route rate_type rate_subtype}
  }
}
"""

VARIABLES = {
    "routes": [
        "Capesize (182 000 dwt)",
        "Capesize (180 000 dwt)",
        "Panamax (75 000 dwt)",
        "Supramax (58 000 dwt)",
        "Handysize (38 000 dwt)",
        "VLCC",
        "Suezmax",
        "Aframax",
    ],
    "rateTypes": ["BULK", "TANK"],
    "rateSubtypes": ["TC", "1 Year T/C"],
    "dateFrom": "2000-01-01",
    "dateTo": "2026-12-31",
}

ROUTE_MAP = {
    "Capesize (182 000 dwt)": "capesize_1y_avg",
    "Capesize (180 000 dwt)": "capesize_1y_avg",
    "Panamax (75 000 dwt)": "panamax_1y_avg",
    "Supramax (58 000 dwt)": "supramax_1y_avg",
    "Handysize (38 000 dwt)": "handysize_1y_avg",
    "VLCC": "vlcc_1y",
    "Suezmax": "suezmax_1y",
    "Aframax": "aframax_1y",
}

OUTPUT = os.path.join("data", "derived", "time_charter_rates_fearnleys.csv")


def main():
    print("Fetching Fearnleys TC rates...")
    resp = requests.post(URL, json={"query": QUERY, "variables": VARIABLES}, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    if "errors" in data:
        print("GraphQL errors:", data["errors"])
        sys.exit(1)

    rate_metas = data["data"]["rate_meta"]
    print(f"Received {len(rate_metas)} rate_meta entries")

    # Build per-column DataFrames (merging 182k with 180k for Capesize)
    col_dfs = {}
    for meta in rate_metas:
        route = meta["info"]["route"]
        rate_type = meta["info"]["rate_type"]
        rate_subtype = meta["info"]["rate_subtype"]
        col = ROUTE_MAP.get(route)
        if col is None:
            print(f"  Skipping unknown route: {route}")
            continue
        rates = meta["rates"]
        print(f"  {route} ({rate_type}/{rate_subtype}): {len(rates)} data points -> {col}")
        if not rates:
            continue
        df = pd.DataFrame(rates)
        df.rename(columns={"rate": col}, inplace=True)
        df["date"] = pd.to_datetime(df["date"])
        df = df.dropna(subset=["date"]).drop_duplicates(subset=["date"])

        if col in col_dfs:
            # If 182k and 180k both exist, prefer 182k for modern dates, fallback to 180k
            if "182" in route:
                col_dfs[col] = df.set_index("date").combine_first(col_dfs[col].set_index("date")).reset_index()
            else:
                col_dfs[col] = col_dfs[col].set_index("date").combine_first(df.set_index("date")).reset_index()
        else:
            col_dfs[col] = df

    if not col_dfs:
        print("No data received!")
        sys.exit(1)

    # Outer-merge all column DataFrames on date
    merged = None
    for col, df in col_dfs.items():
        if merged is None:
            merged = df
        else:
            merged = merged.merge(df, on="date", how="outer")

    # Ensure all expected columns exist in canonical order
    canonical_cols = ["capesize_1y_avg", "panamax_1y_avg", "supramax_1y_avg",
                      "handysize_1y_avg", "vlcc_1y", "suezmax_1y", "aframax_1y"]
    expected_cols = ["date"] + canonical_cols
    for c in expected_cols:
        if c not in merged.columns:
            merged[c] = float("nan")

    merged = merged[expected_cols].sort_values("date").reset_index(drop=True)

    # Completeness guard: if existing CSV has more complete data for the latest date, retain it
    if os.path.exists(OUTPUT):
        try:
            prev_df = pd.read_csv(OUTPUT)
            prev_df["date"] = pd.to_datetime(prev_df["date"])
            latest_new_date = merged["date"].max()
            new_last_row = merged[merged["date"] == latest_new_date].iloc[0]
            new_valid = new_last_row[canonical_cols].count()

            if latest_new_date in prev_df["date"].values:
                prev_last_row = prev_df[prev_df["date"] == latest_new_date].iloc[0]
                prev_valid = prev_last_row[canonical_cols].count()
                if prev_valid > new_valid:
                    print(f"  [GUARD] Existing row for {latest_new_date.date()} has {prev_valid}/7 rates vs {new_valid}/7 new. Retaining existing values.")
                    for c in canonical_cols:
                        if pd.isna(new_last_row[c]) and not pd.isna(prev_last_row[c]):
                            merged.loc[merged["date"] == latest_new_date, c] = prev_last_row[c]
            elif new_valid < 4:
                print(f"  [GUARD] Latest date {latest_new_date.date()} has only {new_valid}/7 rates published (partial morning print). Waiting for full publish.")
                merged = merged[merged["date"] != latest_new_date]
        except Exception as e:
            print(f"  [WARN] Failed to compare with existing CSV: {e}")

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    merged.to_csv(OUTPUT, index=False)
    print(f"\nSaved to {OUTPUT}")

    # Verification
    print(f"Row count: {len(merged)}")
    print(f"Date range: {merged['date'].min().date()} to {merged['date'].max().date()}")
    print(f"Columns: {list(merged.columns)}")
    print(f"\nFirst 3 rows:\n{merged.head(3).to_string(index=False)}")
    print(f"\nLast 3 rows:\n{merged.tail(3).to_string(index=False)}")


if __name__ == "__main__":
    main()
