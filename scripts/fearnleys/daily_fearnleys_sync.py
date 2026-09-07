"""
daily_fearnleys_sync.py
Polite, incremental delta synchronization for Fearnleys Hasura GraphQL backend.
Designed for daily / scheduled automation (runs in <30 seconds without hammer).

Workflow:
  1. Fixtures: Delta pull using cursor id > max_existing_id (only newly added deals).
  2. Rates: Pulls only recent prints (last 30 days) across series and upserts into fearnpulse_rates_full.csv.
  3. S&P Transactions: Checks top 50 recent deals and appends any unseen records.
  4. Broker Comments: Checks top 50 recent comments and appends unseen records.
  5. Custom Reports: Checks top 5 recent publications; saves markdown if new issue.
  6. Rebuilds pre-aggregated cache: scripts/fearnleys/build_fearnleys_cache.py.
"""

import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
import requests

ENDPOINT = "https://pbrokerapp.hasura.app/v1/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://fearnpulse.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) FearnpulseDeltaSync/1.0",
}

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DERIVED_DIR = os.path.join(BASE_DIR, "data", "derived")
REPORTS_DIR = os.path.join(BASE_DIR, "reports", "fearnleys")
DATA_REPORTS_DIR = os.path.join(BASE_DIR, "data", "reports", "fearnleys")

FIXTURES_CSV = os.path.join(DERIVED_DIR, "fearnleys_fixtures_full.csv")
FIXTURES_PARQUET = os.path.join(DERIVED_DIR, "fearnleys_fixtures_full.parquet")
RATES_CSV = os.path.join(DERIVED_DIR, "fearnpulse_rates_full.csv")
SNP_CSV = os.path.join(DERIVED_DIR, "fearnleys_snp_transactions.csv")
COMMENTS_CSV = os.path.join(DERIVED_DIR, "fearnleys_broker_comments.csv")
REPORTS_CATALOG = os.path.join(BASE_DIR, "reports", "fearnleys_reports_catalog.json")


def post_graphql_with_retry(payload, max_retries=3, timeout=30):
    for attempt in range(max_retries):
        try:
            resp = requests.post(ENDPOINT, json=payload, headers=HEADERS, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(2.0 * (attempt + 1))
                continue
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                print(f"    [WARN] GraphQL error: {data['errors']}", flush=True)
                return None
            return data.get("data")
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"    [ERROR] Request failed: {e}", flush=True)
                return None
            time.sleep(2.0 * (attempt + 1))
    return None


# -----------------------------------------------------------------------------
# 1. FIXTURES DELTA SYNC
# -----------------------------------------------------------------------------
FIXTURE_QUERY = """
query GetNewFixtures($lastId: bigint!, $batchSize: Int!) {
  fixture(
    limit: $batchSize
    where: {id: {_gt: $lastId}}
    order_by: {id: asc}
  ) {
    id
    date
    charterer
    owner
    vessel
    imo
    rate
    period
    route
    segment
    department
    commodity
    load_port
    discharge_port
    laycan
    comment
  }
}
"""

FIXTURE_FIELDS = [
    "id", "date", "charterer", "owner", "vessel", "imo", "rate", "period",
    "route", "segment", "department", "commodity", "load_port", "discharge_port",
    "laycan", "comment"
]


def sync_fixtures():
    print(">>> [1/5] Synchronizing Commercial Fixtures (Cursor Delta)...", flush=True)
    if not os.path.exists(FIXTURES_CSV):
        print("    [WARN] Base fixtures CSV not found, skipping delta sync.")
        return 0

    max_id = 0
    with open(FIXTURES_CSV, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                rid = int(r.get("id", 0))
                if rid > max_id:
                    max_id = rid
            except Exception:
                continue

    print(f"    Current highest fixture ID: {max_id}", flush=True)
    payload = {
        "operationName": "GetNewFixtures",
        "query": FIXTURE_QUERY,
        "variables": {"lastId": max_id, "batchSize": 500},
    }
    data = post_graphql_with_retry(payload)
    if not data or "fixture" not in data:
        print("    No response from fixtures endpoint.", flush=True)
        return 0

    new_fixtures = data["fixture"]
    if not new_fixtures:
        print("    Fixtures database is fully up to date (0 new fixtures).", flush=True)
        return 0

    print(f"    Found {len(new_fixtures)} new fixtures. Appending...", flush=True)
    with open(FIXTURES_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIXTURE_FIELDS)
        for r in new_fixtures:
            clean_row = {}
            for col in FIXTURE_FIELDS:
                val = r.get(col)
                if val is None:
                    clean_row[col] = ""
                elif isinstance(val, str):
                    clean_row[col] = val.replace("\r\n", " ").replace("\n", " ")
                else:
                    clean_row[col] = str(val)
            writer.writerow(clean_row)

    # Regenerate Parquet if pandas is available
    try:
        import pandas as pd
        df = pd.read_csv(FIXTURES_CSV, low_memory=False)
        df.to_parquet(FIXTURES_PARQUET, index=False, compression="snappy")
        print("    Regenerated fearnleys_fixtures_full.parquet.", flush=True)
    except Exception as e:
        print(f"    (Parquet update skipped: {e})", flush=True)

    return len(new_fixtures)


# -----------------------------------------------------------------------------
# 2. RATES RECENT DELTA SYNC
# -----------------------------------------------------------------------------
RATES_DELTA_QUERY = """
query GetRecentRates($dateFrom: date!) {
  rate_meta {
    info {
      rate_type
      rate_subtype
      route
    }
    rate_unit
    rates(where: {date: {_gte: $dateFrom}}, order_by: {date: desc}) {
      date
      rate
    }
  }
}
"""


def sync_rates():
    print("\n>>> [2/5] Synchronizing Recent Freight & Asset Benchmarks (30-day window)...", flush=True)
    date_from = (datetime.now(timezone.utc) - timedelta(days=35)).strftime("%Y-%m-%d")
    payload = {
        "operationName": "GetRecentRates",
        "query": RATES_DELTA_QUERY,
        "variables": {"dateFrom": date_from},
    }
    data = post_graphql_with_retry(payload, timeout=45)
    if not data or "rate_meta" not in data:
        print("    Failed to fetch recent rates.", flush=True)
        return 0

    rate_metas = data["rate_meta"]
    new_rows = []
    for m in rate_metas:
        info = m.get("info") or {}
        rt = info.get("rate_type")
        rst = info.get("rate_subtype")
        route = info.get("route")
        unit = m.get("rate_unit") or "usd"
        if not (rt and rst and route):
            continue

        label = f"{re.sub(r'[^a-zA-Z0-9]+', '_', rt).strip('_').upper()}_{re.sub(r'[^a-zA-Z0-9]+', '_', rst).strip('_').upper()}_{re.sub(r'[^a-zA-Z0-9]+', '_', route).strip('_').upper()}"
        for r in m.get("rates", []):
            new_rows.append({
                "label": label,
                "rate_type": rt,
                "rate_subtype": rst,
                "route": route,
                "unit": unit,
                "date": r["date"],
                "rate": r["rate"],
            })

    print(f"    Received {len(new_rows)} recent rate observations.", flush=True)
    if not new_rows or not os.path.exists(RATES_CSV):
        return 0

    import pandas as pd
    df_existing = pd.read_csv(RATES_CSV, low_memory=False)
    df_new = pd.DataFrame(new_rows)

    # Concat, drop duplicates on [rate_type, rate_subtype, route, unit, date] keeping latest
    combined = pd.concat([df_existing, df_new], ignore_index=True)
    before_len = len(df_existing)
    combined = combined.drop_duplicates(subset=["rate_type", "rate_subtype", "route", "unit", "date"], keep="last")
    combined.sort_values(by=["rate_type", "rate_subtype", "route", "date"], inplace=True)
    after_len = len(combined)

    added = after_len - before_len
    print(f"    Appended {added} new rate observations (total now {after_len}).", flush=True)
    combined.to_csv(RATES_CSV, index=False)
    return added


# -----------------------------------------------------------------------------
# 3. S&P DEALS SYNC
# -----------------------------------------------------------------------------
SNP_QUERY = """
query GetRecentSnp {
  snp_transaction(limit: 50, order_by: {created_at: desc}) {
    id
    created_at
    vessel
    built
    yard
    dwt
    segment
    price
    buyer
    comment
  }
}
"""


def sync_snp():
    print("\n>>> [3/5] Synchronizing S&P Transactions Ledger...", flush=True)
    payload = {"operationName": "GetRecentSnp", "query": SNP_QUERY}
    data = post_graphql_with_retry(payload)
    if not data or "snp_transaction" not in data:
        print("    Failed to fetch recent S&P transactions.", flush=True)
        return 0

    recent_deals = data["snp_transaction"]
    if not recent_deals or not os.path.exists(SNP_CSV):
        return 0

    # Collect existing IDs
    existing_ids = set()
    with open(SNP_CSV, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for r in reader:
            existing_ids.add(str(r.get("id")))

    unseen = [d for d in recent_deals if str(d.get("id")) not in existing_ids]
    if not unseen:
        print("    S&P transactions are up to date (0 new deals).", flush=True)
        return 0

    print(f"    Found {len(unseen)} new S&P deals. Appending...", flush=True)
    fields = ["id", "created_at", "vessel", "built", "yard", "dwt", "segment", "price", "buyer", "comment"]
    with open(SNP_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        for d in reversed(unseen):
            clean_d = {k: d.get(k, "") for k in fields}
            writer.writerow(clean_d)
    return len(unseen)


# -----------------------------------------------------------------------------
# 4. BROKER COMMENTS SYNC
# -----------------------------------------------------------------------------
COMMENTS_QUERY = """
query GetRecentComments {
  comment(limit: 50, order_by: {date: desc}) {
    id
    date
    text
    created_at
    comment_meta_id
    metadata {
      comment_type
      comment_subtype
      comment_name
    }
  }
}
"""


def sync_comments():
    print("\n>>> [4/5] Synchronizing Broker Commentary Feed...", flush=True)
    payload = {"operationName": "GetRecentComments", "query": COMMENTS_QUERY}
    data = post_graphql_with_retry(payload)
    if not data or "comment" not in data:
        print("    Failed to fetch recent comments.", flush=True)
        return 0

    recent_comments = data["comment"]
    if not recent_comments or not os.path.exists(COMMENTS_CSV):
        return 0

    existing_ids = set()
    with open(COMMENTS_CSV, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for r in reader:
            existing_ids.add(str(r.get("id")))

    unseen = [c for c in recent_comments if str(c.get("id")) not in existing_ids]
    if not unseen:
        print("    Broker comments are up to date (0 new notes).", flush=True)
        return 0

    print(f"    Found {len(unseen)} new broker comments. Appending...", flush=True)
    fields = ["id", "date", "comment_type", "comment_subtype", "comment_name", "text", "created_at", "comment_meta_id"]
    with open(COMMENTS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        for c in reversed(unseen):
            meta = c.pop("metadata", None) or {}
            c["comment_type"] = meta.get("comment_type", "")
            c["comment_subtype"] = meta.get("comment_subtype", "")
            c["comment_name"] = meta.get("comment_name", "")
            if isinstance(c.get("text"), str):
                c["text"] = c["text"].replace("\r\n", " ").replace("\n", " ")
            writer.writerow({k: c.get(k, "") for k in fields})
    return len(unseen)


# -----------------------------------------------------------------------------
# 5. RESEARCH REPORTS CHECK
# -----------------------------------------------------------------------------
REPORTS_QUERY = """
query GetRecentReports {
  custom_report(limit: 5, order_by: {date: desc}) {
    id
    date
    title
    department
    slug
    status
    pdf_url
    audio_url
    content
    created_at
    updated_at
  }
}
"""


def sync_reports():
    print("\n>>> [5/5] Checking for New Fearnleys Weekly Reports...", flush=True)
    payload = {"operationName": "GetRecentReports", "query": REPORTS_QUERY}
    data = post_graphql_with_retry(payload)
    if not data or "custom_report" not in data:
        print("    Failed to fetch recent reports.", flush=True)
        return 0

    recent_reports = data["custom_report"]
    if not recent_reports or not os.path.exists(REPORTS_CATALOG):
        return 0

    with open(REPORTS_CATALOG, "r", encoding="utf-8") as f:
        catalog = json.load(f)

    existing_ids = {r.get("id") for r in catalog}
    unseen = [r for r in recent_reports if r.get("id") not in existing_ids]

    if not unseen:
        print("    Reports catalog is up to date (0 new publications).", flush=True)
        return 0

    print(f"    Found {len(unseen)} new publications! Updating catalog and generating markdown...", flush=True)
    from fetch_fearnleys_reports import blocks_to_markdown, slugify

    for r in unseen:
        catalog.insert(0, r)
        rep_date = r.get("date") or "undated"
        rep_slug = r.get("slug") or slugify(r.get("title") or r.get("id"))
        filename = f"{rep_date}_{rep_slug}.md"
        md_content = blocks_to_markdown(r)
        for d in [REPORTS_DIR, DATA_REPORTS_DIR]:
            os.makedirs(d, exist_ok=True)
            with open(os.path.join(d, filename), "w", encoding="utf-8") as mf:
                mf.write(md_content)

    with open(REPORTS_CATALOG, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)

    return len(unseen)


def main():
    print("================================================================", flush=True)
    print("  FEARNLEYS HASURA POLITE DAILY INCREMENTAL SYNCHRONIZER        ", flush=True)
    print("================================================================\n", flush=True)

    t0 = time.time()
    n_fix = sync_fixtures()
    n_rates = sync_rates()
    n_snp = sync_snp()
    n_comm = sync_comments()
    n_rep = sync_reports()

    # Rebuild summary cache
    print("\n================================================================", flush=True)
    print("  REBUILDING FEARNLEYS PRE-AGGREGATED FRONTEND CACHE            ", flush=True)
    print("================================================================", flush=True)
    import build_fearnleys_cache
    build_fearnleys_cache.main()

    elapsed = time.time() - t0
    print(f"Daily Sync Complete in {elapsed:.1f}s.")
    print(f"Deltas -> Fixtures: +{n_fix} | Rates: +{n_rates} | S&P: +{n_snp} | Comments: +{n_comm} | Reports: +{n_rep}")
    print("================================================================\n", flush=True)


if __name__ == "__main__":
    main()
