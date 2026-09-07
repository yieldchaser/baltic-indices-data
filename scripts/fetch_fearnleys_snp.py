"""
fetch_fearnleys_snp.py
Harvests all 2,592+ historical vessel Sale & Purchase (S&P) transactions
from Fearnleys Hasura backend.
Saves to:
  data/derived/fearnleys_snp_transactions.csv
"""

import csv
import os
import time
import requests

ENDPOINT = "https://pbrokerapp.hasura.app/v1/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) FearnpulseHarvester/1.0",
}

QUERY = """
query GetSnpTransactions($limit: Int!, $offset: Int!) {
  snp_transaction(
    limit: $limit
    offset: $offset
    order_by: {created_at: desc}
  ) {
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


def fetch_all_snp():
    out_dir = os.path.join("..", "data", "derived")
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, "fearnleys_snp_transactions.csv")
    fieldnames = ["id", "created_at", "vessel", "built", "yard", "dwt", "segment", "price", "buyer", "comment"]

    all_rows = []
    limit = 500
    offset = 0

    print("Fetching Fearnleys S&P transaction records...", flush=True)
    while True:
        resp = requests.post(
            ENDPOINT,
            json={"query": QUERY, "variables": {"limit": limit, "offset": offset}},
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"GraphQL error: {data['errors']}")

        batch = data.get("data", {}).get("snp_transaction", [])
        if not batch:
            break

        all_rows.extend(batch)
        print(f"  Fetched {len(all_rows)} S&P deals so far...", flush=True)
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.2)

    print(f"Total S&P transactions fetched: {len(all_rows)}", flush=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Saved {len(all_rows)} S&P records to {out_csv}\n", flush=True)
    return len(all_rows)


if __name__ == "__main__":
    fetch_all_snp()
