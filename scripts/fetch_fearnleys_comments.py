"""
fetch_fearnleys_comments.py
Harvests all 11,713+ broker commentary notes and market sentiment records
from Fearnleys Hasura backend.
Saves to:
  data/derived/fearnleys_broker_comments.csv
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
query GetComments($limit: Int!, $offset: Int!) {
  comment(
    limit: $limit
    offset: $offset
    order_by: {date: desc}
  ) {
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


def fetch_all_comments():
    out_dir = os.path.join("..", "data", "derived")
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, "fearnleys_broker_comments.csv")
    fieldnames = [
        "id",
        "date",
        "comment_type",
        "comment_subtype",
        "comment_name",
        "text",
        "created_at",
        "comment_meta_id",
    ]

    all_rows = []
    limit = 1000
    offset = 0

    print("Fetching Fearnleys broker commentary feed...", flush=True)
    while True:
        resp = requests.post(
            ENDPOINT,
            json={"query": QUERY, "variables": {"limit": limit, "offset": offset}},
            headers=HEADERS,
            timeout=35,
        )
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"GraphQL error: {data['errors']}")

        batch = data.get("data", {}).get("comment", [])
        if not batch:
            break

        for row in batch:
            meta = row.pop("metadata", None) or {}
            row["comment_type"] = meta.get("comment_type", "")
            row["comment_subtype"] = meta.get("comment_subtype", "")
            row["comment_name"] = meta.get("comment_name", "")
            if isinstance(row.get("text"), str):
                row["text"] = row["text"].replace("\r\n", " ").replace("\n", " ")
            all_rows.append(row)

        print(f"  Fetched {len(all_rows)} commentary notes...", flush=True)
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.25)

    print(f"Total broker comments fetched: {len(all_rows)}", flush=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Saved {len(all_rows)} broker comments to {out_csv}\n", flush=True)
    return len(all_rows)


if __name__ == "__main__":
    fetch_all_comments()
