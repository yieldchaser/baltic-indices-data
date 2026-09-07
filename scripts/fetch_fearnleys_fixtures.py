"""
fetch_fearnleys_fixtures.py
Harvests all 537,160+ commercial charter fixtures from Fearnleys Hasura backend.
Supports:
  - Cursor-based streaming pagination (id > last_id, 1000/batch)
  - Automatic resumption from existing CSV if interrupted
  - Exponential backoff on rate limits / 5xx errors
  - Automatic Parquet export for high-performance analytics
"""

import csv
import os
import sys
import time
from datetime import datetime
import requests

ENDPOINT = "https://pbrokerapp.hasura.app/v1/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) FearnpulseHarvester/1.0",
}

QUERY = """
query GetFixturesBatch($lastId: bigint!, $batchSize: Int!) {
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

FIELDNAMES = [
    "id",
    "date",
    "charterer",
    "owner",
    "vessel",
    "imo",
    "rate",
    "period",
    "route",
    "segment",
    "department",
    "commodity",
    "load_port",
    "discharge_port",
    "laycan",
    "comment",
]


def get_resume_id(csv_path: str) -> tuple[int, int]:
    """Inspects existing CSV to determine highest ID and total existing rows."""
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return 0, 0

    max_id = 0
    row_count = 0
    try:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for r in reader:
                row_count += 1
                try:
                    rid = int(r.get("id", 0))
                    if rid > max_id:
                        max_id = rid
                except (ValueError, TypeError):
                    continue
        return max_id, row_count
    except Exception as e:
        print(f"  [WARN] Error inspecting existing CSV: {e}. Starting from ID 0.", flush=True)
        return 0, 0


def fetch_batch_with_retry(last_id: int, batch_size: int = 1000, max_retries: int = 5):
    payload = {
        "operationName": "GetFixturesBatch",
        "query": QUERY,
        "variables": {"lastId": last_id, "batchSize": batch_size},
    }

    for attempt in range(max_retries):
        try:
            resp = requests.post(ENDPOINT, json=payload, headers=HEADERS, timeout=40)
            if resp.status_code in (429, 500, 502, 503, 504):
                backoff = 2.0 * (2 ** attempt)
                time.sleep(backoff)
                continue
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                print(f"  [WARN] GraphQL errors: {data['errors']}", flush=True)
                return []
            return data.get("data", {}).get("fixture", [])
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"  [ERROR] Exceeded retries at lastId={last_id}: {e}", flush=True)
                return None
            time.sleep(2.0 * (attempt + 1))
    return None


def main():
    print("================================================================", flush=True)
    print("  Fearnleys Commercial Fixtures Harvester (537,160 Fixtures)   ", flush=True)
    print("================================================================", flush=True)

    out_dir = os.path.join("..", "data", "derived")
    os.makedirs(out_dir, exist_ok=True)
    csv_path = os.path.join(out_dir, "fearnleys_fixtures_full.csv")

    TOTAL_EXPECTED = 537160
    BATCH_SIZE = 1000

    last_id, initial_count = get_resume_id(csv_path)
    file_exists = os.path.exists(csv_path) and os.path.getsize(csv_path) > 0

    if last_id > 0:
        print(f"Resuming harvest: Found {initial_count} existing rows (last ID: {last_id})", flush=True)
    else:
        print("Starting fresh harvest...", flush=True)

    f_out = open(csv_path, "a" if file_exists else "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(f_out, fieldnames=FIELDNAMES)
    if not file_exists:
        writer.writeheader()

    total_rows = initial_count
    start_time = time.time()
    batch_idx = 0

    try:
        while True:
            batch = fetch_batch_with_retry(last_id, batch_size=BATCH_SIZE)
            if batch is None:
                print("Encountered fatal network error. Stopping harvest for resumption.", flush=True)
                break
            if not batch:
                print("\nReached end of fixtures catalog (0 rows returned in batch).", flush=True)
                break

            # Clean comments & fields for CSV safety
            for r in batch:
                for col in FIELDNAMES:
                    val = r.get(col)
                    if val is None:
                        r[col] = ""
                    elif isinstance(val, str):
                        r[col] = val.replace("\r\n", " ").replace("\n", " ")

            writer.writerows(batch)
            f_out.flush()

            batch_idx += 1
            batch_count = len(batch)
            total_rows += batch_count
            last_id = int(batch[-1]["id"])

            elapsed = time.time() - start_time
            rows_this_session = total_rows - initial_count
            rate_per_sec = rows_this_session / elapsed if elapsed > 0 else 0
            remaining_rows = max(0, TOTAL_EXPECTED - total_rows)
            eta_sec = remaining_rows / rate_per_sec if rate_per_sec > 0 else 0
            eta_str = f"{int(eta_sec // 60)}m {int(eta_sec % 60):02d}s" if eta_sec >= 60 else f"{int(eta_sec)}s"

            pct = min(100.0, (total_rows / TOTAL_EXPECTED) * 100.0)
            print(
                f"[{total_rows:6d}/{TOTAL_EXPECTED}] ({pct:5.1f}%) | Last ID: {last_id:6d} | Batch: +{batch_count:4d} | Speed: {rate_per_sec:4.0f} rows/s | ETA: {eta_str}",
                flush=True,
            )

            if batch_count < BATCH_SIZE:
                # Last page reached
                print("\nFinal batch complete.", flush=True)
                break

            # Polite pacing
            time.sleep(0.25)

    finally:
        f_out.close()

    total_elapsed = time.time() - start_time
    print(f"\nSaved total {total_rows} fixtures to {csv_path}", flush=True)
    print(f"Elapsed time: {total_elapsed / 60:.1f} minutes", flush=True)

    # Parquet generation
    try:
        import pandas as pd
        parquet_path = os.path.join(out_dir, "fearnleys_fixtures_full.parquet")
        print("Converting CSV to Parquet for fast querying...", flush=True)
        df = pd.read_csv(csv_path, low_memory=False)
        df.to_parquet(parquet_path, index=False, compression="snappy")
        print(f"Saved Parquet archive ({os.path.getsize(parquet_path) / (1024*1024):.1f} MB) to {parquet_path}", flush=True)
    except Exception as e:
        print(f"(Parquet conversion skipped: {e})", flush=True)


if __name__ == "__main__":
    main()
