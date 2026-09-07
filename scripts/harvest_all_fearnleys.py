"""
harvest_all_fearnleys.py
Master orchestrator to ingest all historical Fearnleys datasets:
  Phase 1: Weekly Custom Research Reports (100 reports -> Markdown + JSON catalog)
  Phase 2: Vessel Sale & Purchase Deals (2,592 transactions -> CSV)
  Phase 3: Broker Commentary Feed (11,713 records -> CSV)
  Phase 4: Commercial Charter Fixtures Archive (537,160 fixtures -> CSV + Parquet)
"""

import os
import sys
import time

from fetch_fearnleys_reports import fetch_all_reports
from fetch_fearnleys_snp import fetch_all_snp
from fetch_fearnleys_comments import fetch_all_comments
import fetch_fearnleys_fixtures


def main():
    print("################################################################", flush=True)
    print("     FEARNLEYS COMPLETE INSTITUTIONAL HISTORICAL INGESTION      ", flush=True)
    print("################################################################\n", flush=True)

    start_total = time.time()

    # Phase 1: Research Reports
    print(">>> PHASE 1/4: Weekly Custom Research Reports & Leading Indicators", flush=True)
    num_reports = fetch_all_reports()

    # Phase 2: S&P Transactions
    print("\n>>> PHASE 2/4: Vessel Sale & Purchase (S&P) Transactions", flush=True)
    num_snp = fetch_all_snp()

    # Phase 3: Broker Comments
    print("\n>>> PHASE 3/4: Qualitative Broker Commentary Feed", flush=True)
    num_comments = fetch_all_comments()

    # Phase 4: Commercial Fixtures
    print("\n>>> PHASE 4/4: Commercial Charter Fixtures Archive (537,160 Deals)", flush=True)
    fetch_fearnleys_fixtures.main()

    total_time = time.time() - start_total
    print("\n################################################################", flush=True)
    print("                ALL PHASES COMPLETED SUCCESSFULLY               ", flush=True)
    print("################################################################", flush=True)
    print(f"Total Execution Time: {total_time / 60:.1f} minutes")
    print(f"Reports Digested     : {num_reports} reports")
    print(f"S&P Transactions     : {num_snp} deals")
    print(f"Broker Comments      : {num_comments} notes")
    print("Fixtures Harvested   : Completed (see fearnleys_fixtures_full.csv)")
    print("################################################################\n", flush=True)


if __name__ == "__main__":
    main()
