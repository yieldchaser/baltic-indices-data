"""
fetch_drewry_ais_weekly.py
Automated weekly scraper for Drewry AIS Fleet Analytics PDFs.

Runs incrementally (e.g. in GitHub Actions every Friday).
- Checks current ISO week and recent lookback weeks.
- Dynamically probes DAM ID ranges around the latest anchor.
- Uses fast streaming GET with low timeout to quickly probe availability.
- Downloads newly published PDFs to scripts/drewry_ais_pdfs/ (gitignored).
- Updates reports/drewry/ais_manifest.csv and data/derived/drewry_ais_checkpoint.json.
- Completely idempotent: skips already downloaded reports.
"""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
from datetime import date
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PDF_DIR = REPO_ROOT / "scripts" / "drewry_ais_pdfs"
MANIFEST_FILE = REPO_ROOT / "reports" / "drewry" / "ais_manifest.csv"
CHECKPOINT_FILE = REPO_ROOT / "data" / "derived" / "drewry_ais_checkpoint.json"

BASE_URL = "https://www.drewry.co.uk/AcuCustom/Sitename/DAM"

VESSEL_CLASSES = [
    "Crude_Suezmax", "Crude_VLCC", "Crude_Aframax",
    "Drybulk_Panamax", "Drybulk_Capesize", "Drybulk_Supramax", "Drybulk_Handysize",
    "Product_LR2", "Product_LR1",
    "LPG_FR",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def load_manifest():
    """Load existing filenames from manifest."""
    seen = set()
    if MANIFEST_FILE.exists():
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("filename"):
                    seen.add(row["filename"])
    return seen


def load_checkpoint():
    """Load latest anchor DAM ID."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    # Anchor from Week 35 2026: DAM 33
    return {"last_dam_id": 33, "last_week": 35, "last_year": 2026}


def save_checkpoint(data):
    """Persist latest anchor DAM ID."""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def append_to_manifest(entries):
    """Append newly discovered rows to ais_manifest.csv."""
    if not entries:
        return
    MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    file_exists = MANIFEST_FILE.exists()

    with open(MANIFEST_FILE, "a", newline="", encoding="utf-8") as f:
        fieldnames = ["filename", "year", "week", "vessel_class", "size_bytes", "sha256"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for entry in entries:
            writer.writerow(entry)


def probe_and_fetch(year: int, week: int, dam_id: int, known_files: set, dry_run: bool = False):
    """Probe all vessel classes for a given week and DAM ID using fast streaming GET."""
    downloaded = []

    for cls in VESSEL_CLASSES:
        filename = f"Drewry_AIS_{cls}_Week{str(week).zfill(2)}_{year}.pdf"
        if filename in known_files:
            continue

        url = f"{BASE_URL}/{dam_id:03d}/{filename}"

        try:
            resp = requests.get(url, headers=HEADERS, stream=True, timeout=6)
        except requests.exceptions.RequestException:
            continue

        content_type = resp.headers.get("Content-Type", "").lower()
        if resp.status_code == 200 and "application/pdf" in content_type:
            content = resp.content
            content_len = len(content)
            if content_len > 1000:
                print(f"  [FOUND] DAM/{dam_id:03d} -> {filename} ({content_len:,} bytes)", flush=True)
                if not dry_run:
                    PDF_DIR.mkdir(parents=True, exist_ok=True)
                    out_path = PDF_DIR / filename
                    with open(out_path, "wb") as f:
                        f.write(content)

                    sha256 = hashlib.sha256(content).hexdigest()
                    entry = {
                        "filename": filename,
                        "year": year,
                        "week": week,
                        "vessel_class": cls,
                        "size_bytes": content_len,
                        "sha256": sha256,
                    }
                    downloaded.append(entry)
                    known_files.add(filename)
                else:
                    print(f"  [DRY RUN] Would save {filename}", flush=True)
        resp.close()
        time.sleep(0.2)

    return downloaded


def run_weekly(lookback_weeks: int = 1, spread: int = 2, dry_run: bool = False):
    today = date.today()
    iso_year, current_iso_week, _ = today.isocalendar()

    print(f"=== Drewry AIS Weekly Ingest: Year {iso_year}, Current Week {current_iso_week} ===", flush=True)
    known_files = load_manifest()
    checkpoint = load_checkpoint()
    last_dam = checkpoint.get("last_dam_id", 33)

    weeks_to_check = []
    for w in range(current_iso_week - lookback_weeks, current_iso_week + 1):
        if w > 0:
            weeks_to_check.append(w)

    dam_candidates = range(max(1, last_dam), last_dam + spread + 1)
    print(f"Checking weeks: {weeks_to_check} across DAM/{dam_candidates.start:03d}-{dam_candidates.stop - 1:03d}", flush=True)

    new_downloads = []
    latest_hit_dam = last_dam

    for week in sorted(weeks_to_check):
        for dam_id in dam_candidates:
            hits = probe_and_fetch(iso_year, week, dam_id, known_files, dry_run=dry_run)
            if hits:
                new_downloads.extend(hits)
                latest_hit_dam = max(latest_hit_dam, dam_id)

    if not dry_run:
        if new_downloads:
            append_to_manifest(new_downloads)
            print(f"Downloaded and cataloged {len(new_downloads)} new AIS report(s).", flush=True)
            checkpoint["last_dam_id"] = latest_hit_dam
            checkpoint["last_week"] = current_iso_week
            checkpoint["last_year"] = iso_year
            save_checkpoint(checkpoint)
        else:
            print("No new AIS reports published this cycle. Catalog is up to date.", flush=True)
    else:
        print(f"[DRY RUN] Finished probe. Detected {len(new_downloads)} new files.", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Drewry AIS Weekly Reports Scraper")
    parser.add_argument("--lookback", type=int, default=1, help="Weeks to look back (default: 1)")
    parser.add_argument("--spread", type=int, default=2, help="DAM ID search spread (default: 2)")
    parser.add_argument("--dry-run", action="store_true", help="Probe without writing to disk")
    args = parser.parse_args()

    run_weekly(lookback_weeks=args.lookback, spread=args.spread, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
