"""
harvest_missing_ais_history.py
Downloads the remaining historical AIS Analytics PDFs:
- 2025 weeks 7-50 across DAM 27, 28, 29, 30, 31
- 2024 H2 weeks 26-52 across DAM 25, 26, 27

Updates reports/drewry/ais_manifest.csv directly.
Includes robust try/except around body download to survive transient read timeouts.
"""

import csv
import hashlib
import os
import re
import sys
import time
from pathlib import Path
import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = REPO_ROOT / "scripts" / "drewry_ais_pdfs"
MANIFEST_FILE = REPO_ROOT / "reports" / "drewry" / "ais_manifest.csv"

BASE = "https://www.drewry.co.uk/AcuCustom/Sitename/DAM"

VESSEL_CLASSES = [
    "Crude_Suezmax", "Crude_VLCC", "Crude_Aframax",
    "Drybulk_Panamax", "Drybulk_Capesize", "Drybulk_Supramax", "Drybulk_Handysize",
    "Product_LR2", "Product_LR1",
    "LPG_FR",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

HISTORICAL_PLAN = [
    # 2025 missing weeks
    (2025, 27, range(7, 11)),
    (2025, 28, range(11, 24)),
    (2025, 29, range(24, 35)),
    (2025, 30, range(35, 51)),
    (2025, 31, [50]),
    # 2024 H2 missing weeks
    (2024, 25, [26, 27, 28, 30, 31, 32, 33, 34, 35]),
    (2024, 26, [37, 38, 39, 40, 41, 42, 44, 45, 46, 47]),
    (2024, 27, [48, 49, 50, 51, 52]),
]


def load_existing():
    existing = set()
    if MANIFEST_FILE.exists():
        with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("filename"):
                    existing.add(row["filename"])
    for f in OUT_DIR.glob("*.pdf"):
        existing.add(f.name)
    return existing


def append_manifest_rows(rows):
    if not rows:
        return
    with open(MANIFEST_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["filename", "year", "week", "vessel_class", "size_bytes", "sha256"])
        for r in rows:
            writer.writerow(r)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    existing = load_existing()
    print(f"Loaded {len(existing)} existing files from manifest/disk.", flush=True)

    new_rows = []
    total_found = 0

    session = requests.Session()
    session.headers.update(HEADERS)

    for year, dam_id, weeks in HISTORICAL_PLAN:
        for week in weeks:
            for cls in VESSEL_CLASSES:
                filename = f"Drewry_AIS_PDF_{cls}_Week{str(week).zfill(2)}_{year}1.pdf"
                if filename in existing:
                    continue

                url = f"{BASE}/{dam_id:03d}/{filename}"
                content = None

                for attempt in range(3):
                    try:
                        resp = session.get(url, stream=True, timeout=15)
                        content_type = resp.headers.get("Content-Type", "").lower()
                        if resp.status_code == 200 and "application/pdf" in content_type:
                            data = resp.content
                            if len(data) > 1000:
                                content = data
                        resp.close()
                        break
                    except Exception as exc:
                        if attempt == 2:
                            print(f"  [WARN] Failed to fetch {url}: {exc}", flush=True)
                        time.sleep(1)

                if content:
                    out_path = OUT_DIR / filename
                    with open(out_path, "wb") as fp:
                        fp.write(content)

                    sha256 = hashlib.sha256(content).hexdigest()
                    row = {
                        "filename": filename,
                        "year": year,
                        "week": week,
                        "vessel_class": cls,
                        "size_bytes": len(content),
                        "sha256": sha256,
                    }
                    new_rows.append(row)
                    existing.add(filename)
                    total_found += 1
                    print(f"  [OK] {year}-W{week:02d} {cls} -> {filename} ({len(content):,} bytes)", flush=True)

                    # Flush to manifest every 5 downloads
                    if len(new_rows) >= 5:
                        append_manifest_rows(new_rows)
                        new_rows.clear()

                time.sleep(0.15)

    if new_rows:
        append_manifest_rows(new_rows)
        new_rows.clear()

    print(f"\nFinished historical harvest! Newly downloaded: {total_found} PDFs.", flush=True)


if __name__ == "__main__":
    main()
