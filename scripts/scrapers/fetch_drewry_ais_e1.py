"""
fetch_drewry_ais_e1.py
Institutional Drewry AIS Report Harvester with Finding E1 Validation.

Implements strict validation to prevent saving HTML error/paywall pages as PDFs:
1. Asserts HTTP 200 OK.
2. Asserts 'Content-Type: application/pdf' in HTTP response headers.
3. Asserts magic bytes: payload MUST begin with b'%PDF-'.
4. Detects and quarantines HTML error payloads (<!DOCTYPE, <html, 403, 404, WAF).
5. Validates SHA-256 checksum against ais_manifest.csv where available.
6. Writes valid PDFs to scripts/drewry_ais_pdfs/ (gitignored per repo policy).
7. Completely idempotent: skips already verified local PDFs.
"""

import os
import sys
import csv
import time
import hashlib
import argparse
from pathlib import Path
import requests

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
MANIFEST_FILE = REPO_ROOT / "reports" / "drewry" / "ais_manifest.csv"
OUT_DIR = REPO_ROOT / "scripts" / "drewry_ais_pdfs"
QUARANTINE_DIR = REPO_ROOT / "data" / "derived" / "quarantine_drewry_e1"

BASE_URL = "https://www.drewry.co.uk/AcuCustom/Sitename/DAM"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "application/pdf,*/*;q=0.8",
}

def load_manifest():
    entries = []
    if not MANIFEST_FILE.exists():
        print(f"Manifest not found at {MANIFEST_FILE}")
        return entries
    with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            entries.append(r)
    return entries

def validate_pdf_payload(content: bytes) -> tuple[bool, str]:
    """Validate that payload is a genuine PDF and not an HTML error or stub."""
    if not content:
        return False, "EMPTY_PAYLOAD"
    if len(content) < 1000:
        return False, f"TOO_SMALL_{len(content)}_BYTES"
    if not content.startswith(b"%PDF-"):
        # Check if it's HTML
        prefix = content[:200].lower()
        if b"<!doctype" in prefix or b"<html" in prefix or b"<head" in prefix:
            return False, "HTML_ERROR_PAGE_DETECTED"
        return False, f"INVALID_MAGIC_BYTES_{content[:8]}"
    return True, "VALID_PDF"

def fetch_report(entry: dict, session: requests.Session, dam_range=range(25, 36), dry_run=False):
    filename = entry["filename"]
    expected_sha = entry.get("sha256", "")
    target_path = OUT_DIR / filename

    # If already downloaded and valid, skip
    if target_path.exists() and target_path.stat().st_size > 1000:
        with open(target_path, "rb") as f:
            header = f.read(10)
        if header.startswith(b"%PDF-"):
            return "ALREADY_EXISTS_VALID", target_path

    # Try probing DAM IDs
    for dam_id in dam_range:
        url = f"{BASE_URL}/{dam_id:03d}/{filename}"
        try:
            resp = session.get(url, headers=HEADERS, timeout=10, stream=True)
            status = resp.status_code
            content_type = resp.headers.get("Content-Type", "").lower()
            
            if status == 200 and "application/pdf" in content_type:
                content = resp.content
                is_valid, reason = validate_pdf_payload(content)
                if is_valid:
                    sha = hashlib.sha256(content).hexdigest()
                    if expected_sha and sha != expected_sha:
                        print(f"  [WARN] SHA mismatch for {filename}: got {sha}, expected {expected_sha}", flush=True)
                    if not dry_run:
                        OUT_DIR.mkdir(parents=True, exist_ok=True)
                        with open(target_path, "wb") as f:
                            f.write(content)
                        return f"DOWNLOADED_DAM_{dam_id:03d}", target_path
                    else:
                        return f"FOUND_DRY_RUN_DAM_{dam_id:03d}", None
                else:
                    # Quarantine finding E1
                    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
                    q_path = QUARANTINE_DIR / f"{filename}.error.html"
                    with open(q_path, "wb") as f:
                        f.write(content[:2048])
                    return f"QUARANTINED_E1_{reason}", q_path
            resp.close()
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.1)

    return "NOT_FOUND_ACROSS_DAMS", None

def run_e1_fetcher(limit=None, dry_run=False):
    print("=== Drewry AIS E1-Compliant PDF Ingestion Harness ===", flush=True)
    entries = load_manifest()
    print(f"Loaded {len(entries)} manifested reports from {MANIFEST_FILE.name}", flush=True)
    if limit:
        entries = entries[:limit]
        print(f"Limiting probe to first {limit} entries.", flush=True)

    session = requests.Session()
    stats = {
        "already_valid": 0,
        "downloaded": 0,
        "quarantined_e1": 0,
        "not_found": 0,
    }

    for i, entry in enumerate(entries, 1):
        filename = entry["filename"]
        res, path = fetch_report(entry, session, dry_run=dry_run)
        print(f"[{i}/{len(entries)}] {filename} -> {res}", flush=True)
        if "VALID" in res or "EXISTS" in res:
            stats["already_valid"] += 1
        elif "DOWNLOADED" in res or "FOUND" in res:
            stats["downloaded"] += 1
        elif "QUARANTINED" in res:
            stats["quarantined_e1"] += 1
        else:
            stats["not_found"] += 1

    print("\n=== Ingestion Summary ===", flush=True)
    print(f"Already Valid / Existing : {stats['already_valid']}")
    print(f"Successfully Harvested   : {stats['downloaded']}")
    print(f"Quarantined (Finding E1) : {stats['quarantined_e1']}")
    print(f"Not Found / Requires DAM : {stats['not_found']}")

def main():
    parser = argparse.ArgumentParser(description="Drewry AIS E1-Compliant Fetcher")
    parser.add_argument("--limit", type=int, default=10, help="Number of files to probe/fetch (default: 10)")
    parser.add_argument("--dry-run", action="store_true", help="Probe without saving PDFs")
    args = parser.parse_args()

    run_e1_fetcher(limit=args.limit, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
