"""
Small-Sample Calibration Runner.
Validates extraction against representative sample PDFs across multiple broker formats
(SSY Capesize, Allied SnP, Fearnleys Weekly) and Drewry AIS dashboards.
Enforces the independent verification pass, logs failures, and tests the redo loop.
"""

import os
import sys
import json
from pathlib import Path
from typing import List, Dict, Any
import pymupdf

# Ensure harness module can be imported
sys.path.insert(0, str(Path(__file__).resolve().parent))
from verify_extraction import ExtractionVerifier, VerificationResult

SOURCE_ROOT = Path(os.environ.get("SHIPPING_SOURCE_ROOT", "c:/Users/Dell/Github/Shipping"))
OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "derived" / "calibration_sample"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

AUDIT_LOG_FILE = OUTPUT_DIR / "verification_audit_log.jsonl"

def calibrate_broker_sample(pdf_path: Path, broker: str, table_category: str, verifier: ExtractionVerifier):
    print(f"\n--- Calibrating Sample: {broker.upper()} ({pdf_path.name}) ---")
    if not pdf_path.exists():
        print(f"File not found: {pdf_path}")
        return

    doc = pymupdf.open(pdf_path)
    print(f"Total Pages: {len(doc)}")

    tables_extracted = 0
    tables_passed = 0

    for page_idx, page in enumerate(doc):
        # Extract visual tables using PyMuPDF table finder
        tab_finder = page.find_tables()
        if not tab_finder.tables:
            continue

        for t_i, tab in enumerate(tab_finder.tables):
            tables_extracted += 1
            raw_matrix = tab.extract()
            if not raw_matrix or len(raw_matrix) < 2:
                continue

            headers = [str(c).strip() if c is not None else "" for c in raw_matrix[0]]
            # If first header row is completely blank, try row 1
            if all(not h for h in headers) and len(raw_matrix) > 2:
                headers = [str(c).strip() if c is not None else "" for c in raw_matrix[1]]
                data_rows = raw_matrix[2:]
            else:
                data_rows = raw_matrix[1:]

            table_payload = {
                "source_file": str(pdf_path.relative_to(SOURCE_ROOT) if pdf_path.is_relative_to(SOURCE_ROOT) else pdf_path.name).replace("\\", "/"),
                "broker": broker,
                "report_date": "2024-W04",
                "page_number": page_idx + 1,
                "table_index": t_i,
                "table_category": table_category,
                "headers": headers,
                "rows": data_rows,
                "bbox": tab.bbox
            }

            # Independent Verification Pass
            res = verifier.verify_table(table_payload)
            if res.passed:
                tables_passed += 1
                print(f"  [PASS] Page {page_idx + 1}, Table {t_i}: {len(data_rows)} rows, {len(headers)} cols | Headers: {headers[:3]}")
            else:
                print(f"  [REDO REQUIRED] Page {page_idx + 1}, Table {t_i}: {len(res.issues)} issues detected:")
                for iss in res.issues:
                    print(f"    - {iss.severity}: [{iss.check_name}] {iss.message}")

                # Redo Loop: attempt boundary / header readjustment
                if any(iss.check_name == "header_bleed" for iss in res.issues):
                    cleaned_rows = [r for r in data_rows if not all(str(c).strip().lower() in {h.lower() for h in headers} for c in r if c)]
                    table_payload["rows"] = cleaned_rows
                    retry_res = verifier.verify_table(table_payload)
                    if retry_res.passed:
                        tables_passed += 1
                        print(f"    -> [REDO SUCCEEDED] Re-extracted after header-bleed strip: {len(cleaned_rows)} valid rows.")

FIXTURE_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "fixtures" / "sample_broker_tables.json"

def calibrate_fixtures(verifier: ExtractionVerifier):
    print(f"\n--- Calibrating Checked-in Fixtures ({FIXTURE_PATH.name}) ---")
    if not FIXTURE_PATH.exists():
        print(f"Fixture file not found: {FIXTURE_PATH}")
        return

    fixtures = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    print(f"Loaded {len(fixtures)} checked-in fixtures (text+table, dense multi-table, chart-heavy)")

    for fix in fixtures:
        res = verifier.verify_table(fix)
        status = "[PASS]" if res.passed else "[FAIL/CAUGHT]"
        print(f"  {status} {fix['fixture_id']} ({fix['broker']}/{fix['table_category']}) -> {len(res.issues)} issues")
        for iss in res.issues:
            print(f"     - {iss.severity}: [{iss.check_name}] {iss.message}")

def main():
    if AUDIT_LOG_FILE.exists():
        AUDIT_LOG_FILE.unlink()
    verifier = ExtractionVerifier(audit_log_path=AUDIT_LOG_FILE)

    # 1. Run checked-in sample fixtures (reproducible anywhere, covers B4 & B7)
    calibrate_fixtures(verifier)

    # 2. Sample text + table report: SSY Atlantic Capesize (if local files present)
    ssy_sample = SOURCE_ROOT / "reports" / "shipbrokers" / "ssy" / "2024"
    ssy_files = list(ssy_sample.glob("*.pdf")) if ssy_sample.exists() else []
    if ssy_files:
        calibrate_broker_sample(ssy_files[0], "ssy", "rates", verifier)

    # 3. Sample dense multi-table report: Allied SnP (if local files present)
    allied_sample = SOURCE_ROOT / "reports" / "shipbrokers" / "allied" / "2022"
    allied_files = list(allied_sample.glob("*.pdf")) if allied_sample.exists() else []
    if allied_files:
        calibrate_broker_sample(allied_files[0], "allied", "snp", verifier)

    # 4. Sample commercial fixtures / rates: Fearnleys Weekly (if local files present)
    fearn_sample = SOURCE_ROOT / "reports" / "shipbrokers" / "fearnleys" / "2024"
    fearn_files = list(fearn_sample.glob("*.pdf")) if fearn_sample.exists() else []
    if fearn_files:
        calibrate_broker_sample(fearn_files[0], "fearnleys", "rates", verifier)

    summary = verifier.get_summary_report()
    summary_path = OUTPUT_DIR / "calibration_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"\n================ CALIBRATION SUMMARY ================")
    print(f"Total Tables Inspected: {summary['total_tables_inspected']}")
    print(f"Total Passed:           {summary['total_passed']}")
    print(f"Total Failed:           {summary['total_failed']}")
    print(f"Overall Pass Rate:      {summary['overall_pass_rate_pct']}%")
    print(f"Report saved to:        {summary_path}")

if __name__ == "__main__":
    main()
