"""P1 PASS 2 - VERIFIER (muse-spark). Uses the committed shared harness.

Committed fixture script (M2/M3): imports ``ExtractionVerifier`` from
``calibration/p1/verify_table.py`` (the M3 shared-harness location) instead of
the pre-merge cross-worktree sibling import. No committed file imports from
``shipping-antigravity``. Results were cross-checked with the sibling harness
pre-merge (identical pass/fail + issue check names on these fixtures).
"""
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "calibration" / "p1"))
from verify_table import ExtractionVerifier  # committed shared harness (M3)

TMP = Path(__file__).resolve().parent
ex = json.loads((TMP / "p1_pass1_extract.json").read_text(encoding="utf-8"))
v = ExtractionVerifier(audit_log_path=TMP / "p1_verification_audit_log.jsonl")
A = ex["A"]; B = ex["B"]
results = {}

# A naive first attempt -> expect FAIL (row_count_mismatch 8 vs manual 5)
p1a = {"source_file": "reports/hellenic/demolition/pdfs/2021-07-03_best-oasis-weekly-recycling-market-report-02-july-2021_weekly-ship-recycling-report_137b264ac3ac.pdf",
       "broker": "hellenic", "page_number": 6, "table_index": 0,
       "table_category": "demolition", **A["pass1a"]}
r1a = v.verify_table(p1a, expected_rows=5, expected_cols=10)
results["A_pass1a"] = {"passed": r1a.passed, "rows": r1a.row_count,
                       "cols": r1a.column_count,
                       "issues": [(i.severity, i.check_name, i.message) for i in r1a.issues]}
# A redo -> expect PASS
p1b = {**p1a, **A["pass1b"]}
r1b = v.verify_table(p1b, expected_rows=5, expected_cols=10)
results["A_pass1b_redo"] = {"passed": r1b.passed, "rows": r1b.row_count,
                            "cols": r1b.column_count,
                            "issues": [(i.severity, i.check_name, i.message) for i in r1b.issues]}
# A bleed check: no leakage from p2 headline blocks or p7 contact block
blobA = json.dumps(A["pass1b"]["rows"])
results["A_bleed"] = {"GREECE_absent": "GREECE" not in blobA,
                      "p2_headline_absent": "Domestic prices" not in blobA,
                      "bbox": A["pymupdf_tables_p6"][0]["bbox"],
                      "page_rect": A["page_rect"]}

# B single notes table -> expect PASS
pB = {"source_file": "docs/BDRY-BWET_Form10-Q_March-31-2026.pdf",
      "broker": "amplify", "page_number": 6, "table_index": 0,
      "table_category": "generic", **B["pass1"]}
rB = v.verify_table(pB, expected_rows=9, expected_cols=4)
results["B"] = {"passed": rB.passed, "rows": rB.row_count,
                "cols": rB.column_count,
                "issues": [(i.severity, i.check_name, i.message) for i in rB.issues]}
blobB = json.dumps(B["pass1"]["rows"])
results["B_bleed"] = {
    "money_market_absent": "Invesco" not in blobB and "MONEY MARKET" not in blobB,
    "tanker_block_absent": "West Africa" not in blobB and "Middle East Gulf" not in blobB,
    "visual_fragmentation_first_pass": B["pymupdf_style_visual_tables_p6"]}
# B arithmetic tie-out to printed subtotal line
def num(s): return int(s.replace(",", "").replace("(", "-").replace(")", ""))
un = sum(num(r[1]) for r in B["pass1"]["rows"])
no = sum(num(r[2]) for r in B["pass1"]["rows"])
results["B_arithmetic"] = {"sum_unrealized": un, "subtotal_unrealized_ok": un == -2157385,
                           "sum_notional": no, "subtotal_notional_ok": no == 43916630,
                           "subtotal_line": B["pass1"]["subtotal_line"]}
results["harness_summary"] = v.get_summary_report()
(TMP / "p1_pass2_verify.json").write_text(json.dumps(results, indent=1), encoding="utf-8")
print(json.dumps(results, indent=1)[:4000])
