"""
Independent Verification Harness for Maritime Document Extraction.
Enforces structural invariants, prevents table loss, cross-table row-bleed,
and column shifting across batch runs.
"""

import json
import logging
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("extraction_verifier")

@dataclass
class VerificationIssue:
    severity: str  # "ERROR" or "WARNING"
    check_name: str
    message: str
    row_index: Optional[int] = None
    column_name: Optional[str] = None
    cell_value: Optional[Any] = None

@dataclass
class VerificationResult:
    passed: bool
    source_file: str
    page_number: int
    table_index: int
    broker: str
    table_category: str
    row_count: int
    column_count: int
    issues: List[VerificationIssue] = field(default_factory=list)
    verified_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

class ExtractionVerifier:
    """
    Independent verifier that inspects extracted table structures
    and confirms they conform to physical page constraints and domain invariants.
    """

    VALID_CATEGORIES = {"snp", "demolition", "rates", "fixtures", "fleet_orderbook", "valuations", "generic"}

    def __init__(self, audit_log_path: Optional[Path] = None):
        self.audit_log_path = audit_log_path
        self.stats: Dict[str, Dict[str, int]] = {}

    def verify_table(
        self,
        table_data: Dict[str, Any],
        expected_rows: Optional[int] = None,
        expected_cols: Optional[int] = None
    ) -> VerificationResult:
        source_file = table_data.get("source_file", "unknown")
        page_num = table_data.get("page_number", 1)
        table_idx = table_data.get("table_index", 0)
        broker = table_data.get("broker", "unknown")
        category = table_data.get("table_category", "generic")
        headers = table_data.get("headers", [])
        rows = table_data.get("rows", [])

        issues: List[VerificationIssue] = []

        # 1. Structural count checks
        if not rows:
            issues.append(VerificationIssue("ERROR", "empty_table", "Table contains 0 extracted data rows"))

        if not headers:
            issues.append(VerificationIssue("ERROR", "missing_headers", "Table contains no headers"))

        col_count = len(headers)
        if expected_cols is not None and abs(col_count - expected_cols) > 0:
            issues.append(VerificationIssue("ERROR", "col_count_mismatch", f"Expected {expected_cols} columns, got {col_count}"))

        if expected_rows is not None and abs(len(rows) - expected_rows) > 1:
            issues.append(VerificationIssue("ERROR", "row_count_mismatch", f"Expected {expected_rows} rows, got {len(rows)}"))

        # 2. Row length consistency & column alignment check
        for r_i, row in enumerate(rows):
            if len(row) != col_count:
                issues.append(VerificationIssue(
                    "ERROR", "row_length_mismatch",
                    f"Row {r_i} has {len(row)} columns, expected {col_count}",
                    row_index=r_i
                ))

        # 3. Cross-table row-bleed checks
        header_text_set = {str(h).strip().lower() for h in headers if h}
        for r_i, row in enumerate(rows):
            row_str_set = {str(c).strip().lower() for c in row if c}
            common = header_text_set.intersection(row_str_set)
            if len(common) >= max(2, len(headers) // 2):
                issues.append(VerificationIssue(
                    "ERROR", "header_bleed",
                    f"Row {r_i} appears to be a repeated header block: {common}",
                    row_index=r_i
                ))

            non_empty = [str(c).strip() for c in row if c is not None and str(c).strip()]
            if len(non_empty) == 1 and len(non_empty[0].split()) > 7:
                issues.append(VerificationIssue(
                    "ERROR", "narrative_bleed",
                    f"Row {r_i} contains narrative prose embedded as single cell: '{non_empty[0][:50]}...'",
                    row_index=r_i,
                    cell_value=non_empty[0]
                ))

        # 4. Domain & Category Schema Invariants
        header_map = {str(h).strip().lower(): i for i, h in enumerate(headers) if h}

        if category == "snp":
            self._verify_snp_schema(headers, header_map, rows, issues)
        elif category == "demolition":
            self._verify_demolition_schema(headers, header_map, rows, issues)

        has_errors = any(iss.severity == "ERROR" for iss in issues)
        passed = not has_errors

        res = VerificationResult(
            passed=passed,
            source_file=source_file,
            page_number=page_num,
            table_index=table_idx,
            broker=broker,
            table_category=category,
            row_count=len(rows),
            column_count=col_count,
            issues=issues
        )

        self._record_telemetry(res)
        return res

    def _verify_snp_schema(
        self,
        headers: List[str],
        hmap: Dict[str, int],
        rows: List[List[Any]],
        issues: List[VerificationIssue]
    ):
        vessel_idx = None
        for key, idx in hmap.items():
            if "vessel" in key or "name" in key or "ship" in key:
                vessel_idx = idx
                break

        dwt_idx = None
        for key, idx in hmap.items():
            if "dwt" in key or "deadweight" in key:
                dwt_idx = idx
                break

        for r_i, row in enumerate(rows):
            if vessel_idx is not None and vessel_idx < len(row):
                v_val = str(row[vessel_idx]).strip()
                if v_val.replace(",", "").replace(".", "").isdigit():
                    issues.append(VerificationIssue(
                        "ERROR", "column_shifted_numeric_vessel",
                        f"Vessel column contains numeric value '{v_val}' in row {r_i} (possible column shift)",
                        row_index=r_i, column_name=headers[vessel_idx], cell_value=v_val
                    ))

            if dwt_idx is not None and dwt_idx < len(row):
                d_val = str(row[dwt_idx]).strip().replace(",", "").replace(".", "")
                if d_val and not d_val.isdigit() and d_val.lower() not in ("nan", "none", "-", "n/a", ""):
                    if any(c.isalpha() for c in d_val) and len(d_val) > 4:
                        issues.append(VerificationIssue(
                            "ERROR", "column_shifted_text_in_dwt",
                            f"DWT column contains text '{row[dwt_idx]}' in row {r_i}",
                            row_index=r_i, column_name=headers[dwt_idx], cell_value=row[dwt_idx]
                        ))

    def _verify_demolition_schema(
        self,
        headers: List[str],
        hmap: Dict[str, int],
        rows: List[List[Any]],
        issues: List[VerificationIssue]
    ):
        pass

    def _record_telemetry(self, res: VerificationResult):
        broker = res.broker or "unknown"
        if broker not in self.stats:
            self.stats[broker] = {"total": 0, "passed": 0, "failed": 0, "issues_count": 0}

        self.stats[broker]["total"] += 1
        if res.passed:
            self.stats[broker]["passed"] += 1
        else:
            self.stats[broker]["failed"] += 1
        self.stats[broker]["issues_count"] += len(res.issues)

        if self.audit_log_path:
            try:
                self.audit_log_path.parent.mkdir(parents=True, exist_ok=True)
                with open(self.audit_log_path, "a", encoding="utf-8") as f:
                    entry = asdict(res)
                    f.write(json.dumps(entry) + "\n")
            except Exception as e:
                logger.warning(f"Failed to write audit log entry: {e}")

    def get_summary_report(self) -> Dict[str, Any]:
        total_tables = sum(s["total"] for s in self.stats.values())
        total_passed = sum(s["passed"] for s in self.stats.values())
        total_failed = sum(s["failed"] for s in self.stats.values())
        overall_pass_rate = (total_passed / total_tables * 100.0) if total_tables > 0 else 0.0

        return {
            "total_tables_inspected": total_tables,
            "total_passed": total_passed,
            "total_failed": total_failed,
            "overall_pass_rate_pct": round(overall_pass_rate, 2),
            "by_broker": self.stats
        }
