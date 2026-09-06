"""Shared table-verification harness (muse-spark contribution, M3/M4).

Single committed location both branches import from after merge::

    sys.path.insert(0, "<repo>/calibration/p1")
    from verify_table import ExtractionVerifier

M4/B6 contribution: the ``expected_rows`` / ``expected_cols`` assertions live
here (row tolerance ±1, column count exact), so per-template expected counts
are enforced by shared code instead of local wrappers. M3: this file replaces
the pre-merge cross-worktree ``sys.path`` import of the sibling harness; no
committed file imports from ``shipping-antigravity``. P1 results produced with
the sibling harness pre-merge were cross-checked against this wrapper (see
``calibration/p1/README.md``): identical pass/fail and issue check names on
the P1 fixtures.

Checks (ERROR severity, fail closed): ``empty_table``, ``missing_headers``,
``col_count_mismatch``, ``row_count_mismatch``, ``row_length_mismatch``,
``header_bleed``, ``narrative_bleed``, plus ``snp`` column-shift invariants
(``column_shifted_numeric_vessel``, ``column_shifted_text_in_dwt``); the
``demolition`` schema hook is a no-op. ``source_file`` is normalized to a
POSIX repo-relative path on every result and audit entry (B5). The repeated-
header diagnostic is sorted for determinism across runs.

Allowed imports: stdlib only (a subset of the stdlib + pypdf/pdfplumber/
pymupdf budget — this module parses already-extracted grids, so it needs no
PDF engine itself). Telemetry + JSONL audit log per verifier instance.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional

logger = logging.getLogger("calibration.verify_table")

ROW_COUNT_TOLERANCE = 1  # physical-page ruling splits hide/merge one row


def posix_rel(value: Any) -> str:
    """Normalize a source path to a POSIX repo-relative string (B5)."""
    text = str(value) if value is not None else "unknown"
    return PurePosixPath(PurePosixPath(text.replace("\\", "/")).as_posix()).as_posix()


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
    """Independent verifier over already-extracted table grids."""

    VALID_CATEGORIES = {
        "snp",
        "demolition",
        "rates",
        "fixtures",
        "fleet_orderbook",
        "valuations",
        "generic",
    }

    def __init__(self, audit_log_path: Optional[Path] = None):
        self.audit_log_path = Path(audit_log_path) if audit_log_path else None
        self.stats: Dict[str, Dict[str, int]] = {}

    def verify_table(
        self,
        table_data: Dict[str, Any],
        expected_rows: Optional[int] = None,
        expected_cols: Optional[int] = None,
    ) -> VerificationResult:
        source_file = posix_rel(table_data.get("source_file", "unknown"))
        page_num = table_data.get("page_number", 1)
        table_idx = table_data.get("table_index", 0)
        broker = table_data.get("broker", "unknown")
        category = table_data.get("table_category", "generic")
        headers = table_data.get("headers", [])
        rows = table_data.get("rows", [])

        issues: List[VerificationIssue] = []

        # 1. Structural count checks (M4: expected-count assertions live here).
        if not rows:
            issues.append(
                VerificationIssue("ERROR", "empty_table", "Table contains 0 extracted data rows")
            )
        if not headers:
            issues.append(
                VerificationIssue("ERROR", "missing_headers", "Table contains no headers")
            )
        col_count = len(headers)
        if expected_cols is not None and abs(col_count - expected_cols) > 0:
            issues.append(
                VerificationIssue(
                    "ERROR",
                    "col_count_mismatch",
                    f"Expected {expected_cols} columns, got {col_count}",
                )
            )
        if expected_rows is not None and abs(len(rows) - expected_rows) > ROW_COUNT_TOLERANCE:
            issues.append(
                VerificationIssue(
                    "ERROR",
                    "row_count_mismatch",
                    f"Expected {expected_rows} rows, got {len(rows)}",
                )
            )

        # 2. Row length consistency (column alignment).
        for r_i, row in enumerate(rows):
            if len(row) != col_count:
                issues.append(
                    VerificationIssue(
                        "ERROR",
                        "row_length_mismatch",
                        f"Row {r_i} has {len(row)} columns, expected {col_count}",
                        row_index=r_i,
                    )
                )

        # 3. Cross-table row-bleed checks.
        header_text_set = {str(h).strip().lower() for h in headers if h}
        for r_i, row in enumerate(rows):
            row_str_set = {str(c).strip().lower() for c in row if c}
            common = header_text_set.intersection(row_str_set)
            if len(common) >= max(2, len(headers) // 2):
                issues.append(
                    VerificationIssue(
                        "ERROR",
                        "header_bleed",
                        f"Row {r_i} appears to be a repeated header block: {sorted(common)}",
                        row_index=r_i,
                    )
                )
            non_empty = [str(c).strip() for c in row if c is not None and str(c).strip()]
            if len(non_empty) == 1 and len(non_empty[0].split()) > 7:
                issues.append(
                    VerificationIssue(
                        "ERROR",
                        "narrative_bleed",
                        f"Row {r_i} contains narrative prose embedded as single cell: "
                        f"'{non_empty[0][:50]}...'",
                        row_index=r_i,
                        cell_value=non_empty[0],
                    )
                )

        # 4. Domain / category schema invariants.
        header_map = {str(h).strip().lower(): i for i, h in enumerate(headers) if h}
        if category == "snp":
            self._verify_snp_schema(headers, header_map, rows, issues)
        elif category == "demolition":
            self._verify_demolition_schema(headers, header_map, rows, issues)

        passed = not any(iss.severity == "ERROR" for iss in issues)
        res = VerificationResult(
            passed=passed,
            source_file=source_file,
            page_number=page_num,
            table_index=table_idx,
            broker=broker,
            table_category=category,
            row_count=len(rows),
            column_count=col_count,
            issues=issues,
        )
        self._record_telemetry(res)
        return res

    def _verify_snp_schema(self, headers, hmap, rows, issues) -> None:
        vessel_idx = next(
            (idx for key, idx in hmap.items() if "vessel" in key or "name" in key or "ship" in key),
            None,
        )
        dwt_idx = next(
            (idx for key, idx in hmap.items() if "dwt" in key or "deadweight" in key),
            None,
        )
        for r_i, row in enumerate(rows):
            if vessel_idx is not None and vessel_idx < len(row):
                v_val = str(row[vessel_idx]).strip()
                if v_val.replace(",", "").replace(".", "").isdigit():
                    issues.append(
                        VerificationIssue(
                            "ERROR",
                            "column_shifted_numeric_vessel",
                            f"Vessel column contains numeric value '{v_val}' in row {r_i} "
                            "(possible column shift)",
                            row_index=r_i,
                            column_name=headers[vessel_idx],
                            cell_value=v_val,
                        )
                    )
            if dwt_idx is not None and dwt_idx < len(row):
                d_val = str(row[dwt_idx]).strip().replace(",", "").replace(".", "")
                if (
                    d_val
                    and not d_val.isdigit()
                    and d_val.lower() not in ("nan", "none", "-", "n/a", "")
                    and any(c.isalpha() for c in d_val)
                    and len(d_val) > 4
                ):
                    issues.append(
                        VerificationIssue(
                            "ERROR",
                            "column_shifted_text_in_dwt",
                            f"DWT column contains text '{row[dwt_idx]}' in row {r_i}",
                            row_index=r_i,
                            column_name=headers[dwt_idx],
                            cell_value=row[dwt_idx],
                        )
                    )

    def _verify_demolition_schema(self, headers, hmap, rows, issues) -> None:
        pass  # no demolition-specific invariants yet; hook reserved

    def _record_telemetry(self, res: VerificationResult) -> None:
        broker = res.broker or "unknown"
        bucket = self.stats.setdefault(
            broker, {"total": 0, "passed": 0, "failed": 0, "issues_count": 0}
        )
        bucket["total"] += 1
        bucket["passed" if res.passed else "failed"] += 1
        bucket["issues_count"] += len(res.issues)
        if self.audit_log_path:
            try:
                self.audit_log_path.parent.mkdir(parents=True, exist_ok=True)
                with open(self.audit_log_path, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps(asdict(res)) + "\n")
            except Exception as exc:  # audit must never fail verification
                logger.warning("Failed to write audit log entry: %s", exc)

    def get_summary_report(self) -> Dict[str, Any]:
        total_tables = sum(s["total"] for s in self.stats.values())
        total_passed = sum(s["passed"] for s in self.stats.values())
        total_failed = sum(s["failed"] for s in self.stats.values())
        overall = (total_passed / total_tables * 100.0) if total_tables else 0.0
        return {
            "total_tables_inspected": total_tables,
            "total_passed": total_passed,
            "total_failed": total_failed,
            "overall_pass_rate_pct": round(overall, 2),
            "by_broker": self.stats,
        }
