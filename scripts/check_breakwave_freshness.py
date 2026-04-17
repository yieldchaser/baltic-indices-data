"""
Guardrail checks for Breakwave pipeline freshness.

Checks:
1) reports_vs_web      -> latest local Breakwave report date is not behind website.
2) signals_vs_reports  -> latest Breakwave signal date is not behind local reports.

Usage:
  python scripts/check_breakwave_freshness.py --check all
  python scripts/check_breakwave_freshness.py --check reports_vs_web
  python scripts/check_breakwave_freshness.py --check signals_vs_reports
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, datetime
from pathlib import Path

from source_archive_utils_v2 import REPO_ROOT, REPORTS_ROOT


SIGNALS_PATH = REPO_ROOT / "knowledge" / "derived" / "signals.jsonl"
_REPORT_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})_Breakwave_(Dry_Bulk|Tankers)\.pdf$", re.IGNORECASE)


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def latest_report_date(category: str) -> date | None:
    folder = "drybulk" if category == "drybulk" else "tankers"
    root = REPORTS_ROOT / folder
    latest: date | None = None
    for pdf in root.rglob("*.pdf"):
        match = _REPORT_RE.match(pdf.name)
        if not match:
            continue
        d = _parse_iso_date(match.group(1))
        if d and (latest is None or d > latest):
            latest = d
    return latest


def latest_signal_date(category: str) -> date | None:
    latest: date | None = None
    if not SIGNALS_PATH.exists():
        return None
    with SIGNALS_PATH.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("source") != "breakwave":
                continue
            if row.get("category") != category:
                continue
            d = _parse_iso_date(row.get("date"))
            if d and (latest is None or d > latest):
                latest = d
    return latest


def latest_web_date(category: str) -> date | None:
    import breakwave_scraper as bw  # imported lazily so this script can run offline-only checks

    bw_category = "dry" if category == "drybulk" else "tankers"
    links = bw.collect_links(bw_category, None)
    if not links:
        return None
    latest = max((item.get("date") for item in links if item.get("date") is not None), default=None)
    if latest is None:
        return None
    return latest.date()


def run_reports_vs_web() -> list[str]:
    errors: list[str] = []
    for category in ("drybulk", "tankers"):
        local_d = latest_report_date(category)
        web_d = latest_web_date(category)
        print(f"[reports_vs_web] {category}: local={local_d} web={web_d}")
        if web_d is None:
            errors.append(f"{category}: could not determine website latest date")
            continue
        if local_d is None:
            errors.append(f"{category}: no local reports found")
            continue
        if local_d < web_d:
            errors.append(f"{category}: local reports lag website ({local_d} < {web_d})")
    return errors


def run_signals_vs_reports() -> list[str]:
    errors: list[str] = []
    for category in ("drybulk", "tankers"):
        signal_d = latest_signal_date(category)
        report_d = latest_report_date(category)
        print(f"[signals_vs_reports] {category}: signals={signal_d} reports={report_d}")
        if report_d is None:
            errors.append(f"{category}: no local reports found")
            continue
        if signal_d is None:
            errors.append(f"{category}: no breakwave signals found")
            continue
        if signal_d < report_d:
            errors.append(f"{category}: signals lag reports ({signal_d} < {report_d})")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Breakwave freshness guardrails")
    parser.add_argument(
        "--check",
        choices=["all", "reports_vs_web", "signals_vs_reports"],
        default="all",
        help="Which guardrail to run.",
    )
    args = parser.parse_args()

    errors: list[str] = []
    if args.check in ("all", "reports_vs_web"):
        errors.extend(run_reports_vs_web())
    if args.check in ("all", "signals_vs_reports"):
        errors.extend(run_signals_vs_reports())

    if errors:
        print("[freshness] FAILED:")
        for err in errors:
            print(f"  - {err}")
        return 1

    print("[freshness] OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
