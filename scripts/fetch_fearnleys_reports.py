"""
fetch_fearnleys_reports.py
Harvests all historical Fearnleys weekly market research reports from Hasura GraphQL backend.
Saves:
  1. reports/fearnleys_reports_catalog.json & data/reports/fearnleys_reports_catalog.json
  2. reports/fearnleys/{date}_{slug}.md & data/reports/fearnleys/{date}_{slug}.md
"""

import json
import os
import re
import time
import requests

ENDPOINT = "https://pbrokerapp.hasura.app/v1/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) FearnpulseHarvester/1.0",
}

QUERY = """
query GetAllReports($limit: Int!, $offset: Int!) {
  custom_report(
    limit: $limit
    offset: $offset
    order_by: {date: desc}
  ) {
    id
    date
    title
    department
    slug
    status
    pdf_url
    audio_url
    content
    created_at
    updated_at
  }
}
"""


def slugify(text: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", text or "").strip("_")
    return s.lower() or "report"


def blocks_to_markdown(report: dict) -> str:
    lines = []
    title = (report.get("title") or "Untitled Report").strip()
    date = report.get("date") or "Unknown Date"
    dept = report.get("department") or "General"
    pdf_url = report.get("pdf_url") or ""

    lines.append(f"# {title}\n")
    lines.append(f"**Date**: {date} | **Department**: {dept}")
    if pdf_url:
        lines.append(f" | **PDF**: [{os.path.basename(pdf_url)}]({pdf_url})")
    lines.append("\n---\n")

    content_blocks = report.get("content") or []
    if isinstance(content_blocks, str):
        try:
            content_blocks = json.loads(content_blocks)
        except Exception:
            content_blocks = [{"type": "paragraph", "content": content_blocks}]

    for block in content_blocks:
        if not isinstance(block, dict):
            continue
        btype = block.get("type", "")
        bcontent = block.get("content", "")
        btitle = block.get("title", "")

        if btype == "heading1":
            lines.append(f"\n## {bcontent}\n")
        elif btype == "heading2":
            lines.append(f"\n### {bcontent}\n")
        elif btype in ("fullWidthParagraph", "paragraph"):
            lines.append(f"{bcontent}\n")
        elif btype in ("halfWidthChartFromFile", "fullWidthChartFromFile", "chart"):
            chart_title = btitle or "Indicator Chart"
            chart_url = bcontent
            lines.append(f"\n![{chart_title}]({chart_url})\n*Figure: {chart_title}*\n")
        elif btype == "pageBreak":
            lines.append("\n---\n")

    return "\n".join(lines)


def fetch_all_reports():
    dirs = [
        os.path.join("..", "reports", "fearnleys"),
        os.path.join("..", "data", "reports", "fearnleys"),
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)

    catalog_paths = [
        os.path.join("..", "reports", "fearnleys_reports_catalog.json"),
        os.path.join("..", "data", "reports", "fearnleys_reports_catalog.json"),
    ]

    limit = 50
    offset = 0
    all_reports = []

    print("Fetching Fearnleys weekly custom research reports...", flush=True)
    while True:
        resp = requests.post(
            ENDPOINT,
            json={"query": QUERY, "variables": {"limit": limit, "offset": offset}},
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"GraphQL error: {data['errors']}")

        batch = data.get("data", {}).get("custom_report", [])
        if not batch:
            break

        all_reports.extend(batch)
        print(f"  Fetched {len(all_reports)} reports so far...", flush=True)
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.2)

    print(f"Total reports fetched: {len(all_reports)}", flush=True)

    # Save catalogs
    for cp in catalog_paths:
        with open(cp, "w", encoding="utf-8") as f:
            json.dump(all_reports, f, indent=2, ensure_ascii=False)
        print(f"Saved master catalog to {cp}", flush=True)

    # Convert and write individual markdown reports
    count_saved = 0
    for rep in all_reports:
        rep_date = rep.get("date") or "undated"
        rep_slug = rep.get("slug") or slugify(rep.get("title") or rep.get("id"))
        filename = f"{rep_date}_{rep_slug}.md"

        md_content = blocks_to_markdown(rep)
        for d in dirs:
            filepath = os.path.join(d, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(md_content)
        count_saved += 1

    print(f"Successfully generated {count_saved} markdown reports in reports/fearnleys/ and data/reports/fearnleys/\n", flush=True)
    return len(all_reports)


if __name__ == "__main__":
    fetch_all_reports()
