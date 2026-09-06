"""
P0 Work Item: Linked Assets Skipped Queue Builder.
Enumerates the 8,424 skipped linked assets from knowledge/manifests/documents.jsonl,
recovers parent-document attribution (doc_id, date, source, section),
and resolves local disk paths for chart/image assets.
"""

import os
import sys
import json
from pathlib import Path
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from collections import Counter

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SOURCE_ROOT = Path(os.environ.get("SHIPPING_SOURCE_ROOT", str(REPO_ROOT)))
DOCUMENTS_JSONL = SOURCE_ROOT / "knowledge" / "manifests" / "documents.jsonl"
OUTPUT_FILE = REPO_ROOT / "data" / "derived" / "p0_skipped_assets_queue.jsonl"
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

def find_local_image_mirror(html_path: Path, href: str) -> Path:
    # 1. Look in adjacent assets/ folder
    stem = html_path.stem
    parent = html_path.parent
    assets_dir = parent / "assets"
    if assets_dir.exists():
        # Match by filename or slug
        slug = Path(urlparse(href).path).name
        if slug:
            matches = list(assets_dir.glob(f"*{slug}*"))
            if matches:
                return matches[0]

    # 2. Look in reports/breakwave/images/ or reports/hellenic/
    slug = Path(urlparse(href).path).name
    if slug:
        common_img = SOURCE_ROOT / "reports" / "breakwave" / "images"
        if common_img.exists():
            matches = list(common_img.glob(f"*{slug}*"))
            if matches:
                return matches[0]

    return None

def build_queue(max_docs: int = None):
    print(f"Reading manifest from: {DOCUMENTS_JSONL}")
    if not DOCUMENTS_JSONL.exists():
        print("Manifest not found.")
        return

    records = []
    total_skipped_tally = 0
    resolved_local_count = 0

    with open(DOCUMENTS_JSONL, "r", encoding="utf-8") as f:
        for line in f:
            d = json.loads(line)
            skipped_cnt = d.get("linked_assets_skipped", 0)
            if skipped_cnt <= 0:
                continue

            total_skipped_tally += skipped_cnt
            doc_id = d.get("doc_id")
            source = d.get("source")
            cat = d.get("category")
            date_str = d.get("date")
            src_path_str = d.get("source_path")
            if not src_path_str:
                continue

            src_path = SOURCE_ROOT / src_path_str
            if not src_path.exists():
                continue

            try:
                soup = BeautifulSoup(src_path.read_text(encoding="utf-8", errors="ignore"), "html.parser")
            except Exception:
                continue

            # Find image and link candidates
            links = []
            for img in soup.find_all("img"):
                u = img.get("src") or img.get("data-src")
                if u:
                    links.append((u, "image/chart"))
            for a in soup.find_all("a", href=True):
                u = a.get("href")
                if u and any(u.lower().endswith(ext) for ext in [".pdf", ".xlsx", ".csv"]):
                    links.append((u, "document"))

            for idx, (url, asset_type) in enumerate(links):
                local_path = find_local_image_mirror(src_path, url)
                is_resolved = local_path is not None
                if is_resolved:
                    resolved_local_count += 1

                records.append({
                    "asset_id": f"{doc_id}__asset_{idx:02d}",
                    "parent_doc_id": doc_id,
                    "parent_date": date_str,
                    "source": source,
                    "category": cat,
                    "source_html_path": src_path_str,
                    "asset_url": url,
                    "asset_type": asset_type,
                    "local_path": str(local_path.relative_to(SOURCE_ROOT)) if local_path else None,
                    "is_resolved_local": is_resolved,
                    "status": "ready_for_vision_stage1" if is_resolved else "pending_mirror"
                })

            if max_docs and len(records) >= max_docs:
                break

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        for r in records:
            out.write(json.dumps(r) + "\n")

    print(f"\n--- P0 Skipped Assets Queue Summary ---")
    print(f"Total Skipped In Manifest: {total_skipped_tally}")
    print(f"Total Candidates Extracted: {len(records)}")
    print(f"Resolved to Local Disk:    {resolved_local_count}")
    print(f"Queue File Created at:     {OUTPUT_FILE}")

if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    build_queue(limit)
