from __future__ import annotations

import argparse
from pathlib import Path

from source_archive_utils_v2 import REPORTS_ROOT, make_soup, repair_text


BREAKWAVE_ROOT = REPORTS_ROOT / "breakwave"
HELLENIC_ROOT = REPORTS_ROOT / "hellenic"
REQUIRED_META = (
    "archive-source",
    "archive-category",
    "archive-url",
    "archive-date",
)
CLUTTER_PATTERNS = (
    "platform.twitter.com",
    "Google Translate",
    "Facebook Social Plugin",
    "share-post",
    "Select Language",
)


def iter_breakwave_files(year_filter: int | None) -> list[Path]:
    years = [str(year_filter)] if year_filter else sorted(
        [item.name for item in BREAKWAVE_ROOT.iterdir() if item.is_dir() and item.name.isdigit()]
    )
    files: list[Path] = []
    for year in years:
        files.extend(sorted((BREAKWAVE_ROOT / year).glob("*.html")))
    return files


def iter_hellenic_files(year_filter: int | None) -> list[Path]:
    files: list[Path] = []
    for category_dir in sorted(HELLENIC_ROOT.iterdir()):
        if not category_dir.is_dir():
            continue
        years = [str(year_filter)] if year_filter else sorted(
            [item.name for item in category_dir.iterdir() if item.is_dir() and item.name.isdigit()]
        )
        for year in years:
            files.extend(sorted((category_dir / year).glob("*.html")))
    return files


def check_file(path: Path) -> list[str]:
    issues: list[str] = []
    page_soup = make_soup(path.read_text(encoding="utf-8"))

    title = repair_text((page_soup.title.string if page_soup.title else "") or "")
    h1 = repair_text((page_soup.find("h1").get_text(" ", strip=True) if page_soup.find("h1") else ""))
    if not title:
        issues.append("missing title")
    if not h1:
        issues.append("missing h1")

    for meta_name in REQUIRED_META:
        meta_tag = page_soup.find("meta", attrs={"name": meta_name})
        if meta_tag is None or not repair_text(meta_tag.get("content")):
            issues.append(f"missing meta:{meta_name}")

    body_text = repair_text(page_soup.body.get_text(" ", strip=True) if page_soup.body else "")
    if len(body_text) < 80:
        issues.append("body too short")

    raw_html = path.read_text(encoding="utf-8")
    for pattern in CLUTTER_PATTERNS:
        if pattern in raw_html:
            issues.append(f"leftover clutter:{pattern}")

    for img in page_soup.find_all("img", src=True):
        src = repair_text(img["src"])
        if src.startswith("http"):
            issues.append("remote image ref")
            continue
        asset_path = (path.parent / src).resolve()
        if not asset_path.exists():
            issues.append(f"missing image:{src}")

    for anchor in page_soup.find_all("a", href=True):
        href = repair_text(anchor["href"])
        if ".pdf" not in href.lower() or href.startswith("http"):
            continue
        asset_path = (path.parent / href).resolve()
        if not asset_path.exists():
            issues.append(f"missing pdf:{href}")

    return issues


def run(source: str, year_filter: int | None, max_show: int) -> int:
    files: list[Path] = []
    if source in {"all", "breakwave"}:
        files.extend(iter_breakwave_files(year_filter))
    if source in {"all", "hellenic"}:
        files.extend(iter_hellenic_files(year_filter))

    total_issues = 0
    shown = 0
    for path in files:
        issues = check_file(path)
        if issues:
            total_issues += len(issues)
            if shown < max_show:
                print(f"{path}: {', '.join(sorted(set(issues)))}")
                shown += 1

    print(f"Validated {len(files)} archive files")
    print(f"Total issues: {total_issues}")
    return 0 if total_issues == 0 else 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate normalized Breakwave and Hellenic archive HTML files")
    parser.add_argument("--source", choices=["all", "breakwave", "hellenic"], default="all")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--max-show", type=int, default=25)
    args = parser.parse_args()
    raise SystemExit(run(args.source, args.year, args.max_show))


if __name__ == "__main__":
    main()
