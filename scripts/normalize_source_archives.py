from __future__ import annotations

import argparse
import os
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from source_archive_utils_v2 import (
    REPORTS_ROOT,
    clean_node_text,
    humanize_slug,
    make_soup,
    remove_empty_tags,
    repair_text,
    standard_archive_html,
    strip_attrs,
    unwrap_redundant_containers,
)


BREAKWAVE_ROOT = REPORTS_ROOT / "breakwave"
HELLENIC_ROOT = REPORTS_ROOT / "hellenic"
TEMP_ROOT = REPORTS_ROOT.parent / ".tmp_source_normalize"
MONTH_PATTERN = (
    "january|february|march|april|may|june|july|august|"
    "september|october|november|december"
)


def atomic_write(path: Path, content: str) -> None:
    TEMP_ROOT.mkdir(exist_ok=True)
    temp_path = TEMP_ROOT / f"{path.name}.tmp"
    temp_path.write_text(content, encoding="utf-8")
    os.replace(temp_path, path)


def parse_any_date(text: str) -> datetime | None:
    value = repair_text(text)
    for fmt in (
        "%B %d, %Y",
        "%b %d, %Y",
        "%d %B %Y",
        "%d %b %Y",
        "%Y-%m-%d",
        "%d/%m/%Y",
    ):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    match = re.search(r"(\d{2})[/\-](\d{2})[/\-](\d{4})", value)
    if match:
        try:
            return datetime(int(match.group(3)), int(match.group(2)), int(match.group(1)))
        except ValueError:
            return None

    match = re.search(rf"({MONTH_PATTERN})-(\d{{1,2}})-(20\d{{2}})", value.lower())
    if match:
        try:
            return datetime.strptime(
                f"{match.group(1)} {match.group(2)} {match.group(3)}",
                "%B %d %Y",
            )
        except ValueError:
            return None
    return None


def extract_archive_url(page_soup: BeautifulSoup, domain_hint: str) -> str:
    for selector in ["meta[name='archive-url']", ".meta a[href]", "a[href]"]:
        for element in page_soup.select(selector):
            href = repair_text(element.get("content") or element.get("href"))
            if href and domain_hint in href:
                return href
    return ""


def extract_breakwave_date(page_soup: BeautifulSoup, url: str) -> str:
    candidates = []
    meta_date = page_soup.select_one("meta[name='archive-date']")
    if meta_date:
        candidates.append(meta_date.get("content", ""))
    meta_block = page_soup.select_one(".meta")
    if meta_block:
        candidates.append(meta_block.get_text(" ", strip=True))
    candidates.append(url)
    for candidate in candidates:
        parsed = parse_any_date(candidate)
        if parsed:
            return parsed.strftime("%B %d, %Y")
    match = re.search(r"/(20\d{2})/(\d{1,2})/(\d{1,2})/", url)
    if match:
        try:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3))).strftime("%B %d, %Y")
        except ValueError:
            pass
    return "unknown"


def extract_hellenic_date(page_soup: BeautifulSoup, url: str) -> str:
    candidates = []
    meta_date = page_soup.select_one("meta[name='archive-date']")
    if meta_date:
        candidates.append(meta_date.get("content", ""))
    meta_block = page_soup.select_one(".meta")
    if meta_block:
        candidates.append(meta_block.get_text(" ", strip=True))
    if page_soup.body:
        candidates.append(page_soup.body.get_text(" ", strip=True))
    candidates.append(url)
    for candidate in candidates:
        parsed = parse_any_date(candidate)
        if parsed:
            return parsed.strftime("%d %B %Y")
    return "unknown"


def first_meaningful_text(root: Tag | None, *, minimum_length: int = 18) -> str:
    if root is None:
        return ""
    for tag in root.find_all(["h1", "h2", "h3", "strong", "p", "li"], limit=60):
        text = repair_text(tag.get_text(" ", strip=True))
        if len(text) >= minimum_length:
            return text
    return ""


def title_from_url(url: str) -> str:
    slug = urlparse(url).path.rstrip("/").split("/")[-1]
    slug = slug.replace("-amp-", "-and-")
    return humanize_slug(slug).title()


def normalize_breakwave_body(body: Tag, html_path: Path) -> str:
    markup = str(body) if body.name == "section" else f"<section>{str(body)}</section>"
    fragment = make_soup(markup).section
    if fragment is None:
        return "<p><em>No content extracted - visit the original URL.</em></p>"

    for tag in fragment.select("script, style, noscript, nav, header, footer, form, button, aside, svg"):
        tag.decompose()

    for iframe in list(fragment.find_all("iframe")):
        src = repair_text(iframe.get("src") or iframe.get("data-url"))
        note = fragment.new_tag("div")
        note["class"] = "archive-note"
        note.string = f"Embedded chart: {src}" if src else "Embedded chart removed during archiving"
        iframe.replace_with(note)

    for img in list(fragment.find_all("img")):
        src = repair_text(img.get("src") or img.get("data-src"))
        if not src:
            img.decompose()
            continue
        if src.startswith("http"):
            img.decompose()
            continue
        asset_path = (html_path.parent / src).resolve()
        if not asset_path.exists():
            img.decompose()
            continue
        img["src"] = src
        img["loading"] = "lazy"

    clean_node_text(fragment)
    strip_attrs(fragment)
    unwrap_redundant_containers(fragment)
    remove_empty_tags(fragment)
    return str(fragment)


def normalize_hellenic_body(body: Tag, html_path: Path) -> str:
    markup = str(body) if body.name == "section" else f"<section>{str(body)}</section>"
    fragment = make_soup(markup).section
    if fragment is None:
        return "<p><em>Content not extracted - visit original URL.</em></p>"

    for tag in fragment.select(
        "script, style, noscript, iframe, select, form, button, svg, .sharedaddy, .jp-relatedposts, .share-post"
    ):
        tag.decompose()

    for tag in fragment.select("h1, div[itemprop='author'], [aria-label='Language Translate Widget']"):
        tag.decompose()

    for container in list(fragment.find_all(["div", "ul", "li", "span"])):
        text = repair_text(container.get_text(" ", strip=True)).lower()
        hrefs = " ".join(repair_text(anchor.get("href", "")) for anchor in container.find_all("a", href=True)).lower()
        has_local_image = any(
            repair_text(img.get("src", "")).strip()
            and not repair_text(img.get("src", "")).startswith("http")
            for img in container.find_all("img", src=True)
        )
        if "translate.google.com" in hrefs and not has_local_image:
            container.decompose()
            continue
        if ("facebook.com/sharer" in hrefs or "pinterest.com/pin/create" in hrefs) and not has_local_image:
            container.decompose()
            continue
        if "powered by" in text and "translate" in text and not has_local_image:
            container.decompose()
            continue
        if text in {"share", "save"} and container.name in {"div", "li", "span"} and not has_local_image:
            container.decompose()
            continue

    for paragraph in list(fragment.find_all("p")):
        text = repair_text(paragraph.get_text(" ", strip=True))
        if text.lower().startswith("in ") and "report / analysis" in text.lower():
            paragraph.decompose()

    for span in list(fragment.find_all("span")):
        text = repair_text(span.get_text(" ", strip=True))
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            span.decompose()

    for img in list(fragment.find_all("img")):
        src = repair_text(img.get("src") or img.get("data-src") or img.get("data-lazy-src"))
        if not src or src.startswith("http"):
            img.decompose()
            continue
        asset_path = (html_path.parent / src).resolve()
        if asset_path.exists() and asset_path.stat().st_size < 5000:
            img.decompose()
            continue
        img["src"] = src
        img["loading"] = "lazy"

    for anchor in fragment.find_all("a", href=True):
        href = repair_text(anchor.get("href", ""))
        if href.startswith("http") and ".pdf" in href.lower():
            pdf_name = Path(urlparse(href).path).name
            local_matches = list((html_path.parent.parent / "pdfs").glob(f"*{pdf_name}"))
            if local_matches:
                anchor["href"] = f"../pdfs/{local_matches[0].name}"
            else:
                anchor.decompose()
        elif ".pdf" in href.lower():
            pdf_name = Path(href).name
            local_path = (html_path.parent.parent / "pdfs" / pdf_name).resolve()
            if local_path.exists():
                anchor["href"] = f"../pdfs/{pdf_name}"
            else:
                anchor.decompose()

    clean_node_text(fragment)
    strip_attrs(fragment)
    unwrap_redundant_containers(fragment)
    remove_empty_tags(fragment)
    return str(fragment)


def normalize_breakwave_file(path: Path, dry_run: bool) -> bool:
    page_soup = make_soup(path.read_text(encoding="utf-8"))
    url = extract_archive_url(page_soup, "breakwaveadvisors.com")
    body = (
        page_soup.select_one("body > section")
        or page_soup.select_one(".entry-content")
        or page_soup.select_one("article")
        or page_soup.body
    )
    title = ""
    for selector in ["meta[property='og:title']", "title", "h1"]:
        element = page_soup.select_one(selector)
        if element is None:
            continue
        if element.name == "meta":
            title = repair_text(element.get("content"))
        else:
            title = repair_text(element.get_text(" ", strip=True))
        if title:
            break
    if not title and url:
        title = title_from_url(url)
    if not title:
        title = first_meaningful_text(body) or path.stem

    tags: list[str] = []
    tag_line = page_soup.select_one(".tags")
    if tag_line:
        raw_tags = repair_text(tag_line.get_text(" ", strip=True)).replace("Tags:", "").strip()
        for part in re.split(r"\s+\|\s+|\s+·\s+|,\s*", raw_tags):
            if part:
                tags.append(part)

    html_doc = standard_archive_html(
        title=title,
        archive_source="breakwave_insights",
        source_url=url or "",
        published_date=extract_breakwave_date(page_soup, url),
        category="insights",
        body_html=normalize_breakwave_body(body, path),
        source_label="Breakwave Advisors",
        tags=tags,
        accent_color="#1a6b3c",
    )
    if dry_run:
        return True
    atomic_write(path, html_doc)
    return True


def normalize_hellenic_file(path: Path, dry_run: bool) -> bool:
    page_soup = make_soup(path.read_text(encoding="utf-8"))
    url = extract_archive_url(page_soup, "hellenicshippingnews.com")
    category = path.parent.parent.name
    body = (
        page_soup.select_one("body > section")
        or page_soup.select_one("article.post-listing .entry")
        or page_soup.select_one("article.post .entry")
        or page_soup.select_one("article.post-listing > div")
        or page_soup.select_one("article.post > div")
        or page_soup.select_one("article.post-listing")
        or page_soup.body
    )

    title = ""
    for selector in ["title", "h1.entry-title", "article h1", "h1"]:
        element = page_soup.select_one(selector)
        if element is None:
            continue
        title = repair_text(element.get_text(" ", strip=True))
        if selector == "title":
            title = re.sub(r"\s+\|\s+Hellenic Shipping News Worldwide$", "", title).strip()
        if title:
            break
    if not title and url:
        title = title_from_url(url)
    if not title:
        title = first_meaningful_text(body) or path.stem

    html_doc = standard_archive_html(
        title=title,
        archive_source="hellenic",
        source_url=url or "",
        published_date=extract_hellenic_date(page_soup, url),
        category=category,
        body_html=normalize_hellenic_body(body, path),
        source_label="Hellenic Shipping News",
        accent_color="#003366",
    )
    if dry_run:
        return True
    atomic_write(path, html_doc)
    return True


def iter_breakwave_files(year_filter: int | None) -> list[Path]:
    years = [str(year_filter)] if year_filter else sorted([item.name for item in BREAKWAVE_ROOT.iterdir() if item.is_dir() and item.name.isdigit()])
    files: list[Path] = []
    for year in years:
        files.extend(sorted((BREAKWAVE_ROOT / year).glob("*.html")))
    return files


def iter_hellenic_files(year_filter: int | None) -> list[Path]:
    files: list[Path] = []
    for category_dir in sorted(HELLENIC_ROOT.iterdir()):
        if not category_dir.is_dir():
            continue
        years = [str(year_filter)] if year_filter else sorted([item.name for item in category_dir.iterdir() if item.is_dir() and item.name.isdigit()])
        for year in years:
            files.extend(sorted((category_dir / year).glob("*.html")))
    return files


def run(source: str, year_filter: int | None, dry_run: bool) -> None:
    total = 0
    if source in {"all", "breakwave"}:
        files = iter_breakwave_files(year_filter)
        print(f"Breakwave files: {len(files)}")
        for index, path in enumerate(files, 1):
            if index % 250 == 0:
                print(f"  Breakwave {index}/{len(files)}")
            normalize_breakwave_file(path, dry_run)
        total += len(files)

    if source in {"all", "hellenic"}:
        files = iter_hellenic_files(year_filter)
        print(f"Hellenic files: {len(files)}")
        for index, path in enumerate(files, 1):
            if index % 250 == 0:
                print(f"  Hellenic {index}/{len(files)}")
            normalize_hellenic_file(path, dry_run)
        total += len(files)

    print(f"Normalized {total} archive files")


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize archived Breakwave and Hellenic HTML files")
    parser.add_argument("--source", choices=["all", "breakwave", "hellenic"], default="all")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(args.source, args.year, args.dry_run)


if __name__ == "__main__":
    main()
