"""
Breakwave Advisors Report Scraper  v1.1
========================================
Downloads Dry Bulk and Tanker biweekly PDFs from breakwaveadvisors.com.

Output structure:
  C:/Users/Dell/Github/Shipping/reports/
    drybulk/
      2026/  2025/  2024/  ...  2018/
    tankers/
      2026/  2025/  2024/  2023/

Usage:
    python breakwave_scraper.py                    # download everything
    python breakwave_scraper.py --dry-run          # list what would be downloaded
    python breakwave_scraper.py --category dry     # dry bulk only
    python breakwave_scraper.py --category tankers # tankers only
    python breakwave_scraper.py --year 2024        # single year

Install deps first:
    pip install requests beautifulsoup4 lxml
"""

import os
import re
import sys
import time
import argparse
import requests
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime
from bs4 import BeautifulSoup, Tag

# ─────────────────────────── Config ──────────────────────────────────────────

BASE_URL    = "https://www.breakwaveadvisors.com"
OUTPUT_ROOT = Path(r"C:\Users\Dell\Github\Shipping\reports")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.breakwaveadvisors.com/",
}

PAGE_DELAY     = 1.5   # sec between page fetches
DOWNLOAD_DELAY = 2.5   # sec between PDF downloads

# ─────────────────────── Known archive pages ──────────────────────────────────
# Keys are the year (int) or "current" for the /publications page
# Values are URL paths

DRY_PAGES = {
    "current": "/publications",
    2025: "/2025-dry-reports",
    2024: "/2024-dry-reports",
    2023: "/2023-reports",
    2022: "/2022-reports",
    "older": "/older-reports",   # covers 2021 and earlier, no year filter
}

TANKER_PAGES = {
    "current": "/publications",
    2025: "/2025-tanker-report",   # singular
    2024: "/2024-tanker-report",   # singular
    2023: "/2023-tanker-reports",
}

# ─────────────────────────── Session ─────────────────────────────────────────

session = requests.Session()
session.headers.update(HEADERS)

# ─────────────────────────── Utilities ───────────────────────────────────────

DATE_RE = re.compile(
    r"^(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},?\s+\d{4}$",
    re.IGNORECASE,
)


def parse_date(text: str) -> datetime | None:
    text = re.sub(r"\s+", " ", text.strip())
    for fmt in ("%B %d, %Y", "%B %d %Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    return None


def is_date_link(a: Tag) -> bool:
    return bool(DATE_RE.match(re.sub(r"\s+", " ", a.get_text(strip=True))))


def is_pdf(url: str) -> bool:
    path = urlparse(url).path.lower()
    return path.endswith(".pdf")


def fetch_soup(url: str, retries: int = 3) -> BeautifulSoup | None:
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=25)
            r.raise_for_status()
            return BeautifulSoup(r.content, "lxml")
        except Exception as e:
            print(f"    ⚠  [{attempt+1}/{retries}] {url}: {e}")
            if attempt < retries - 1:
                time.sleep(4)
    return None


def resolve_to_pdf(href: str, source_page: str) -> str | None:
    """
    Given a link href (possibly relative), resolve to the actual PDF URL.
    Handles three cases:
      1. href IS a .pdf URL → return directly
      2. href is a Squarespace static URL → return directly
      3. href is an intermediate page → fetch it and hunt for a PDF link
    """
    url = urljoin(source_page, href)

    if is_pdf(url):
        return url

    if "squarespace.com/static" in url:
        return url  # usually a direct file even without .pdf suffix

    # Follow the link
    time.sleep(PAGE_DELAY)
    soup = fetch_soup(url)
    if soup is None:
        return None

    # Search for embedded PDF links
    for tag in soup.find_all(True):
        for attr in ("href", "src", "data", "action"):
            val = tag.get(attr, "")
            if val and (is_pdf(val) or "squarespace.com/static" in val):
                return urljoin(url, val)

    # Look at <script> JSON blobs that sometimes contain CDN URLs
    for script in soup.find_all("script"):
        txt = script.string or ""
        m = re.search(r'(https://[^\s"\']+\.pdf)', txt)
        if m:
            return m.group(1)

    return None


def make_filename(category: str, date: datetime, pdf_url: str) -> str:
    cat = "Dry_Bulk" if category == "dry" else "Tankers"
    ext = Path(urlparse(pdf_url).path).suffix or ".pdf"
    if not ext.startswith("."):
        ext = ".pdf"
    return f"{date.strftime('%Y-%m-%d')}_Breakwave_{cat}{ext}"


def download(url: str, dest: Path, dry_run: bool) -> bool:
    if dry_run:
        print(f"    [DRY RUN] → {dest}")
        return True

    if dest.exists() and dest.stat().st_size > 10_000:
        print(f"    ✓ skip (exists): {dest.name}")
        return True

    dest.parent.mkdir(parents=True, exist_ok=True)

    try:
        r = session.get(url, timeout=90, stream=True)
        r.raise_for_status()
        ct = r.headers.get("content-type", "")
        if "html" in ct and "pdf" not in ct:
            # Try to find PDF redirect in response body
            soup = BeautifulSoup(r.content, "lxml")
            for tag in soup.find_all(True):
                for attr in ("href", "src"):
                    val = tag.get(attr, "")
                    if val and is_pdf(val):
                        return download(urljoin(url, val), dest, dry_run)
            print(f"    ✗ Not a PDF ({ct}): {url}")
            return False

        with open(dest, "wb") as f:
            for chunk in r.iter_content(65536):
                f.write(chunk)

        kb = dest.stat().st_size / 1024
        print(f"    ↓ {dest.name}  ({kb:.0f} KB)")
        return True

    except Exception as e:
        print(f"    ✗ {e}")
        if dest.exists():
            dest.unlink()
        return False


# ──────────────────────── Page parsers ───────────────────────────────────────

def scrape_archive_page(path: str, expected_year: int | None = None) -> list[dict]:
    """Scrape a simple archive page — just date links."""
    url = BASE_URL + path
    print(f"\n  📄 {url}")
    soup = fetch_soup(url)
    if soup is None:
        return []

    items = []
    for a in soup.find_all("a", href=True):
        if not is_date_link(a):
            continue
        date = parse_date(a.get_text(strip=True))
        if date is None:
            continue
        if expected_year and date.year != expected_year:
            continue
        items.append({"date": date, "href": a["href"], "source": url})

    print(f"     → {len(items)} date links found")
    time.sleep(PAGE_DELAY)
    return items


def scrape_publications_page(category: str) -> list[dict]:
    """
    Scrape /publications for the *current* year.
    Dry Bulk and Tankers sections are in separate containers — we isolate
    each by finding the section heading and walking its siblings/parent.
    """
    url = BASE_URL + "/publications"
    print(f"\n  📄 {url}  [looking for: {'DRY BULK' if category=='dry' else 'TANKER'}]")
    soup = fetch_soup(url)
    if soup is None:
        return []

    # Find section heading
    target_kw = "DRY BULK BIWEEKLY" if category == "dry" else "TANKERS BIWEEKLY"
    heading_el = None
    for el in soup.find_all(True):
        if target_kw in el.get_text(separator=" ").upper():
            # prefer the tightest element that contains the text
            if heading_el is None or len(el.get_text()) < len(heading_el.get_text()):
                heading_el = el

    if heading_el is None:
        print(f"     ⚠  Could not find '{target_kw}' section heading")
        # Fallback: return all date links from 2026
        items = []
        for a in soup.find_all("a", href=True):
            if not is_date_link(a):
                continue
            date = parse_date(a.get_text(strip=True))
            if date and date.year >= 2026:
                items.append({"date": date, "href": a["href"], "source": url})
        print(f"     → {len(items)} fallback links (2026+)")
        return items

    # Walk up to find the section container, then collect all date links in it
    # Try parent, grandparent, etc. until we find date links
    container = heading_el
    items = []
    for _ in range(6):
        container = container.parent
        if container is None:
            break
        links = [
            a for a in container.find_all("a", href=True) if is_date_link(a)
        ]
        if links:
            for a in links:
                date = parse_date(a.get_text(strip=True))
                if date and date.year >= 2026:
                    items.append({"date": date, "href": a["href"], "source": url})
            break

    # Deduplicate by date
    seen = set()
    unique = []
    for it in items:
        k = it["date"].date()
        if k not in seen:
            seen.add(k)
            unique.append(it)

    print(f"     → {len(unique)} current-year links found")
    time.sleep(PAGE_DELAY)
    return unique


# ──────────────────────── Orchestration ──────────────────────────────────────

def collect_links(category: str, year_filter: int | None = None) -> list[dict]:
    pages = DRY_PAGES if category == "dry" else TANKER_PAGES
    all_items = []

    for key, path in pages.items():
        if year_filter:
            # Only process the matching year (or "current" for 2026)
            if key == "current":
                if year_filter < 2026:
                    continue
            elif key != year_filter:
                continue

        if key == "current":
            items = scrape_publications_page(category)
        elif key == "older":
            items = scrape_archive_page(path, expected_year=None)
        else:
            items = scrape_archive_page(path, expected_year=key)

        all_items.extend(items)

    # Deduplicate and sort newest-first
    seen = set()
    unique = []
    for it in sorted(all_items, key=lambda x: x["date"], reverse=True):
        k = it["date"].date()
        if k not in seen:
            seen.add(k)
            unique.append(it)

    return unique


def run(category: str, dry_run: bool, year_filter: int | None):
    label = "Dry Bulk" if category == "dry" else "Tankers"
    folder = "drybulk" if category == "dry" else "tankers"
    print(f"\n{'═'*62}")
    print(f"  Breakwave {label} — {'DRY RUN' if dry_run else 'DOWNLOAD'}")
    print(f"  Output root: {OUTPUT_ROOT / folder}")
    print(f"{'═'*62}")

    links = collect_links(category, year_filter)
    print(f"\n  ✅ {len(links)} unique report dates collected\n")

    ok = fail = 0
    for item in links:
        date: datetime = item["date"]
        href: str      = item["href"]
        source: str    = item["source"]

        print(f"  📅 {date.strftime('%Y-%m-%d')}  {href[:70]}")

        # Resolve href → PDF URL
        pdf_url = resolve_to_pdf(href, source)
        if pdf_url is None:
            print(f"     ✗ Could not resolve PDF")
            fail += 1
            continue

        print(f"     PDF: {pdf_url[:80]}")

        dest = OUTPUT_ROOT / folder / str(date.year) / make_filename(category, date, pdf_url)
        success = download(pdf_url, dest, dry_run)

        if success:
            ok += 1
        else:
            fail += 1

        if not dry_run:
            time.sleep(DOWNLOAD_DELAY)

    print(f"\n  {'─'*40}")
    print(f"  ✓ {ok} success   ✗ {fail} failed")
    print(f"  {'─'*40}\n")
    return ok, fail


# ─────────────────────────── Entry point ─────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Download Breakwave Advisors reports")
    p.add_argument("--category", choices=["dry", "tankers", "both"], default="both",
                   help="Which category to download (default: both)")
    p.add_argument("--dry-run", action="store_true",
                   help="List URLs without downloading")
    p.add_argument("--year", type=int, default=None,
                   help="Download only a specific year, e.g. --year 2024")
    args = p.parse_args()

    cats = ["dry", "tankers"] if args.category == "both" else [args.category]
    total_ok = total_fail = 0

    for cat in cats:
        ok, fail = run(cat, args.dry_run, args.year)
        total_ok += ok
        total_fail += fail

    print(f"{'═'*62}")
    print(f"  TOTAL:  ✓ {total_ok} downloaded   ✗ {total_fail} failed")
    print(f"{'═'*62}")


if __name__ == "__main__":
    main()
