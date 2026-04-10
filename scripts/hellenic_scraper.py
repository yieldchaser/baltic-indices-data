"""
Hellenic Shipping News Multi-Category Scraper
============================================
Archives selected Hellenic Shipping News report categories as clean,
repo-local HTML snapshots with sidecar images and optional PDFs.

Output:
  reports/hellenic/{category}/{year}/{date}_{slug}.html
  reports/hellenic/{category}/{year}/{date}_{slug}_imgN.{ext}
  reports/hellenic/{category}/pdfs/{date}_{filename}.pdf
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from source_archive_utils_v2 import (
    REPORTS_ROOT,
    clean_node_text,
    configure_utf8_stdio,
    make_soup,
    remove_empty_tags,
    repair_text,
    sanitize_filename,
    slugify,
    standard_archive_html,
    strip_attrs,
    unwrap_redundant_containers,
)


BASE_URL = "https://www.hellenicshippingnews.com"
OUTPUT_ROOT = REPORTS_ROOT / "hellenic"

CATEGORIES = {
    "dry_charter": (
        "https://www.hellenicshippingnews.com/category/report-analysis/"
        "weekly-dry-time-charter-estimates/"
    ),
    "tanker_charter": (
        "https://www.hellenicshippingnews.com/category/report-analysis/"
        "weekly-tanker-time-charter-estimates/"
    ),
    "iron_ore": (
        "https://www.hellenicshippingnews.com/category/commodities/"
        "chinese-iron-ore-and-steelmaking-prices/"
    ),
    "vessel_valuations": (
        "https://www.hellenicshippingnews.com/category/report-analysis/"
        "weekly-vessel-valuations-report/"
    ),
    "demolition": (
        "https://www.hellenicshippingnews.com/category/report-analysis/"
        "weekly-demolition-reports/"
    ),
    "shipbuilding": (
        "https://www.hellenicshippingnews.com/category/report-analysis/"
        "weekly-shipbuilding-reports/"
    ),
}

MONTH_NAMES = (
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
)

PAGE_DELAY = 1.5
ARTICLE_DELAY = 1.5
ASSET_DELAY = 0.5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE_URL + "/",
}

dl_session = requests.Session()
dl_session.headers.update(HEADERS)
configure_utf8_stdio()


def get_driver(headed: bool = False):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    opts = Options()
    if not headed:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1400,900")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument("--log-level=3")
    opts.page_load_strategy = "eager"
    try:
        driver = webdriver.Chrome(options=opts)
    except Exception as exc:
        print(f"  x Chrome failed: {exc}")
        sys.exit(1)
    driver.set_page_load_timeout(60)
    return driver


def fetch_soup(driver, url: str, wait: float = 2.5) -> BeautifulSoup | None:
    for attempt in range(3):
        try:
            driver.get(url)
            time.sleep(wait)
            return make_soup(driver.page_source)
        except Exception as exc:
            first_line = str(exc).split("\n")[0][:120]
            print(f"    ! [{attempt + 1}/3] {first_line}")
            try:
                page_source = driver.page_source
                if page_source and len(page_source) > 500:
                    print("    partial page recovered")
                    return make_soup(page_source)
            except Exception:
                pass
            time.sleep(3)
    return None


def extract_link_year(url: str) -> int | None:
    match = re.search(r"(20\d{2})", url)
    return int(match.group(1)) if match else None


def get_article_links(page_soup: BeautifulSoup, category_url: str) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()
    domain = urlparse(category_url).netloc
    category_path = urlparse(category_url).path.rstrip("/")

    def add(href: str) -> None:
        if not href:
            return
        if not href.startswith("http"):
            href = urljoin(BASE_URL, href)
        href = href.split("#")[0].rstrip("/")
        parsed = urlparse(href)
        if parsed.netloc != domain or parsed.query:
            return
        path = parsed.path.strip("/")
        if any(token in path for token in ["category/", "tag/", "/page/", "author/", "feed", "wp-json", "cdn-cgi"]):
            return
        if parsed.path.rstrip("/") == category_path:
            return
        segments = [segment for segment in path.split("/") if segment]
        if len(segments) != 1:
            return
        if href not in seen:
            seen.add(href)
            links.append(href)

    for selector in [
        "h2.entry-title a",
        "h1.entry-title a",
        ".entry-title a",
        ".post-title a",
        "h2 > a[rel='bookmark']",
        "a[rel='bookmark']",
    ]:
        for anchor in page_soup.select(selector):
            add(anchor.get("href", ""))

    for anchor in page_soup.find_all("a", href=True):
        text = repair_text(anchor.get_text(strip=True)).lower()
        if text.startswith("read more") or text == "continue reading":
            add(anchor["href"])

    if not links:
        print("    ! No article links found on this page")
    return links


def get_next_page_url(page_soup: BeautifulSoup, current_url: str) -> str | None:
    next_link = page_soup.find("a", rel="next")
    if next_link and next_link.get("href"):
        href = next_link["href"]
        return urljoin(BASE_URL, href) if not href.startswith("http") else href

    page_text = page_soup.get_text()
    match = re.search(r"Page\s+(\d+)\s+of\s+(\d+)", page_text)
    if match:
        current_page = int(match.group(1))
        total_pages = int(match.group(2))
        if current_page < total_pages:
            base = re.sub(r"/page/\d+/?$", "", current_url.rstrip("/"))
            return f"{base}/page/{current_page + 1}/"

    current_page = 1
    match = re.search(r"/page/(\d+)/?$", current_url)
    if match:
        current_page = int(match.group(1))

    linked_pages = []
    for anchor in page_soup.find_all("a", href=True):
        match = re.search(r"/page/(\d+)/?$", anchor["href"])
        if match:
            linked_pages.append(int(match.group(1)))
    if linked_pages and current_page < max(linked_pages):
        base = re.sub(r"/page/\d+/?$", "", current_url.rstrip("/"))
        return f"{base}/page/{current_page + 1}/"

    return None


def collect_category_urls(driver, category_name: str, category_url: str, year_filter: int | None) -> list[str]:
    all_links: list[str] = []
    seen: set[str] = set()
    page_url = category_url
    page_num = 1

    while page_url:
        print(f"    Page {page_num}: ...{page_url[-60:]}")
        page_soup = fetch_soup(driver, page_url)
        if page_soup is None:
            print("    x Failed to fetch listing page")
            break

        links = get_article_links(page_soup, category_url)
        added = 0
        for link in links:
            if link not in seen:
                seen.add(link)
                all_links.append(link)
                added += 1
        print(f"      +{added} links  (total {len(all_links)})")

        if year_filter:
            page_years = [year for year in (extract_link_year(link) for link in links) if year]
            if page_years and max(page_years) < year_filter:
                print(f"    Reached pages older than {year_filter}; stopping pagination for {category_name}")
                break

        next_url = get_next_page_url(page_soup, page_url)
        if not next_url:
            print("    End of pages")
            break
        page_url = next_url
        page_num += 1
        time.sleep(PAGE_DELAY)

    if year_filter:
        target = str(year_filter)
        return [url for url in all_links if target in url or extract_link_year(url) is None]
    return all_links


def resolve_asset_url(page_url: str, raw_url: str) -> str:
    """Normalize malformed scraped asset URLs into a usable absolute URL."""
    candidate = repair_text(raw_url or "").strip()
    if not candidate:
        return ""

    candidate = re.sub(r"^(?:%20|\s)+", "", candidate)
    embedded = re.search(r"https?://[^\s\"'<>]+", candidate)
    if embedded:
        candidate = embedded.group(0)
    elif candidate.startswith("https:/") and not candidate.startswith("https://"):
        candidate = "https://" + candidate[len("https:/") :].lstrip("/")
    elif candidate.startswith("http:/") and not candidate.startswith("http://"):
        candidate = "http://" + candidate[len("http:/") :].lstrip("/")
    elif candidate.startswith("//"):
        candidate = "https:" + candidate
    elif candidate.startswith("www."):
        candidate = "https://" + candidate
    elif not candidate.startswith("http"):
        candidate = urljoin(page_url, candidate)

    return candidate


def download_file(url: str, dest: Path, *, minimum_size: int = 500) -> bool:
    if dest.exists() and dest.stat().st_size > minimum_size:
        return True
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        response = dl_session.get(url, timeout=30, stream=True)
        response.raise_for_status()
        dest.write_bytes(response.content)
        time.sleep(ASSET_DELAY)
        return dest.exists() and dest.stat().st_size > minimum_size
    except Exception as exc:
        print(f"    ! Asset download failed: {url[-70:]}  {exc}")
        if dest.exists():
            dest.unlink()
        return False


def parse_date(page_soup: BeautifulSoup, url: str) -> datetime | None:
    for selector in [
        "meta[property='article:published_time']",
        "meta[name='article:published_time']",
        "time[datetime]",
        ".post-meta",
        ".updated",
        "span.updated",
        ".entry-date",
        "time",
    ]:
        element = page_soup.select_one(selector)
        if not element:
            continue
        raw = repair_text(element.get("content") or element.get("datetime") or element.get_text(" ", strip=True))
        for fmt in [
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S%z",
            "%d/%m/%Y",
            "%B %d, %Y",
            "%b %d, %Y",
            "%d %B %Y",
            "%d %b %Y",
        ]:
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
        match = re.search(r"(\d{2})[/\-](\d{2})[/\-](\d{4})", raw)
        if match:
            try:
                return datetime(int(match.group(3)), int(match.group(2)), int(match.group(1)))
            except ValueError:
                pass

    slug = urlparse(url).path.strip("/").lower()
    month_pattern = "|".join(MONTH_NAMES)
    month_match = re.search(rf"({month_pattern})-(\d{{1,2}})-(20\d{{2}})", slug)
    if month_match:
        try:
            return datetime.strptime(
                f"{month_match.group(1)} {month_match.group(2)} {month_match.group(3)}",
                "%B %d %Y",
            )
        except ValueError:
            pass

    iso_match = re.search(r"(20\d{2})-(\d{2})-(\d{2})", slug)
    if iso_match:
        try:
            return datetime(int(iso_match.group(1)), int(iso_match.group(2)), int(iso_match.group(3)))
        except ValueError:
            pass
    return None


def find_title(page_soup: BeautifulSoup, url: str) -> str:
    selectors = [
        "h1.entry-title",
        "h1.post-title",
        "article h1",
        "title",
        "h1",
    ]
    for selector in selectors:
        element = page_soup.select_one(selector)
        if element is None:
            continue
        text = repair_text(element.get_text(" ", strip=True))
        if selector == "title":
            text = re.sub(r"\s+\|\s+Hellenic Shipping News Worldwide$", "", text).strip()
        if text:
            return text
    return repair_text(url.rstrip("/").split("/")[-1]).replace("-", " ").title()


def find_article_root(page_soup: BeautifulSoup) -> BeautifulSoup | None:
    for selector in [
        "article.post-listing .post-inner .entry",
        "article.post .post-inner .entry",
        "article.post-listing .entry",
        "article.post .entry",
    ]:
        element = page_soup.select_one(selector)
        if element:
            return element
    return None


def should_skip_image(img, src: str) -> bool:
    lowered = src.lower()
    if any(token in lowered for token in ["logo", "icon", "avatar", "googlelogo", "pinit", "sponsor", "banner", "pixel"]):
        return True
    try:
        width = int(str(img.get("width", "")).replace("px", ""))
    except ValueError:
        width = 0
    try:
        height = int(str(img.get("height", "")).replace("px", ""))
    except ValueError:
        height = 0
    return (width and width < 160) or (height and height < 120)


def normalize_body(
    body: BeautifulSoup,
    page_url: str,
    base_name: str,
    image_dir: Path,
    pdf_dir: Path,
) -> tuple[str, list[str]]:
    fragment = make_soup(f"<section>{str(body)}</section>").section
    if fragment is None:
        return "<p><em>Content not extracted - visit original URL.</em></p>", []

    for tag in fragment.select(
        "script, style, noscript, iframe, select, form, button, svg, .sharedaddy, .jp-relatedposts, .share-post"
    ):
        tag.decompose()

    for tag in fragment.select("h1, div[itemprop='author'], [aria-label='Language Translate Widget']"):
        tag.decompose()

    downloaded_pdfs: list[str] = []
    seen_pdfs: set[str] = set()
    for anchor in list(fragment.find_all("a", href=True)):
        href = anchor.get("href", "")
        absolute = resolve_asset_url(page_url, href)
        if not absolute:
            anchor.decompose()
            continue
        if ".pdf" in absolute.lower() or "pdf" in repair_text(anchor.get_text(" ", strip=True)).lower():
            pdf_name = sanitize_filename(f"{base_name}_{absolute.split('/')[-1]}")
            if not pdf_name.endswith(".pdf"):
                pdf_name += ".pdf"
            pdf_dest = pdf_dir / pdf_name
            if download_file(absolute, pdf_dest, minimum_size=1000):
                anchor["href"] = f"../../pdfs/{pdf_name}"
                anchor.string = repair_text(anchor.get_text(" ", strip=True)) or f"Download PDF: {pdf_name}"
                if pdf_name not in seen_pdfs:
                    downloaded_pdfs.append(pdf_name)
                    seen_pdfs.add(pdf_name)
            else:
                anchor.decompose()

    image_index = 0
    for img in list(fragment.find_all("img")):
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
        if not src or "data:image" in src:
            img.decompose()
            continue
        src = resolve_asset_url(page_url, src)
        if not src:
            img.decompose()
            continue
        if should_skip_image(img, src):
            img.decompose()
            continue
        image_index += 1
        ext = Path(urlparse(src).path).suffix or ".jpg"
        if len(ext) > 5:
            ext = ".jpg"
        image_name = sanitize_filename(f"{base_name}_img{image_index}{ext}")
        image_dest = image_dir / image_name
        if not download_file(src, image_dest, minimum_size=5000):
            img.decompose()
            continue
        img["src"] = image_name
        img["loading"] = "lazy"

    clean_node_text(fragment)
    strip_attrs(fragment)
    unwrap_redundant_containers(fragment)
    remove_empty_tags(fragment)
    return str(fragment), downloaded_pdfs


def extract_and_save(
    page_soup: BeautifulSoup,
    url: str,
    category_name: str,
    *,
    dry_run: bool,
    overwrite: bool,
) -> bool:
    title = find_title(page_soup, url)
    date_obj = parse_date(page_soup, url)
    if date_obj:
        date_str = date_obj.strftime("%d %B %Y")
        year = str(date_obj.year)
        date_stamp = date_obj.strftime("%Y-%m-%d")
    else:
        date_str = "unknown"
        year = "unknown"
        date_stamp = "0000-00-00"

    slug = slugify(url.rstrip("/").split("/")[-1])
    base_name = sanitize_filename(f"{date_stamp}_{slug}")
    dest_html = OUTPUT_ROOT / category_name / year / f"{base_name}.html"
    dest_pdf_dir = OUTPUT_ROOT / category_name / "pdfs"
    dest_img_dir = OUTPUT_ROOT / category_name / year

    if not overwrite and dest_html.exists() and dest_html.stat().st_size > 800:
        print(f"    skip: {dest_html.name}")
        return True

    if dry_run:
        print(f"    [DRY RUN] {date_str}  {title[:70]} -> {dest_html.name}")
        return True

    body = find_article_root(page_soup)
    body_html = "<p><em>Content not extracted - visit original URL.</em></p>"
    extra_parts: list[str] = []

    if body is not None:
        body_html, pdf_names = normalize_body(body, url, base_name, dest_img_dir, dest_pdf_dir)
        if pdf_names:
            pdf_links = "".join(
                f'<p><a href="../../pdfs/{name}">Download PDF: {name}</a></p>' for name in pdf_names
            )
            extra_parts.append(pdf_links)

    html_doc = standard_archive_html(
        title=title,
        archive_source="hellenic",
        source_url=url,
        published_date=date_str,
        category=category_name,
        body_html=body_html,
        source_label="Hellenic Shipping News",
        extra_html="\n".join(extra_parts),
        accent_color="#003366",
    )

    dest_html.parent.mkdir(parents=True, exist_ok=True)
    dest_html.write_text(html_doc, encoding="utf-8")
    print(f"    saved: {dest_html.name}  ({dest_html.stat().st_size // 1024} KB)")
    return True


def run_category(
    driver,
    category_name: str,
    category_url: str,
    *,
    dry_run: bool,
    year_filter: int | None,
    overwrite: bool,
) -> tuple[int, int]:
    print(f"\n  {'-' * 60}")
    print(f"  {category_name.upper()} -> {category_url}")
    print(f"  {'-' * 60}")

    urls = collect_category_urls(driver, category_name, category_url, year_filter)
    print(f"\n  {len(urls)} articles found for {category_name}")

    ok = fail = 0
    for index, url in enumerate(urls, 1):
        print(f"\n  [{index}/{len(urls)}] {url.rstrip('/').split('/')[-1][:70]}")
        time.sleep(ARTICLE_DELAY)
        page_soup = fetch_soup(driver, url, wait=2.5)
        if page_soup is None:
            print("    x Failed to fetch article")
            fail += 1
            continue
        if extract_and_save(page_soup, url, category_name, dry_run=dry_run, overwrite=overwrite):
            ok += 1
        else:
            fail += 1

    print(f"\n  {category_name}: ok={ok}  failed={fail}")
    return ok, fail


def run(categories: list[str], *, dry_run: bool, year_filter: int | None, headed: bool, overwrite: bool) -> None:
    print("\n" + "=" * 64)
    print("  Hellenic Shipping News Scraper")
    print(f"  Categories : {', '.join(categories)}")
    print(f"  Mode       : {'DRY RUN' if dry_run else 'DOWNLOAD'}")
    if year_filter:
        print(f"  Year       : {year_filter}")
    print("=" * 64)

    total_ok = total_fail = 0
    for category_name in categories:
        category_url = CATEGORIES[category_name]
        driver = get_driver(headed=headed)
        try:
            ok, fail = run_category(
                driver,
                category_name,
                category_url,
                dry_run=dry_run,
                year_filter=year_filter,
                overwrite=overwrite,
            )
            total_ok += ok
            total_fail += fail
        finally:
            driver.quit()

    print("\n" + "=" * 64)
    print(f"  TOTAL  ok={total_ok}  failed={total_fail}")
    print("=" * 64 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Hellenic Shipping News Scraper")
    parser.add_argument("--category", choices=list(CATEGORIES.keys()) + ["all"], default="all")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--headed", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    categories = list(CATEGORIES.keys()) if args.category == "all" else [args.category]
    run(
        categories,
        dry_run=args.dry_run,
        year_filter=args.year,
        headed=args.headed,
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    main()
