"""
Baltic Exchange Weekly Market Roundup Scraper  v3
======================================================================
Uses Selenium to discover links on the JS-rendered site and save each report
as a clean self-contained HTML snapshot.

Changes in v3:
  - Completely rewritten year-filter logic: handles custom JS dropdowns
    (not just native <select>) — the root cause of 25-link-only results
  - --debug flag: saves page HTML + filter DOM snapshot for inspection
  - Dry tab: keeps the proven infinite-scroll path
  - Other tabs: opens custom dropdown, cycles through each year option

Output:
  reports/baltic/{category}/{year}/{file}.html
  reports/baltic/{category}/{year}/assets/{asset}.{ext}
  reports/baltic/{category}/pdfs/{asset}.pdf

Install:
    pip install selenium requests beautifulsoup4 lxml

Usage:
    python baltic_scraper.py                        # all categories
    python baltic_scraper.py --category tanker      # one category
    python baltic_scraper.py --dry-run              # list URLs, no download
    python baltic_scraper.py --year 2024            # single year
    python baltic_scraper.py --headed               # show browser window
    python baltic_scraper.py --debug --category tanker --headed
                                                    # dump DOM for inspection
"""

import re
import sys
import time
import json
import argparse
import requests
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime
from bs4 import BeautifulSoup

from source_archive_utils_v2 import (
    REPORTS_ROOT,
    asset_kind,
    clean_node_text,
    deterministic_asset_filename,
    infer_asset_extension,
    is_mirrorable_asset,
    minimum_asset_size,
    normalize_asset_url,
    relative_asset_href,
    remove_empty_tags,
    strip_attrs,
    unwrap_redundant_containers,
)

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL    = "https://www.balticexchange.com"
LISTING_URL = "https://www.balticexchange.com/en/data-services/WeeklyRoundup.html"
OUTPUT_ROOT = REPORTS_ROOT / "baltic"
TAB_DIRECT_URLS = {
    "dry":       LISTING_URL,  # dry is default tab; hash fragment breaks it,
    "tanker":    LISTING_URL + "#tanker",
    "gas":       LISTING_URL + "#main_par_tabbedcontent2tabbedcontentitem_4",
    "container": LISTING_URL + "#main_par_tabbedcontent2tabbedcontentitem_5",
    "ningbo":    LISTING_URL + "#ningbo",
}
DEBUG_DIR   = OUTPUT_ROOT / "_debug"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.balticexchange.com/",
}

PAGE_DELAY     = 1.2
DOWNLOAD_DELAY = 1.5
ASSET_DELAY    = 0.35

CATEGORIES = {
    "dry": {
        "label":   "Dry",
        "anchor":  "dry",
        "pattern": re.compile(r"/WeeklyRoundup/dry/news/(\d{4})/(.+)\.html", re.IGNORECASE),
    },
    "tanker": {
        "label":   "Tankers",
        "anchor":  "tanker",
        "pattern": re.compile(r"/WeeklyRoundup/tanker/news/(\d{4})/(.+)\.html", re.IGNORECASE),
    },
    "gas": {
        "label":   "Gas",
        "anchor":  "gas",
        "pattern": re.compile(r"/WeeklyRoundup/Gas/News/(\d{4})/(.+)\.html", re.IGNORECASE),
    },
    "container": {
        "label":   "Container",
        "anchor":  "container",
        "pattern": re.compile(r"/WeeklyRoundup/Container/News/(\d{4})/(.+)\.html", re.IGNORECASE),
    },
    "ningbo": {
        "label":   "Ningbo",
        "anchor":  "ningbo",
        "pattern": re.compile(r"/WeeklyRoundup/ningbo/news/(\d{4})/(.+)\.html", re.IGNORECASE),
    },
}

# ── Requests session ──────────────────────────────────────────────────────────

session = requests.Session()
session.headers.update(HEADERS)

for stream_name in ("stdout", "stderr"):
    stream = getattr(sys, stream_name, None)
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8", errors="replace")


def fetch_soup_with_driver(driver, url: str) -> "BeautifulSoup | None":
    """Fetch a JS-rendered page using an existing Selenium driver."""
    for attempt in range(3):
        try:
            driver.get(url)
            time.sleep(4)  # wait for JS content to load
            soup = BeautifulSoup(driver.page_source, "lxml")
            if len(soup.get_text(strip=True)) > 300:
                return soup
            time.sleep(2)  # extra wait and retry
        except Exception as e:
            print(f"    ⚠  driver fetch [{attempt+1}/3]: {e}")
            time.sleep(3)
    return None


# ── Selenium helpers ───────────────────────────────────────────────────────────

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
    try:
        driver = webdriver.Chrome(options=opts)
    except Exception as e:
        print(f"  ✗ Could not start Chrome: {e}")
        sys.exit(1)
    driver.set_page_load_timeout(30)
    return driver


def dismiss_cookie_banner(driver):
    from selenium.webdriver.common.by import By
    for btn_text in ["Accept All", "Accept Cookies", "Accept", "OK"]:
        try:
            btn = driver.find_element(By.XPATH,
                f"//button[contains(translate(.,'abcdefghijklmnopqrstuvwxyz',"
                f"'ABCDEFGHIJKLMNOPQRSTUVWXYZ'),'{btn_text.upper()}')]")
            btn.click()
            time.sleep(1.5)
            return
        except Exception:
            pass


def click_tab(driver, label: str) -> bool:
    clicked = driver.execute_script("""
        var label = arguments[0];
        var tags = ['a','button','li','div','span'];
        for (var t = 0; t < tags.length; t++) {
            var els = document.getElementsByTagName(tags[t]);
            for (var i = 0; i < els.length; i++) {
                if (els[i].textContent.trim() === label) {
                    els[i].click(); return true;
                }
            }
        }
        return false;
    """, label)
    return bool(clicked)


def count_links(driver, pattern) -> int:
    soup = BeautifulSoup(driver.page_source, "lxml")
    return sum(1 for a in soup.find_all("a", href=True) if pattern.search(a["href"]))


def scrape_links(driver, pattern, seen: set, links: list) -> int:
    """Extract matching hrefs from current page source."""
    soup = BeautifulSoup(driver.page_source, "lxml")
    added = 0
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if pattern.search(href):
            full = urljoin(BASE_URL, href)
            if full not in seen:
                seen.add(full)
                links.append(full)
                added += 1
    return added


# ── Year filter: custom dropdown handling ─────────────────────────────────────

def dump_filter_dom(driver, cat: str):
    """Save page HTML and filter element snapshot for manual inspection."""
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    # Save full page
    html_path = DEBUG_DIR / f"page_{cat}.html"
    html_path.write_text(driver.page_source, encoding="utf-8")
    print(f"  💾 Page HTML → {html_path}")

    # Dump candidate filter elements
    candidates = driver.execute_script("""
        var results = [];
        var els = document.querySelectorAll('*');
        for (var i = 0; i < els.length; i++) {
            var el = els[i];
            var text = el.textContent.trim();
            if (text === 'All' || /^20\\d\\d$/.test(text)) {
                var rect = el.getBoundingClientRect();
                if (rect.width > 5 && rect.height > 5) {
                    results.push({
                        tag:   el.tagName,
                        cls:   el.className.substring(0, 100),
                        id:    el.id,
                        text:  text,
                        outerHTML: el.outerHTML.substring(0, 300),
                        x: Math.round(rect.x),
                        y: Math.round(rect.y),
                        w: Math.round(rect.width),
                        h: Math.round(rect.height)
                    });
                }
            }
        }
        return results;
    """)
    dom_path = DEBUG_DIR / f"filter_dom_{cat}.json"
    dom_path.write_text(json.dumps(candidates, indent=2), encoding="utf-8")
    print(f"  💾 Filter DOM ({len(candidates or [])} candidates) → {dom_path}")
    if candidates:
        print(f"  First 5 candidates:")
        for c in (candidates or [])[:5]:
            print(f"    [{c['tag']}] cls={c['cls'][:40]!r}  text={c['text']!r}  @ ({c['x']},{c['y']})")


def _visible_filter_container(driver) -> "str | None":
    """
    Return a JS expression that resolves to the VISIBLE .article-filter-options
    element (the one belonging to the currently active tab).
    Multiple tabs each have their own .article-filter-options; only the active
    tab's container has a non-zero bounding rect.
    """
    info = driver.execute_script("""
        var containers = document.querySelectorAll('.article-filter-options');
        for (var i = 0; i < containers.length; i++) {
            var r = containers[i].getBoundingClientRect();
            if (r.width > 5 && r.height > 5) {
                return {index: i, width: Math.round(r.width), height: Math.round(r.height)};
            }
        }
        return null;
    """)
    return info  # dict with 'index', or None


def get_baltic_years(driver) -> list:
    """Get year options from the active tab's dropdown. Works in headless mode."""
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(0.3)
    years = driver.execute_script("""
        function isVisible(el) {
            while (el && el !== document.body) {
                var s = window.getComputedStyle(el);
                if (s.display === 'none' || s.visibility === 'hidden') return false;
                el = el.parentElement;
            }
            return true;
        }
        var containers = document.querySelectorAll('.article-filter-options');
        var container = null;
        for (var c = 0; c < containers.length; c++) {
            if (isVisible(containers[c])) { container = containers[c]; break; }
        }
        if (!container && containers.length > 0) container = containers[0];
        if (!container) return [];
        var results = []; var seen = {};
        var all = container.querySelectorAll('*');
        for (var i = 0; i < all.length; i++) {
            var text = all[i].textContent.trim();
            if (/^20\\d\\d$/.test(text) && !seen[text]) {
                seen[text] = 1; results.push(text);
            }
        }
        return results;
    """)
    return years or []


def click_year_option(driver, year_str: str) -> bool:
    """Click a year in the active tab's dropdown. Headless-safe (uses computedStyle)."""
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(0.4)
    result = driver.execute_script("""
        var target = arguments[0];
        function isVisible(el) {
            while (el && el !== document.body) {
                var s = window.getComputedStyle(el);
                if (s.display === 'none' || s.visibility === 'hidden') return false;
                el = el.parentElement;
            }
            return true;
        }
        // Step 1: click the visible toggle to open dropdown
        var toggles = document.querySelectorAll('.select-selected');
        var toggle = null;
        for (var i = 0; i < toggles.length; i++) {
            if (isVisible(toggles[i])) { toggle = toggles[i]; break; }
        }
        if (!toggle) toggle = toggles[0];
        if (!toggle) return 'no-toggle';
        toggle.click();

        // Step 2: find visible container
        var containers = document.querySelectorAll('.article-filter-options');
        var container = null;
        for (var c = 0; c < containers.length; c++) {
            if (isVisible(containers[c])) { container = containers[c]; break; }
        }
        if (!container) container = containers[0];
        if (!container) return 'no-container';

        // Step 3: click the year
        var items = container.querySelectorAll('div, li, span, a');
        for (var j = 0; j < items.length; j++) {
            if (items[j].textContent.trim() === target) {
                items[j].click();
                return 'clicked:' + target;
            }
        }
        return 'not-found:' + target;
    """, year_str)
    return isinstance(result, str) and "clicked:" in result


def cycle_year_filter(driver, years: list, pattern, seen: set, links: list) -> int:
    """
    Cycle through each year in the Baltic Exchange dropdown, scraping links per year.
    Returns total new links added.
    """
    total = 0
    for yr in years:
        ok = click_year_option(driver, yr)
        if not ok:
            print(f"    {yr}: ✗ could not click — skipping")
            continue
        time.sleep(2.5)

        # Scroll to load all items for this year
        last_h = driver.execute_script("return document.body.scrollHeight")
        for _ in range(60):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.2)
            new_h = driver.execute_script("return document.body.scrollHeight")
            if new_h == last_h:
                time.sleep(1.5)
                new_h = driver.execute_script("return document.body.scrollHeight")
                if new_h == last_h:
                    break
            last_h = new_h

        added = scrape_links(driver, pattern, seen, links)
        print(f"    {yr}: +{added} links  (total {len(links)})")
        total += added
    return total


# ── Main link-discovery per tab ───────────────────────────────────────────────

def selenium_get_tab_links(driver, cat: str, debug: bool = False) -> list:
    label   = CATEGORIES[cat]["label"]
    pattern = CATEGORIES[cat]["pattern"]
    tab_url = TAB_DIRECT_URLS[cat]

    print(f"\n  🌐 Navigating directly to '{label}': {tab_url}")
    driver.get(tab_url)
    time.sleep(5)
    dismiss_cookie_banner(driver)
    time.sleep(1)

    n_initial = count_links(driver, pattern)
    print(f"  Links visible: {n_initial}")

    if debug:
        dump_filter_dom(driver, cat)

    seen  = set()
    links = []

    # ── DRY tab: infinite scroll approach (proven to work) ───────────────────
    if cat == "dry":
        print(f"  ⏬ Dry tab — infinite scroll mode...")
        last_h = driver.execute_script("return document.body.scrollHeight")
        scroll_count = 0
        for _ in range(120):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)
            new_h = driver.execute_script("return document.body.scrollHeight")
            if new_h == last_h:
                time.sleep(2)
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_h = driver.execute_script("return document.body.scrollHeight")
                if new_h == last_h:
                    break
            last_h = new_h
            scroll_count += 1
        print(f"  ✓ Scrolled {scroll_count} times")
        scrape_links(driver, pattern, seen, links)
        print(f"  📎 {len(links)} links collected for '{label}'")
        return links

    # ── Other tabs: year filter cycling ──────────────────────────────────────
    years = get_baltic_years(driver)
    if years:
        print(f"  📅 Years found in dropdown: {years}")
        total_added = cycle_year_filter(driver, years, pattern, seen, links)
        if total_added == 0:
            print(f"  ⚠  Year cycling yielded 0 links — falling back to current view scrape")
            scrape_links(driver, pattern, seen, links)
    else:
        print(f"  ⚠  No year dropdown — scrolling to load all content...")
        # Scroll in smaller steps to reliably trigger lazy-load in headless mode
        for _ in range(30):
            driver.execute_script("window.scrollBy(0, 600);")
            time.sleep(0.8)
        # Final full-page scroll passes
        last_h = driver.execute_script("return document.body.scrollHeight")
        for _ in range(20):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2.5)
            new_h = driver.execute_script("return document.body.scrollHeight")
            if new_h == last_h:
                break
            last_h = new_h
        time.sleep(3)  # final settle
        scrape_links(driver, pattern, seen, links)

    # If we still only have ~25 links, something is wrong — warn
    if len(links) <= 25 and not debug:
        print(f"  ⚠  Only {len(links)} links — year filter may have failed.")
        print(f"     Re-run with --debug --headed --category {cat} to inspect DOM")

    print(f"  📎 {len(links)} links collected for '{label}'")
    return links


def discover_all_links(categories: list, headed: bool,
                        debug: bool = False) -> dict:
    results = {}
    for cat in categories:
        driver = get_driver(headed=headed)
        try:
            links = selenium_get_tab_links(driver, cat, debug=debug)
            results[cat] = sorted(set(links), reverse=True)
        finally:
            driver.quit()
        time.sleep(PAGE_DELAY)
    return results


def get_download_driver(headed: bool = False):
    """Return a persistent driver for downloading report pages."""
    return get_driver(headed=headed)


# ── Content extraction & PDF ──────────────────────────────────────────────────

HTML_CSS = """
*, *::before, *::after { box-sizing: border-box; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Georgia, serif;
    font-size: 15px; line-height: 1.7; color: #1a1a1a;
    max-width: 860px; margin: 0 auto; padding: 32px 24px; background: #fff;
}
h1 { font-size: 1.6em; color: #00416a; margin: 0 0 6px; border-bottom: 2px solid #00416a; padding-bottom: 8px; }
h2 { font-size: 1.2em; color: #00416a; margin: 24px 0 6px; }
h3 { font-size: 1.0em; color: #00416a; margin: 16px 0 4px; }
p  { margin: 0 0 12px; }
a  { color: #00416a; }
table { width: 100%; border-collapse: collapse; margin: 16px 0; font-size: 0.9em; }
thead th { background: #00416a; color: #fff; padding: 8px 12px; text-align: left; font-weight: 600; }
tbody td { padding: 7px 12px; border-bottom: 1px solid #e0e6ed; vertical-align: top; }
tbody tr:nth-child(even) td { background: #f5f8fc; }
tbody tr:hover td { background: #eaf2fb; }
.meta { color: #555; font-size: 0.82em; margin: 6px 0 18px; padding: 6px 10px;
        background: #f0f4f8; border-left: 3px solid #00416a; border-radius: 2px; }
hr { border: none; border-top: 1px solid #dce3ea; margin: 20px 0; }
"""

MONTH_MAP = {m[:3].lower(): i for i, m in enumerate(
    ["", "January","February","March","April","May","June",
     "July","August","September","October","November","December"], 1
)}


def try_parse_date(text: str) -> "datetime | None":
    text = re.sub(r"\s+", " ", text.strip())
    for fmt in ["%d %B %Y", "%d %b %Y", "%B %d, %Y", "%b %d, %Y",
                "%d/%m/%Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    m = re.match(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", text)
    if m:
        day, mon, year = int(m.group(1)), m.group(2)[:3].lower(), int(m.group(3))
        if mon in MONTH_MAP:
            try:
                return datetime(year, MONTH_MAP[mon], day)
            except ValueError:
                pass
    return None


def extract_date_from_page(soup: BeautifulSoup) -> "datetime | None":
    # Restrict search to article body to avoid picking up nav/ticker dates
    body = None
    for sel in ["article", ".article-content", ".news-content", ".rte",
                ".content-body", "main", "#main"]:
        el = soup.select_one(sel)
        if el and len(el.get_text(strip=True)) > 100:
            body = el
            break
    search_scope = body or soup

    for selector in ["time", ".date", ".article-date", ".news-date",
                     "[class*='date']", "[class*='Date']", ".calendar"]:
        el = search_scope.select_one(selector)
        if el:
            d = try_parse_date(el.get_text(strip=True))
            if d and d.year >= 2014:
                return d
    for m in re.finditer(
        r"\b(\d{1,2})\s+(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|"
        r"May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|"
        r"Nov(?:ember)?|Dec(?:ember)?)\s+(\d{4})\b",
        search_scope.get_text()
    ):
        d = try_parse_date(m.group(0))
        if d and d.year >= 2014:
            return d
    return None


def mirror_asset(
    *,
    page_url: str,
    raw_url: str,
    base_name: str,
    html_path: Path,
    assets_dir: Path,
    link_text: str = "",
) -> tuple[str, str] | None:
    absolute = normalize_asset_url(page_url, raw_url)
    if not absolute or not is_mirrorable_asset(absolute, link_text):
        return None

    extension = infer_asset_extension(absolute, link_text)
    if not extension:
        return None
    kind_name = asset_kind(extension)
    min_size = minimum_asset_size(kind_name)

    try:
        response = session.get(absolute, timeout=30, stream=True)
        response.raise_for_status()
        payload = response.content
    except Exception as exc:
        print(f"    ! Asset download failed: {absolute[-80:]}  {exc}")
        return None

    if len(payload) <= min_size:
        return None

    assets_dir.mkdir(parents=True, exist_ok=True)
    filename = deterministic_asset_filename(base_name, absolute, payload, extension)
    destination = assets_dir / filename
    if not destination.exists():
        destination.write_bytes(payload)
    time.sleep(ASSET_DELAY)
    return relative_asset_href(html_path, destination), absolute


def extract_article_html(
    soup: BeautifulSoup,
    url: str,
    title: str,
    date: "datetime | None",
    html_dest: Path,
) -> str:
    article = None
    for sel in ["article", ".article-content", ".news-content",
                ".rte", ".content-body", "main", "#main",
                "[class*='article']", "[class*='content']"]:
        el = soup.select_one(sel)
        if el and len(el.get_text(strip=True)) > 150:
            article = el
            break
    if article is None:
        article = soup.find("body") or soup

    article_fragment = BeautifulSoup(str(article), "lxml")
    article_root = article_fragment.find(["article", "body", "section", "div"]) or article_fragment

    for tag in article_root.find_all(["nav", "header", "footer", "script", "style", "noscript", "aside"]):
        tag.decompose()

    base_name = html_dest.stem
    assets_dir = html_dest.parent / "assets"
    pdf_dir = html_dest.parent.parent / "pdfs"

    for iframe in list(article_root.find_all("iframe")):
        src = iframe.get("src") or ""
        mirrored = mirror_asset(
            page_url=url,
            raw_url=src,
            base_name=f"{base_name}_embed",
            html_path=html_dest,
            assets_dir=assets_dir,
        ) if src else None
        note = article_root.new_tag("div")
        note["class"] = "archive-note"
        if mirrored:
            href, original = mirrored
            link = article_root.new_tag("a", href=href)
            link.string = f"Embedded asset: {Path(urlparse(original).path).name or 'download'}"
            note.append(link)
        else:
            note.string = f"Embedded chart: {src}" if src else "Embedded content removed during archiving"
        iframe.replace_with(note)

    for anchor in list(article_root.find_all("a", href=True)):
        href = anchor.get("href") or ""
        link_text = anchor.get_text(" ", strip=True)
        absolute = normalize_asset_url(url, href)
        if not absolute:
            anchor.decompose()
            continue
        extension = infer_asset_extension(absolute, link_text)
        mirrored = mirror_asset(
            page_url=url,
            raw_url=href,
            base_name=base_name,
            html_path=html_dest,
            assets_dir=pdf_dir if extension == ".pdf" else assets_dir,
            link_text=link_text,
        )
        if mirrored:
            local_href, original = mirrored
            anchor["href"] = local_href
            if not link_text:
                anchor.string = f"Linked asset: {Path(urlparse(original).path).name or 'download'}"
        else:
            anchor["href"] = absolute

    for img in list(article_root.find_all("img")):
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
        if not src:
            img.decompose()
            continue
        mirrored = mirror_asset(
            page_url=url,
            raw_url=src,
            base_name=f"{base_name}_img",
            html_path=html_dest,
            assets_dir=assets_dir,
        )
        if mirrored:
            local_src, _ = mirrored
            img["src"] = local_src
            img.attrs.pop("data-src", None)
            img.attrs.pop("data-lazy-src", None)
        else:
            img["src"] = normalize_asset_url(url, src)
        img["loading"] = "lazy"

    clean_node_text(article_root)
    strip_attrs(article_root)
    unwrap_redundant_containers(article_root)
    remove_empty_tags(article_root)

    date_str = date.strftime("%d %B %Y") if date else ""
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><title>{title}</title>
<style>{HTML_CSS}</style></head><body>
<h1>{title}</h1>
<p class="meta">Date: {date_str} &nbsp;|&nbsp; <a href="{url}">{url}</a></p><hr>
{article_root}
</body></html>"""


def save_as_html_snapshot(html: str, dest: Path) -> bool:
    """Save as clean self-contained HTML — readable in browser, parseable by scripts."""
    out = dest.with_suffix(".html")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    print(f"    ↓ {out.name}  ({out.stat().st_size // 1024} KB)")
    return True


def extract_year_from_url(url: str) -> "int | None":
    m = re.search(r"/news/(\d{4})/", url, re.IGNORECASE)
    return int(m.group(1)) if m else None


def make_filename(cat: str, url: str, date: "datetime | None", title: str) -> str:
    slug = urlparse(url).path.rstrip("/").split("/")[-1].replace(".html", "")
    wk = re.search(r"[Ww]eek[\s\-_]*(\d+)", title)
    week = f"W{int(wk.group(1)):02d}" if wk else ""
    year = extract_year_from_url(url) or "unk"
    if date and date.year == year:   # only trust date if year matches URL
        ds = date.strftime("%Y-%m-%d")
        return f"{ds}_{week+'_' if week else ''}{slug[:40]}_{cat}.html"
    # No valid date — use year+slug (always unique)
    return f"{year}_{week+'_' if week else ''}{slug[:45]}_{cat}.html"


def sanitize(s: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', "_", s)


def process_report(url: str, cat: str, dry_run: bool, overwrite: bool, driver=None) -> bool:
    year = extract_year_from_url(url) or "unknown"
    time.sleep(PAGE_DELAY)
    if driver is None:
        return False
    soup = fetch_soup_with_driver(driver, url)
    if soup is None:
        return False

    h1 = soup.find(["h1", "h2"])
    title = h1.get_text(strip=True) if h1 else url.split("/")[-1].replace(".html","")
    title = title.replace("–", "-").strip()
    date  = extract_date_from_page(soup)

    filename = sanitize(make_filename(cat, url, date, title))
    dest     = OUTPUT_ROOT / cat / str(year) / filename

    # Check if already saved under any extension and with reasonable size.
    # Overwrite mode bypasses this guard to force historical remirroring.
    if not overwrite:
        existing = False
        for ext in [".pdf", ".html"]:
            p = dest.with_suffix(ext)
            if p.exists() and p.stat().st_size > 1500:
                existing = True
                break
        if existing:
            print(f"    ✓ skip: {p.name}")
            return True

    if dry_run:
        ds = date.strftime("%Y-%m-%d") if date else f"{year}-??-??"
        wk = re.search(r"[Ww]eek[\s\-_]*(\d+)", title)
        week = f"W{int(wk.group(1)):02d}" if wk else ""
        print(f"    [DRY RUN] {ds}  {week+'  ' if week else ''}{title[:50]}  → {filename}")
        return True

    html = extract_article_html(soup, url, title, date, dest)
    return save_as_html_snapshot(html, dest)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(categories: list, dry_run: bool, year_filter: "int | None",
        headed: bool, debug: bool, overwrite: bool):
    print(f"\n{'═'*64}")
    print(f"  Baltic Exchange Weekly Roundup Scraper  v3")
    print(f"  Categories : {', '.join(categories)}")
    print(f"  Mode       : {'DRY RUN' if dry_run else 'DOWNLOAD'}")
    print(f"  Browser    : {'headed' if headed else 'headless'}")
    print(f"  Overwrite  : {'ON' if overwrite else 'OFF'}")
    if year_filter:
        print(f"  Year filter: {year_filter}")
    if debug:
        print(f"  Debug      : ON  (DOM dumps → {DEBUG_DIR})")
    print(f"{'═'*64}")

    all_links = discover_all_links(categories, headed, debug=debug)

    filtered = {}
    for cat in categories:
        links = all_links.get(cat, [])
        if year_filter:
            links = [l for l in links if extract_year_from_url(l) == year_filter]
        seen = set()
        filtered[cat] = [l for l in sorted(links, reverse=True)
                         if not seen.__contains__(l) and not seen.add(l)]

    total = sum(len(v) for v in filtered.values())
    print(f"\n  ✅ {total} unique URLs to process\n")

    ok = fail = 0
    for cat in categories:
        links = filtered.get(cat, [])
        if not links:
            print(f"\n  ⚠  {CATEGORIES[cat]['label']}: 0 links found")
            continue
        print(f"\n  {'─'*62}")
        print(f"  📂 {CATEGORIES[cat]['label']}  ({len(links)} reports)")
        print(f"  {'─'*62}")
        dl_driver = get_download_driver(headed=headed)
        try:
            for url in links:
                yr = extract_year_from_url(url) or "?"
                print(f"\n  [{yr}] {url.split('/')[-1]}")
                if process_report(url, cat, dry_run, overwrite, driver=dl_driver):
                    ok += 1
                else:
                    fail += 1
                if not dry_run:
                    time.sleep(DOWNLOAD_DELAY)
        finally:
            if dl_driver:
                dl_driver.quit()

    print(f"\n{'═'*64}")
    print(f"  TOTAL  ✓ {ok} saved   ✗ {fail} failed")
    print(f"{'═'*64}\n")


def main():
    p = argparse.ArgumentParser(description="Baltic Exchange Weekly Roundup scraper v3")
    p.add_argument("--category",
                   choices=["dry","tanker","gas","container","ningbo","all"],
                   default="all")
    p.add_argument("--dry-run",  action="store_true")
    p.add_argument("--year",     type=int, default=None)
    p.add_argument("--headed",   action="store_true")
    p.add_argument("--debug",    action="store_true",
                   help="Dump filter DOM + full page HTML for inspection")
    p.add_argument("--overwrite", action="store_true",
                   help="Rebuild archived snapshots even when destination files already exist")
    args = p.parse_args()

    cats = list(CATEGORIES.keys()) if args.category == "all" else [args.category]
    run(cats, args.dry_run, args.year, args.headed, args.debug, args.overwrite)


if __name__ == "__main__":
    main()
