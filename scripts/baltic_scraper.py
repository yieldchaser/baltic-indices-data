"""
Baltic Exchange Weekly Market Roundup Scraper  v4
======================================================================
Uses Selenium to discover links on the JS-rendered site and save each report
as a clean self-contained HTML snapshot.

Changes in v4 (Decision 1.2 — Baltic capture fix):
  - Wall mechanism (measured 2026-09-06): balticexchange.com serves a Radware
    "Challenge Validation" crypto-challenge (sec_cpt cookie) to browser-UAs
    over static requests, while plain static requests (default UA) are served
    the full server-rendered article WITH the legacy `div.article-content`
    wrapper. Selenium/Chrome passes the challenge but the hydrated DOM injects
    a cookie-consent banner as the FIRST h1 ("This site uses cookies") and
    re-renders the article WITHOUT the `article-content` class. The old
    download path (Selenium-only, first-h1 title, no verification) therefore
    archived banner-poisoned titles + classless DOM, and `adapt_baltic` in
    process_knowledge.py (keyed on `div.article-content`) fell back to the
    header div → date+title stub chunks for all 2026 docs.
  - Static-first article fetch: plain static GET (default UA, NOT a browser
    UA — browser UAs trigger the challenge) with the legacy wrapper intact;
    Selenium driver kept as fallback for challenged/failed static fetches.
  - Consent hardening: banner nodes stripped, title picker skips consent
    headings, archived snapshots re-wrapped in `div.article-content` so the
    archive→compile contract holds regardless of live-DOM redesigns.
  - Quarantine gate: every snapshot verified (challenge markers absent,
    consent text absent, article length floor, category market markers
    present) BEFORE any write — stubs are logged, never archived, even under
    --overwrite. Mirrored asset payloads get the same stub rejection (the
    April 2026 re-scrape archived 1.9KB challenge pages as assets).
  - --refetch-year mode: deterministic re-fetch of URLs embedded in existing
    snapshots (no Selenium discovery needed).

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
    python baltic_scraper.py --refetch-year 2026 --overwrite
                                                    # re-fetch URLs embedded
                                                    # in 2026 snapshots
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

# ── Requests sessions ─────────────────────────────────────────────────────────

session = requests.Session()
session.headers.update(HEADERS)

# Static-first article session (Decision 1.2). Deliberately NOT a browser UA:
# balticexchange.com answers browser-UAs over static requests with a Radware
# "Challenge Validation" crypto-challenge (sec_cpt), while plain requests
# (default python-requests UA) are served the full server-rendered article
# WITH the legacy `div.article-content` wrapper intact (measured 2026-09-06
# across dry/tanker/gas/container). robots.txt allows crawling (User-agent: *
# Crawl-delay: 1) — callers keep >=1.2s gaps between requests.
static_session = requests.Session()
static_session.headers.update({
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.balticexchange.com/",
})

# ── Capture-verification markers (Decision 1.2 quarantine gate) ──────────────

# Cookie-consent banner signatures. Only ever matched against SHORT nodes
# (<300 chars) or the extracted article text post-strip, so genuine article
# prose can never trip them.
CONSENT_MARKERS = (
    "this site uses cookies",
    "we use cookies",
    "accept all cookies",
    "accept cookies",
    "cookie consent",
    "cookie policy",
    "manage cookies",
    "reject all",
)

# Bot-manager / CDN challenge signatures (Radware "Challenge Validation").
CHALLENGE_MARKERS = (
    "challenge validation",
    "sec-container",
    "sec_cpt",
    "just a moment",
    "checking your browser",
    "verify you are a human",
    "attention required! | cloudflare",
    "cloudflare ray id",
)

# Per-category market-data markers proving a snapshot holds a real weekly
# report (not a stub). Calibrated 2026-09-06: every genuine 2024-2026 Baltic
# article carries several of these; stub captures (date+title, ~35 chars)
# carry none.
MARKET_MARKERS = {
    "dry":       ("capesize", "panamax", "supramax", "handysize", "bci", "bdi", "5tc"),
    "tanker":    ("vlcc", "suezmax", "aframax", "dirty", " tce", "/day", " ws", "ws "),
    "gas":       ("lng", "lpg", "blng"),
    "container": ("fbx", "teu", "feu", "blanking", "carrier", "freight"),
    "ningbo":    ("ningbo", "freight index", "teu", "feu"),
}

# Article-text length floor for the quarantine gate. Measured 2026-09-06 over
# 764 Baltic 2024-2026 snapshots: shortest genuine article 678 chars
# (container), category p5 876+; stub captures are ~35 chars (date + title).
ARTICLE_MIN_CHARS = 500

for stream_name in ("stdout", "stderr"):
    stream = getattr(sys, stream_name, None)
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8", errors="replace")


def fetch_soup_static(url: str, retries: int = 1) -> "BeautifulSoup | None":
    """Fetch an article page over plain static GET (Decision 1.2: primary path).

    Uses ``static_session`` (default UA — browser UAs trigger the Radware
    crypto-challenge). Returns None on HTTP errors, challenge pages, or
    transport failures so the caller can fall back to Selenium / quarantine.
    """
    for attempt in range(retries + 1):
        try:
            response = static_session.get(url, timeout=30)
            if response.status_code != 200:
                print(f"    ⚠  static fetch [{attempt+1}/{retries+1}]: HTTP {response.status_code}")
                time.sleep(2)
                continue
            lowered = response.text[:6000].lower()
            if any(marker in lowered for marker in CHALLENGE_MARKERS):
                print(f"    ⚠  static fetch [{attempt+1}/{retries+1}]: challenge page served")
                time.sleep(2)
                continue
            soup = BeautifulSoup(response.text, "lxml")
            if len(soup.get_text(strip=True)) > 300:
                return soup
            print(f"    ⚠  static fetch [{attempt+1}/{retries+1}]: near-empty page")
            time.sleep(2)
        except Exception as e:
            print(f"    ⚠  static fetch [{attempt+1}/{retries+1}]: {e}")
            time.sleep(2)
    return None


def fetch_soup_with_driver(driver, url: str) -> "BeautifulSoup | None":
    """Fetch a JS-rendered page using an existing Selenium driver (fallback)."""
    for attempt in range(3):
        try:
            driver.get(url)
            time.sleep(4)  # wait for JS content to load
            dismiss_cookie_banner(driver)  # drop the consent overlay so the
            time.sleep(1)                  # article heading is the first h1
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


def pick_article_title(soup: BeautifulSoup, url: str) -> str:
    """Article heading, never the cookie-consent banner (Decision 1.2).

    The hydrated Baltic DOM injects "This site uses cookies" as an early
    heading and the server page carries nav headings ("Who We Are", …), so
    document-order first-h1-wins poisoned every snapshot title. Prefer the
    heading inside the article container, then fall back to document order
    (still skipping consent headings), then the URL slug.
    """
    roots = [locate_article_root(soup), soup]
    for root in roots:
        if root is None:
            continue
        for tag in root.find_all(["h1", "h2"]):
            text = tag.get_text(" ", strip=True).replace("–", "-").strip()
            if text and not any(marker in text.lower() for marker in CONSENT_MARKERS):
                return text
    return url.rstrip("/").split("/")[-1].replace(".html", "")


def strip_consent_nodes(soup: BeautifulSoup) -> int:
    """Decompose cookie-consent banner nodes from a fetched soup (in place).

    Only short nodes (<300 chars) carrying consent markers are removed, plus
    cookie accept/reject buttons — article prose is never at risk.
    Returns the number of nodes removed.
    """
    removed = 0
    for node in list(soup.find_all(["div", "section", "aside", "p", "h1", "h2", "button", "span"])):
        text = node.get_text(" ", strip=True)
        if not text or len(text) >= 300:
            continue
        lowered = text.lower()
        if any(marker in lowered for marker in CONSENT_MARKERS):
            node.decompose()
            removed += 1
        elif node.name == "button" and ("accept" in lowered or "reject" in lowered):
            node.decompose()
            removed += 1
    return removed


def locate_article_root(soup: BeautifulSoup):
    """Find the article container using the scraper's selector cascade."""
    for sel in ["article", ".article-content", ".news-content",
                ".rte", ".content-body", "main", "#main",
                "[class*='article']", "[class*='content']"]:
        el = soup.select_one(sel)
        if el and len(el.get_text(strip=True)) > 150:
            return el
    return soup.find("body") or soup


def probe_article_text(soup: BeautifulSoup) -> str:
    """Extractable article text after consent stripping (verification probe)."""
    root = locate_article_root(soup)
    return root.get_text(" ", strip=True) if root else ""


def verify_snapshot(cat: str, soup: BeautifulSoup, article_text: str) -> tuple:
    """Quarantine gate (Decision 1.2): True/False + reason.

    A snapshot is archivable only when it is free of challenge/consent
    signatures, clears the article-length floor, and carries category
    market-data markers. Anything else is a stub: logged, never archived.
    """
    lowered_page = soup.get_text(" ", strip=True).lower()
    if any(marker in lowered_page for marker in CHALLENGE_MARKERS):
        return False, "challenge-page"
    lowered_article = (article_text or "").lower()
    if any(marker in lowered_article for marker in CONSENT_MARKERS):
        return False, "consent-poisoned"
    if len(article_text or "") < ARTICLE_MIN_CHARS:
        return False, f"short:{len(article_text or '')}"
    markers = MARKET_MARKERS.get(cat, ())
    if markers and not any(marker in lowered_article for marker in markers):
        return False, "no-market-markers"
    has_time = soup.find("time") is not None
    has_week = bool(re.search(r"week[\s\-_]*\d{1,2}", lowered_article))
    if not (has_time or has_week):
        return False, "no-report-identity"
    return True, "ok"


def extract_source_url(html_path: Path) -> str | None:
    """Canonical live URL embedded in an archived snapshot's meta paragraph."""
    try:
        soup = BeautifulSoup(
            html_path.read_text(encoding="utf-8", errors="ignore"), "lxml"
        )
    except OSError:
        return None
    meta = soup.select_one("p.meta a[href]")
    if meta and meta.get("href") and "WeeklyRoundup" in meta["href"]:
        return meta["href"]
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if "WeeklyRoundup" in href and "/news/" in href.lower():
            return href if href.startswith("http") else urljoin(BASE_URL, href)
    return None



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
    ["January","February","March","April","May","June",
     "July","August","September","October","November","December"], 1
)}


def try_parse_date(text: str) -> "datetime | None":
    text = re.sub(r"\s+", " ", text.strip())
    text = re.sub(r"\bSept\b", "Sep", text, flags=re.IGNORECASE)
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

    # Quarantine (Decision 1.2): never mirror bot-challenge stubs as assets.
    # The April 2026 re-scrape archived ~1.9KB "Challenge Validation" pages
    # (browser-UA asset fetches get challenged); the payload hash in the
    # filename then sprawls one new stub per run. Real assets pass through.
    if extension in (".html", ".htm"):
        head = payload[:32768].decode("utf-8", errors="ignore").lower()
        if any(marker in head for marker in CHALLENGE_MARKERS):
            print(f"    ⛔ QUARANTINE asset stub (not mirrored): {absolute[-80:]}")
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
    strip_consent_nodes(soup)  # Decision 1.2: drop the consent overlay first
    article = locate_article_root(soup)

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

    # Decision 1.2: preserve the archive→compile contract. adapt_baltic keys
    # on div.article-content, which the hydrated live DOM no longer carries —
    # re-wrap when missing. Also unwrap a nested <body> (a body-fallback root
    # would otherwise serialize its own <body> tag inside the outer one).
    if article_root.name == "body":
        inner_html = article_root.decode_contents()
    else:
        inner_html = str(article_root)
    if "article-content" not in inner_html:
        inner_html = f'<div class="article-content">{inner_html}</div>'

    date_str = date.strftime("%d %B %Y") if date else ""
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><title>{title}</title>
<style>{HTML_CSS}</style></head><body>
<h1>{title}</h1>
<p class="meta">Date: {date_str} &nbsp;|&nbsp; <a href="{url}">{url}</a></p><hr>
{inner_html}
</body></html>"""


def save_as_html_snapshot(html: str, dest: Path) -> bool:
    """Save as clean self-contained HTML — readable in browser, parseable by scripts."""
    out = dest.with_suffix(".html")
    out.parent.mkdir(parents=True, exist_ok=True)
    # Forced LF: .gitattributes normalizes reports/**/*.html to eol=lf, and
    # source_hash reads working-copy bytes — CRLF from a Windows checkout
    # would create false cross-platform hash mismatches (Decision 1.2).
    with out.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(html)
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


def process_report(url: str, cat: str, dry_run: bool, overwrite: bool, driver=None,
                   dest_override: "Path | None" = None) -> bool:
    year = extract_year_from_url(url) or "unknown"
    time.sleep(PAGE_DELAY)
    # Decision 1.2: static-first (challenge-proof, banner-free server markup),
    # Selenium fallback for challenged/failed static fetches.
    soup = fetch_soup_static(url)
    if soup is None:
        if driver is None:
            print(f"    ✗ static fetch failed, no driver: {url.split('/')[-1]}")
            return False
        soup = fetch_soup_with_driver(driver, url)
    if soup is None:
        return False

    # Harden + verify BEFORE any write: stubs are quarantined, never archived
    # (even under --overwrite — good snapshots are never clobbered by stubs).
    strip_consent_nodes(soup)
    title = pick_article_title(soup, url)
    date = extract_date_from_page(soup)
    article_text = probe_article_text(soup)
    ok, reason = verify_snapshot(cat, soup, article_text)
    if not ok:
        print(f"    ⛔ QUARANTINE [{reason}] (not archived): {url.split('/')[-1]}")
        return False

    filename = sanitize(make_filename(cat, url, date, title))
    # Refetch mode rewrites the enumerated snapshot in place: the 2026 set
    # contains near-duplicate filename pairs for the same live URL (dated +
    # year-only forms), and recomputing dest would orphan one twin and shift
    # doc_ids. In-place keeps every source_path/doc_id stable.
    dest = dest_override if dest_override is not None else OUTPUT_ROOT / cat / str(year) / filename

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
        target = dest.name if dest_override is not None else filename
        print(f"    [DRY RUN] {ds}  {week+'  ' if week else ''}{title[:50]}  → {target}")
        return True

    html = extract_article_html(soup, url, title, date, dest)
    return save_as_html_snapshot(html, dest)


# ── Main ──────────────────────────────────────────────────────────────────────

def refetch_year(year: int, categories: list, dry_run: bool,
                 headed: bool, overwrite: bool):
    """Deterministic re-fetch of URLs embedded in existing snapshots (v4).

    No Selenium discovery: enumerates reports/baltic/<cat>/<year>/*.html,
    extracts each snapshot's canonical live URL, and re-archives it through
    process_report (static-first + Selenium fallback + quarantine gate).
    Static failures are retried once with a shared Selenium driver so the
    progressively-stronger ladder is honored without starting a browser
    when static succeeds everywhere.
    """
    print(f"\n{'═'*64}")
    print(f"  Baltic Exchange Weekly Roundup Scraper  v4  (refetch)")
    print(f"  Categories : {', '.join(categories)}")
    print(f"  Year       : {year}")
    print(f"  Mode       : {'DRY RUN' if dry_run else 'DOWNLOAD'}")
    print(f"  Overwrite  : {'ON' if overwrite else 'OFF'}")
    print(f"{'═'*64}")

    ok = fail = 0
    for cat in categories:
        year_dir = OUTPUT_ROOT / cat / str(year)
        files = sorted(year_dir.glob("*.html")) if year_dir.exists() else []
        print(f"\n  {'─'*62}")
        print(f"  📂 {CATEGORIES[cat]['label']}  ({len(files)} snapshots)")
        print(f"  {'─'*62}")
        retry_queue = []
        for path in files:
            url = extract_source_url(path)
            if not url:
                print(f"\n  [?] {path.name}: no embedded source URL — skipped")
                fail += 1
                continue
            print(f"\n  [{year}] {url.split('/')[-1]}  (→ {path.name})")
            # Static-only pass first (driver=None); failures are collected
            # for one shared-driver Selenium retry below.
            if process_report(url, cat, dry_run, overwrite, driver=None,
                              dest_override=path):
                ok += 1
            else:
                retry_queue.append((path.name, url, path))
            if not dry_run:
                time.sleep(DOWNLOAD_DELAY)
        if retry_queue and not dry_run:
            print(f"  🔁 Retrying {len(retry_queue)} static failures with Selenium…")
            dl_driver = get_download_driver(headed=headed)
            try:
                for name, url, path in retry_queue:
                    print(f"\n  [{year}] (retry) {url.split('/')[-1]}  (→ {name})")
                    if process_report(url, cat, dry_run, overwrite,
                                      driver=dl_driver, dest_override=path):
                        ok += 1
                    else:
                        fail += 1
                    time.sleep(DOWNLOAD_DELAY)
            finally:
                if dl_driver:
                    dl_driver.quit()
        else:
            fail += len(retry_queue)

    print(f"\n{'═'*64}")
    print(f"  TOTAL  ✓ {ok} saved   ✗ {fail} failed/quarantined")
    print(f"{'═'*64}\n")


def run(categories: list, dry_run: bool, year_filter: "int | None",
        headed: bool, debug: bool, overwrite: bool):
    print(f"\n{'═'*64}")
    print(f"  Baltic Exchange Weekly Roundup Scraper  v4")
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
    p = argparse.ArgumentParser(description="Baltic Exchange Weekly Roundup scraper v4")
    p.add_argument("--category",
                   choices=["dry","tanker","gas","container","ningbo","all"],
                   default="all")
    p.add_argument("--dry-run",  action="store_true")
    p.add_argument("--year",     type=int, default=None)
    p.add_argument("--refetch-year", type=int, default=None,
                   help="Re-fetch URLs embedded in existing reports/baltic/*/<year> "
                        "snapshots (no Selenium discovery). Combine with --overwrite "
                        "to rewrite stubs.")
    p.add_argument("--headed",   action="store_true")
    p.add_argument("--debug",    action="store_true",
                   help="Dump filter DOM + full page HTML for inspection")
    p.add_argument("--overwrite", action="store_true",
                   help="Rebuild archived snapshots even when destination files already exist")
    args = p.parse_args()

    cats = list(CATEGORIES.keys()) if args.category == "all" else [args.category]
    if args.refetch_year:
        refetch_year(args.refetch_year, cats, args.dry_run,
                     args.headed, args.overwrite)
    else:
        run(cats, args.dry_run, args.year, args.headed, args.debug, args.overwrite)


if __name__ == "__main__":
    main()
