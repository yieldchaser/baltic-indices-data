"""
fetch_drewry_opinions.py
Scrapes Drewry's complete archive across all business units and sectors:
- Maritime Research Opinions
- Logistics Executive Briefings
- News & Events Releases
- Supply Chain Advisors & Index Assessments (WCI, IACI, trackers)
- Financial Research & Equity Indices

Features:
- Sweeps the full 132-page archive (all 658 feed items)
- Extracts exact title, date, and URL directly from feed cards
- Authenticated subscriber session via session cookie (from .env or --cookie)
- Direct HTTP requests with smart rate-limit cooldown (pauses 60s on 429)
- Incremental writing to disk (<slug>.md) and manifest (_manifest.csv)
- Skips already downloaded articles so reruns/resumptions are instant
"""

import argparse
import csv
import json
import os
import threading
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


def load_dotenv():
    """Load key-value pairs from .env into os.environ if present."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    env_path = os.path.join(base_dir, ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_dotenv()

BASE = "https://www.drewry.co.uk"
BROWSE_URL = f"{BASE}/maritime-research-opinion-browser"

REQUEST_DELAY_SECONDS = 4.0

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36 Edg/152.0.0.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,en-IN;q=0.8",
}


def fetch(url, label=""):
    """GET with a clear pass/fail report and smart rate-limit cooldown."""
    return _request_with_backoff("GET", url, label)


def _request_with_backoff(method, url, label, data=None, max_retries=2):
    for attempt in range(max_retries + 1):
        try:
            if method == "GET":
                resp = requests.get(url, headers=HEADERS, timeout=25)
            else:
                post_headers = {**HEADERS, "Content-Type": "application/x-www-form-urlencoded"}
                resp = requests.post(url, headers=post_headers, data=data, timeout=25)
        except requests.exceptions.RequestException as e:
            print(f"  [{label}] FAILED: {type(e).__name__}: {e}")
            return None

        if resp.status_code == 429:
            if attempt == max_retries:
                print(f"  [{label}] FAILED: HTTP 429 after {max_retries} cooldown attempts, skipping item")
                return None
            wait = 45 * (attempt + 1)
            print(f"  [{label}] HTTP 429 (quota ceiling) -- resting for {wait}s before retry {attempt + 1}/{max_retries}...")
            time.sleep(wait)
            continue

        if resp.status_code != 200:
            print(f"  [{label}] FAILED: HTTP {resp.status_code} for {url}")
            return None

        return resp.text
    return None


def discover_all_items(max_pages=132):
    """Sweep all browse pages and extract all article cards."""
    all_links = []
    print(f"Starting complete archive discovery across {max_pages} pages...")
    for page in range(1, max_pages + 1):
        data = {"SelTopics": "", "gotoPage": str(page), "ItemListSorting": "0", "txtKeyword": ""}
        html = _request_with_backoff("POST", BROWSE_URL, f"browse page {page}", data=data)
        if html is None:
            continue
        soup = BeautifulSoup(html, "html.parser")
        page_items = []
        for a in soup.find_all("a", href=True):
            if a.get_text(strip=True).lower() == "learn more":
                full_url = urljoin(BASE, a["href"])
                card = a.find_parent("div", class_=lambda c: c and "item" in c.lower()) or a.find_parent("li") or a.find_parent("div")
                h_title = card.find(["h2", "h3"]) if card else None
                title = h_title.get_text(strip=True) if h_title else ""
                h_date = card.find("h4") if card else None
                date = h_date.get_text(strip=True) if h_date else ""
                page_items.append({"url": full_url, "title": title, "date": date})
        print(f"  [page {page:3d}/{max_pages}] {len(page_items)} item(s) found")
        all_links.extend(page_items)
        time.sleep(REQUEST_DELAY_SECONDS)
    return all_links


def parse_article_html(url, html, card_title="", card_date=""):
    """Extract article title, date, and body paragraphs."""
    if not html:
        return None, "FAILED: empty response"
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("title")
    if title_tag and title_tag.get_text(strip=True).lower() == "login":
        return None, "GATED (requires login)"

    h1 = soup.find("h1")
    h1_text = h1.get_text(strip=True) if h1 else ""
    if h1_text.lower() == "login":
        return None, "GATED (requires login)"

    generic_titles = {"news & events", "supply chain advisors", "maritime research", "news", "sectors", "login"}
    if card_title:
        title = card_title
    elif h1_text and h1_text.lower() not in generic_titles:
        title = h1_text
    else:
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            title = og["content"]
        else:
            h2 = soup.find("h2")
            title = h2.get_text(strip=True) if h2 else (h1_text or "Drewry Article")

    date_span = soup.find("span", class_="aos-ArticleDate")
    date_text = date_span.get_text(strip=True) if date_span else card_date

    body_div = soup.find("div", class_="ao-Article")
    if body_div:
        paragraphs = [p.get_text(strip=True) for p in body_div.find_all("p") if p.get_text(strip=True)]
    else:
        paragraphs = []
        for div in soup.find_all("div", class_=True):
            cl = " ".join(div.get("class", []))
            if any(k in cl.lower() for k in ["article-body", "article", "entry-content"]):
                ps = [p.get_text(strip=True) for p in div.find_all("p") if p.get_text(strip=True)]
                if len(ps) > len(paragraphs):
                    paragraphs = ps

    if not paragraphs:
        return None, "FAILED: no paragraphs found"

    body_text = "\n\n".join(paragraphs)
    return {
        "url": url,
        "title": title,
        "date": date_text,
        "body": body_text,
        "paragraph_count": len(paragraphs),
    }, "OK"


def fetch_article_direct(url, card_title="", card_date=""):
    """Direct fetch using subscriber session headers."""
    html = fetch(url, label="article")
    if html is None:
        return None, "FAILED"
    return parse_article_html(url, html, card_title=card_title, card_date=card_date)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--browse", action="store_true",
                         help="Use the paginated archive (primary method, real coverage)")
    parser.add_argument("--max-pages", type=int, default=132,
                         help="Pages to fetch in --browse mode")
    parser.add_argument("--out-dir", default="knowledge/assets/drewry_opinions")
    parser.add_argument("--rediscover", action="store_true",
                         help="Force a fresh discovery pass even if cached links exist.")
    parser.add_argument("--cookie", default=os.environ.get("DREWRY_COOKIE"),
                         help="Authenticated browser session cookie string.")
    args = parser.parse_args()

    if args.cookie:
        HEADERS["Cookie"] = args.cookie
        print("Authenticated subscriber session active from --cookie / .env")

    os.makedirs(args.out_dir, exist_ok=True)
    all_links_cache = os.path.join(args.out_dir, "_all_discovered_links.json")

    if os.path.exists(all_links_cache) and not args.rediscover:
        print(f"Using cached link index from {all_links_cache}")
        with open(all_links_cache, "r", encoding="utf-8") as f:
            raw_links = json.load(f)
    else:
        raw_links = discover_all_items(max_pages=args.max_pages)
        with open(all_links_cache, "w", encoding="utf-8") as f:
            json.dump(raw_links, f, indent=2)
        print(f"Cached {len(raw_links)} discovered item(s) to {all_links_cache}")

    seen_slugs = set()
    unique_items = []
    for item in raw_links:
        url = item["url"]
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        if slug not in seen_slugs:
            seen_slugs.add(slug)
            unique_items.append(item)

    # Prioritize core opinion/briefing sections first
    unique_items.sort(key=lambda it: 0 if any(k in it["url"] for k in ["maritime-research", "logistics-executive", "/news/"]) else 1)

    print(f"\nTotal unique items in full archive: {len(unique_items)}")

    manifest_path = os.path.join(args.out_dir, "_manifest.csv")
    manifest_fields = ["url", "title", "date", "paragraph_count"]

    manifest_lock = threading.Lock()
    manifest_exists = os.path.exists(manifest_path)
    manifest_file = open(manifest_path, "a", newline="", encoding="utf-8")
    manifest_writer = csv.DictWriter(manifest_file, fieldnames=manifest_fields)
    if not manifest_exists:
        manifest_writer.writeheader()

    stats = {"ok": 0, "gated": 0, "failed": 0, "skipped": 0}

    for item in unique_items:
        url = item["url"]
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        out_path = os.path.join(args.out_dir, f"{slug}.md")

        if os.path.exists(out_path):
            stats["skipped"] += 1
            continue

        print(f"Fetching: {url}")
        try:
            article, status = fetch_article_direct(url, card_title=item.get("title", ""), card_date=item.get("date", ""))
            if article:
                with open(out_path, "w", encoding="utf-8") as f:
                    f.write(f"# {article['title']}\n\n")
                    if article["date"]:
                        f.write(f"*{article['date']}*\n\n")
                    f.write(article["body"])
                print(f"  OK -> {out_path} ({article['paragraph_count']} paragraphs)")
                with manifest_lock:
                    manifest_writer.writerow({k: article[k] for k in manifest_fields})
                    manifest_file.flush()
                    stats["ok"] += 1
            else:
                print(f"  [article] {status}: {url}")
                if "GATED" in status:
                    stats["gated"] += 1
                else:
                    stats["failed"] += 1
        except Exception as e:
            print(f"  [article] FAILED unexpectedly on {url}: {type(e).__name__}: {e}")
            stats["failed"] += 1

        time.sleep(REQUEST_DELAY_SECONDS)

    manifest_file.close()

    print(f"\n==========================================")
    print(f"Full Archive Harvest Summary:")
    print(f"  Total unique items in feed: {len(unique_items)}")
    print(f"  Newly downloaded this run: {stats['ok']}")
    print(f"  Already present on disk: {stats['skipped']}")
    print(f"  Gated (login required): {stats['gated']}")
    print(f"  Failed: {stats['failed']}")
    print(f"  Total articles now on disk: {stats['ok'] + stats['skipped']}")
    print(f"  Manifest at: {manifest_path}")
    print(f"==========================================\n")


if __name__ == "__main__":
    main()
