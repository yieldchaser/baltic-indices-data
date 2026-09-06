"""
fetch_drewry_opinions_incremental.py
Automated incremental scraper for Drewry Maritime Opinions & Executive Briefings.

Runs incrementally (e.g. in GitHub Actions every Friday).
- Queries the latest 1-2 pages of the Drewry opinion browser.
- Identifies newly posted articles not yet in reports/drewry/opinions/.
- Extracts clean markdown with frontmatter and appends to _manifest.csv.
- Strictly paced (3.5s delay) to respect Affino CMS limits.
- Supports automatic Monid proxy fallback if local IP hits HTTP 429 cooldown.
- Completely idempotent: skips already archived articles.
"""

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
OPINIONS_DIR = REPO_ROOT / "reports" / "drewry" / "opinions"
MANIFEST_PATH = OPINIONS_DIR / "_manifest.csv"
CHECKPOINT_PATH = REPO_ROOT / "data" / "derived" / "drewry_opinions_checkpoint.json"

BASE_URL = "https://www.drewry.co.uk"
BROWSE_URL = f"{BASE_URL}/maritime-research-opinion-browser"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

REQUEST_DELAY = 3.5
HAS_MONID = shutil.which("monid") is not None


def scrape_with_monid(url):
    """Fetch URL via Monid proxy when local IP is in 429 cooldown."""
    if not HAS_MONID:
        return None, "monid_unavailable"
    try:
        q = json.dumps({"url": url})
        escaped_q = q.replace('"', '\\"')
        cmd = f'monid run -p context.dev -e /web/scrape/html --query "{escaped_q}" --wait -j'
        res = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding="utf-8", errors="replace")
        stdout = res.stdout or ""
        start = stdout.find("{")
        end = stdout.rfind("}") + 1
        if start == -1 or end <= start:
            return None, "monid_invalid_response"
        data = json.loads(stdout[start:end])
        html = data.get("output", {}).get("html", "")
        if html:
            return html, "OK"
        return None, "monid_empty_html"
    except Exception as e:
        return None, f"monid_error_{e}"


def load_known_slugs():
    """Load existing article slugs from disk and manifest."""
    slugs = set()
    if OPINIONS_DIR.exists():
        for p in OPINIONS_DIR.glob("*.md"):
            slugs.add(p.stem)
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("slug"):
                    slugs.add(row["slug"])
    return slugs


def discover_recent_items(max_pages=2):
    """Fetch recent browse pages and extract article cards."""
    items = []
    for page in range(1, max_pages + 1):
        data = {
            "SelTopics": "",
            "gotoPage": str(page),
            "ItemListSorting": "0",
            "txtKeyword": "",
        }
        html = None
        for attempt in range(2):
            try:
                post_headers = {**HEADERS, "Content-Type": "application/x-www-form-urlencoded"}
                resp = requests.post(BROWSE_URL, headers=post_headers, data=data, timeout=15)
                if resp.status_code == 200:
                    html = resp.text
                    break
                elif resp.status_code == 429:
                    print(f"  [Notice] Browse page {page} got HTTP 429, trying Monid fallback...", flush=True)
                    m_html, status = scrape_with_monid(BROWSE_URL)
                    if status == "OK" and m_html:
                        html = m_html
                        break
            except requests.exceptions.RequestException:
                time.sleep(1)

        if not html:
            # Try GET fallback
            try:
                resp = requests.get(BROWSE_URL, headers=HEADERS, timeout=15)
                if resp.status_code == 200:
                    html = resp.text
                elif resp.status_code == 429:
                    m_html, status = scrape_with_monid(BROWSE_URL)
                    if status == "OK" and m_html:
                        html = m_html
            except Exception:
                pass

        if not html:
            print(f"  [WARN] Failed to fetch browse page {page}", flush=True)
            continue

        soup = BeautifulSoup(html, "html.parser")
        page_items = []
        for a in soup.find_all("a", href=True):
            if a.get_text(strip=True).lower() == "learn more":
                full_url = urljoin(BASE_URL, a["href"])
                card = a.find_parent("div", class_=lambda c: c and "item" in c.lower()) or a.find_parent("li") or a.find_parent("div")
                h_title = card.find(["h2", "h3"]) if card else None
                title = h_title.get_text(strip=True) if h_title else ""
                h_date = card.find("h4") if card else None
                date_str = h_date.get_text(strip=True) if h_date else ""
                page_items.append({"url": full_url, "title": title, "date": date_str})

        print(f"  [Browse page {page}] Discovered {len(page_items)} article link(s)", flush=True)
        items.extend(page_items)
        time.sleep(REQUEST_DELAY)

    return items


def parse_article_page(url, cookie=None, card_title="", card_date=""):
    """Fetch and parse single article text."""
    headers = dict(HEADERS)
    if cookie:
        headers["Cookie"] = cookie

    html = None
    for attempt in range(2):
        try:
            resp = requests.get(url, headers=headers, timeout=20)
            if resp.status_code == 200:
                html = resp.text
                break
            elif resp.status_code == 429:
                print(f"    [Notice] Article {url} got HTTP 429, trying Monid fallback...", flush=True)
                m_html, status = scrape_with_monid(url)
                if status == "OK" and m_html:
                    html = m_html
                    break
        except requests.exceptions.RequestException:
            time.sleep(1)

    if not html:
        return None, "HTTP_ERROR"

    soup = BeautifulSoup(html, "html.parser")

    # Check for gating
    h1 = soup.find("h1")
    h1_text = h1.get_text(strip=True) if h1 else ""
    if h1_text.lower() == "login" or (soup.title and "login" in soup.title.get_text(strip=True).lower()):
        return None, "GATED"

    # Extract Title
    title = card_title
    if not title:
        if h1_text and h1_text.lower() not in ["login", "news", "sectors", "maritime research"]:
            title = h1_text
        else:
            og = soup.find("meta", property="og:title")
            title = og["content"] if og and og.get("content") else (h1_text or "Drewry Article")

    # Extract Date
    date_span = soup.find("span", class_="aos-ArticleDate")
    date_text = date_span.get_text(strip=True) if date_span else card_date

    # Extract Body paragraphs
    body_div = soup.find("div", class_="ao-Article")
    paragraphs = []
    if body_div:
        paragraphs = [p.get_text(strip=True) for p in body_div.find_all("p") if p.get_text(strip=True)]
    else:
        for div in soup.find_all("div", class_=True):
            cl = " ".join(div.get("class", []))
            if any(k in cl.lower() for k in ["article-body", "article", "entry-content"]):
                ps = [p.get_text(strip=True) for p in div.find_all("p") if p.get_text(strip=True)]
                if len(ps) > len(paragraphs):
                    paragraphs = ps

    if not paragraphs:
        return None, "NO_CONTENT"

    body = "\n\n".join(paragraphs)
    return {
        "url": url,
        "title": title,
        "date": date_text,
        "paragraphs": len(paragraphs),
        "body": body,
    }, "OK"


def append_to_manifest(entry):
    """Append new article row to _manifest.csv."""
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    file_exists = MANIFEST_PATH.exists()

    with open(MANIFEST_PATH, "a", newline="", encoding="utf-8") as f:
        fieldnames = ["slug", "url", "title", "date", "status", "paragraphs", "file_path"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(entry)


def run_incremental(max_pages=2, cookie=None, dry_run=False):
    print("=== Drewry Opinions Incremental Ingest ===", flush=True)
    known_slugs = load_known_slugs()
    print(f"Existing known opinion articles: {len(known_slugs)}", flush=True)

    cookie = cookie or os.environ.get("DREWRY_COOKIE")
    recent_items = discover_recent_items(max_pages=max_pages)

    # Deduplicate within batch
    unique_items = []
    seen_in_batch = set()
    for it in recent_items:
        slug = it["url"].rstrip("/").rsplit("/", 1)[-1]
        if slug not in seen_in_batch:
            seen_in_batch.add(slug)
            unique_items.append((slug, it))

    new_articles = []
    for slug, item in unique_items:
        if slug in known_slugs:
            continue
        new_articles.append((slug, item))

    print(f"New unpublished/unharvested articles found: {len(new_articles)}", flush=True)
    if not new_articles:
        print("Everything is up to date.", flush=True)
        return

    OPINIONS_DIR.mkdir(parents=True, exist_ok=True)
    saved_count = 0

    for idx, (slug, item) in enumerate(new_articles, 1):
        print(f"[{idx}/{len(new_articles)}] Fetching: {slug} ...", flush=True)
        if dry_run:
            print(f"  [DRY RUN] Would fetch and save {slug}", flush=True)
            continue

        data, status = parse_article_page(item["url"], cookie=cookie, card_title=item["title"], card_date=item["date"])
        if status == "OK" and data:
            md_path = OPINIONS_DIR / f"{slug}.md"
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(f"# {data['title']}\n\n")
                if data["date"]:
                    f.write(f"*{data['date']}*\n\n")
                f.write(data["body"] + "\n")

            manifest_entry = {
                "slug": slug,
                "url": item["url"],
                "title": data["title"],
                "date": data["date"],
                "status": "OK",
                "paragraphs": data["paragraphs"],
                "file_path": f"reports/drewry/opinions/{slug}.md",
            }
            append_to_manifest(manifest_entry)
            saved_count += 1
            print(f"  -> Saved {md_path.name} ({data['paragraphs']} paragraphs)", flush=True)
        else:
            print(f"  -> Skipped ({status})", flush=True)

        time.sleep(REQUEST_DELAY)

    print(f"Completed incremental ingest: {saved_count} new article(s) saved.", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Drewry Opinions Incremental Ingest")
    parser.add_argument("--max-pages", type=int, default=2, help="Browse pages to scan (default: 2)")
    parser.add_argument("--cookie", default=None, help="Drewry session cookie")
    parser.add_argument("--dry-run", action="store_true", help="Scan without saving")
    args = parser.parse_args()

    run_incremental(max_pages=args.max_pages, cookie=args.cookie, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
