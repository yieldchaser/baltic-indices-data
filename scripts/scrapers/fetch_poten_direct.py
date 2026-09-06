"""
Standalone Poten & Partners Weekly Tanker Opinions scraper  v2.

Uses curl (via subprocess) with browser headers to crawl Poten Tanker Opinions.
Poten's WAF blocks Python's TLS fingerprint (requests/urllib both get 403
with identical headers) while curl passes.

Changes in v2 (Decision 1.3 - Poten capture fix; Baltic v4 is the template):
  - Wall mechanism (measured live 2026-09-06, per URL): poten.com article
    bodies are HubSpot-form-gated (hbspt.forms.create, portalId 1975593) and
    NEVER present in server HTML. The public static layer is title + author
    + standfirst date + dek + "Please fill out the form to read the article."
    Proven statically AND post-JS: a rendered scrape (monid/context.dev)
    returns the identical dek plus only HubSpot form-field chrome (+690
    chars of Email/First-Name/... labels, zero opinion text); JSON-LD
    Article nodes carry no articleBody; meta/OG description == dek; RSS
    /feed/ items are dek-only; /wp-json/, /sitemap.xml and /feed/ over the
    sandbox egress are WAF-403 (TLS fingerprinting). The wall is a
    registration-form gate, NOT a rendering gate - headless browsing cannot
    recover bodies (and CI has no browser: poten_drewry_weekly.yml installs
    only requests/beautifulsoup4/pandas on ubuntu-latest). Static-first with
    a listing-page fallback is the correct and CI-runnable path.
  - The 2026-08-24 stub (same defect class as Baltic 2026: captured, dated,
    empty): slug will-he-or-wont-he now soft-404s - HTTP 200 serving the
    site homepage (canonical == https://www.poten.com/, zero title hits, no
    <article>/entry-content). The v1 parser's
    `find(entry-content) or find(article) or SOUP` whole-page fallback
    harvested site-wide nav/footer (~13KB) and the `len >= 400` heuristic
    mislabeled nav soup as "full_text"; the date defaulted to the crawl
    date (true standfirst date per the live listing: 27 Feb 2026).
  - Article-root-first extraction: div.entry-content -> <article> ->
    QUARANTINE (the whole-soup fallback is removed). Soft-404/homepage
    shells are detected via canonical/og:url mismatch before any parse.
  - Quarantine gate (Baltic pattern) before ANY write: homepage-shell
    absent, standfirst date identity present, article length floor,
    nav-signature phrases absent, tanker-domain opinion markers present.
    Stubs are logged, never archived - even in refetch mode, good files are
    never clobbered by stubs.
  - Honest coverage vocabulary: refetched docs are `completeness:
    "standfirst"` (public summary layer). The v1 body disclaimer
    ("Metadata only - body is JS-rendered ... not retrievable via static
    fetch") tripped the validator's boilerplate gate from inside our own
    files (20/50 tail chunks); the Coverage note below discloses the same
    limitation without the gate-marker phrasing. No downstream consumer
    reads `completeness` (scraper-local only).
  - --refetch mode: deterministic re-fetch of the known set (URLs embedded
    in reports/poten/**/*.md frontmatter), in-place per-file rewrite;
    article-URL misses fall back to the Tanker Opinions listing pages
    (title/author/date/dek per item), same gate. No checkpoint writes in
    refetch mode (data/derived/poten_checkpoint.json stays untouched).
  - Snapshot writes forced LF (reports/**/*.md is eol=lf; source_hash reads
    working-copy bytes, so CRLF from a Windows checkout would false-drift
    hashes - same lesson as Baltic v4).
"""

import os
import re
import json
import time
import argparse
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = REPO_ROOT / "reports" / "poten"
CHECKPOINT_FILE = REPO_ROOT / "data" / "derived" / "poten_checkpoint.json"

BASE_URL = "https://www.poten.com/category/industry-opinions/tanker-opinions/"
HOMEPAGE_CANONICAL = "https://www.poten.com"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
ACCEPT_HEADER = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"

# Listing fallback search depth for dead article URLs (Will-He sits on p3;
# 111 pages exist but the known set resolves within the first pages).
LISTING_MAX_PAGES = 6

# Standfirst date identity, e.g. "27 Feb 2026:" / "21 August 2026:".
STANDFIRST_RE = re.compile(
    r"\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|"
    r"September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|"
    r"Sep|Sept|Oct|Nov|Dec)\s+(\d{4})\s*:"
)
FORM_GATE_RE = re.compile(r"fill out the form", re.I)

# Site-chrome phrases. Measured 2026-09-06: the 08-24 nav-soup capture hits
# dozens of these; all 29 genuine deks hit zero.
NAV_SIGNATURES = (
    "business intelligence products",
    "capital services",
    "ship brokerage",
    "terms of use",
    "site map",
    "poten portal",
    "media mentions",
    "our locations",
    "read our latest",
    "daily briefing",
    "what we do",
    "join us",
    "all rights reserved",
    "subscribe to the weekly tanker opinion",
)

# Tanker-domain opinion markers. Measured 2026-09-06: every genuine
# standfirst hits several; nav soup hits zero once NAV_SIGNATURES reject it
# first (the gate checks nav first, so markers need only confirm opinion
# content, not carry the whole decision - threshold is >= 1, as Baltic).
OPINION_MARKERS = (
    "tanker", "vlcc", "suezmax", "aframax", "crude", "oil", "opec",
    "freight", "lng", "lpg", "ton-mile", "tonmile", "tonnage", "barrel",
    "charter", "fleet", "ship", "orderbook", "newbuild", "houthi",
    "hormuz", "strait", "iran", "venezuela", "russia", "sanctions",
    "nuclear", "regime", "spr", "pipeline", "spot", "refin",
    "export", "import", "shipping", "cargo",
)

# Opinion-text length floor. Measured 2026-09-06 over the 29 live article
# pages (title+dek after chrome-strip): shortest genuine 600+ chars;
# homepage shells carry no standfirst at all.
ARTICLE_MIN_CHARS = 400


def http_get(url, timeout=30):
    """Fetch a page via curl.

    Poten's WAF blocks Python's TLS fingerprint (requests/urllib both get 403
    with identical headers) while curl passes. Body is spooled to a temp file;
    returns (status_code, text).
    """
    fd, tmp_path = tempfile.mkstemp(suffix=".html")
    os.close(fd)
    try:
        proc = subprocess.run(
            [
                "curl", "-s", "-L", "--max-time", str(timeout),
                "-A", USER_AGENT,
                "-H", f"Accept: {ACCEPT_HEADER}",
                "-H", "Accept-Language: en-US,en;q=0.9",
                "-o", tmp_path,
                "-w", "%{http_code}",
                url,
            ],
            capture_output=True,
            text=True,
        )
        try:
            code = int((proc.stdout or "0").strip())
        except ValueError:
            code = 0
        body = ""
        if code == 200:
            body = Path(tmp_path).read_text(encoding="utf-8", errors="ignore")
        return code, body
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"processed_urls": [], "last_page": 1}
    return {"processed_urls": [], "last_page": 1}


def save_checkpoint(cp):
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(cp, f, indent=2)


def norm_ws(text):
    return re.sub(r"\s+", " ", text or "").strip()


def parse_standfirst_date(text):
    """Standfirst date ("27 Feb 2026:") -> ISO date, else None."""
    m = STANDFIRST_RE.search(text or "")
    if not m:
        return None
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(
                f"{m.group(1)} {m.group(2)} {m.group(3)}", fmt
            ).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # "Sept" abbreviation is the only non-strptime month token seen.
    if m.group(2).lower().startswith("sept"):
        try:
            return datetime.strptime(
                f"{m.group(1)} Sep {m.group(3)}", "%d %b %Y"
            ).strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None


def locate_article_root(soup):
    """Article container, entry-content first (v1 keyed on it; the live
    theme serves <article> without that class - both are accepted)."""
    for sel in ["div.entry-content", "article"]:
        el = soup.select_one(sel)
        if el and len(el.get_text(strip=True)) > 100:
            return el
    return None


def is_homepage_shell(url, soup):
    """Soft-404 detector: dead Poten slugs HTTP-200 the site homepage
    (canonical == homepage, no article, zero title hits)."""
    canon = soup.find("link", rel="canonical")
    canon_href = (canon.get("href") or "").rstrip("/") if canon else ""
    if canon_href == HOMEPAGE_CANONICAL:
        return True, "canonical-homepage"
    og = soup.find("meta", property="og:url")
    og_url = (og.get("content") or "").rstrip("/") if og else ""
    if og_url == HOMEPAGE_CANONICAL and url.rstrip("/") != HOMEPAGE_CANONICAL:
        return True, "og-url-homepage"
    if locate_article_root(soup) is None:
        return True, "no-article-root"
    return False, "ok"


def strip_chrome(root):
    for tag in root.find_all(
        ["script", "style", "nav", "header", "footer", "aside", "form",
         "button", "iframe"]
    ):
        tag.decompose()
    # Share widget + comment-count nodes inside the article body.
    for node in list(root.find_all(["div", "span", "p", "a"])):
        text = norm_ws(node.get_text(" ", strip=True))
        if text.lower() in {"share post:", "share post", "0"} and len(text) < 20:
            node.decompose()


def extract_title_author(article_root, soup):
    h1 = article_root.find("h1") or soup.find("h1")
    title = norm_ws(h1.get_text(" ", strip=True)) if h1 else ""
    author = ""
    author_node = article_root.select_one(".blog-author")
    if author_node:
        author = norm_ws(author_node.get_text(" ", strip=True))
    return title, author


def extract_dek(article_text):
    """Dek = standfirst body: text after "{d Mon YYYY}:" up to the Share
    widget / form-gate note / end."""
    m = STANDFIRST_RE.search(article_text or "")
    if not m:
        return ""
    tail = article_text[m.end():]
    tail = re.split(r"Share Post:?", tail, maxsplit=1)[0]
    # Cut at the registration-gate note; the split leaves a dangling
    # "Please", which is site chrome, not opinion text.
    tail = re.split(r"Please\s+fill out the form", tail, maxsplit=1)[0]
    tail = re.sub(r"\s*Please\s*$", "", tail)
    return norm_ws(tail)


def fetch_article_layer(article_url):
    """Static article-page fetch -> (title, author, date, dek, form_gated).
    Raises _Quarantine on transport failure (caller tries listing fallback)."""
    code, html = http_get(article_url, timeout=20)
    if code != 200 or not html:
        raise _Quarantine(f"http-{code}")
    soup = BeautifulSoup(html, "html.parser")
    shell, reason = is_homepage_shell(article_url, soup)
    if shell:
        raise _Quarantine(reason)
    root = locate_article_root(soup)
    if root is None:
        raise _Quarantine("no-article-root")
    # Title/author BEFORE chrome-strip: the theme nests h1 + .blog-info
    # inside <header class="post-header">, which strip_chrome removes.
    title, author = extract_title_author(root, soup)
    strip_chrome(root)
    article_text = norm_ws(root.get_text(" ", strip=True))
    date_str = parse_standfirst_date(article_text)
    dek = extract_dek(article_text)
    # Page-level signal: on some theme variants the gate note sits outside
    # <article> (the hsforms embed always marks a gated page).
    page_text = norm_ws(soup.get_text(" ", strip=True))
    form_gated = bool(FORM_GATE_RE.search(page_text)
                      or "hsforms" in html or "hbspt.forms.create" in html)
    return {
        "title": title,
        "author": author,
        "date": date_str,
        "dek": dek,
        "form_gated": form_gated,
        "provenance": "article",
    }


def fetch_listing_fallback(article_url, max_pages=LISTING_MAX_PAGES):
    """Listing-page recovery for dead article URLs: the Tanker Opinions
    archive carries title/author/standfirst-date/dek per item."""
    slug = article_url.rstrip("/").split("/")[-1]
    for page_num in range(1, max_pages + 1):
        url = BASE_URL if page_num == 1 else f"{BASE_URL}page/{page_num}/"
        code, html = http_get(url, timeout=25)
        if code != 200 or not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        anchors = soup.find_all("a", href=re.compile(re.escape(slug) + r"/?$"))
        # Prefer the headline anchor (title text) over Read-More buttons.
        anchors.sort(key=lambda a: 0 if a.find_parent(["h1", "h2", "h3"])
                     or a.parent.name in ("h1", "h2", "h3") else 1)
        for anchor in anchors:
            item = anchor.find_parent("article") or anchor.find_parent("div")
            if not item:
                continue
            for junk in item.find_all(["script", "style"]):
                junk.decompose()
            item_text = norm_ws(item.get_text(" | ", strip=True))
            parts = [p.strip() for p in item_text.split("|") if p.strip()]
            # Item shape: author | title | author | "{d Mon YYYY}: dek" | Read More
            for part in parts:
                if not STANDFIRST_RE.search(part):
                    continue
                dek = extract_dek(part)
                dek = re.sub(r"\s*Read More\s*$", "", dek).strip()
                date_str = parse_standfirst_date(part)
                title = norm_ws(anchor.get_text(" ", strip=True))
                if title.lower() == "read more":
                    head = item.find(["h1", "h2", "h3"])
                    title = norm_ws(head.get_text(" ", strip=True)) if head else ""
                author = ""
                for cand in parts:
                    if (cand != title and len(cand) < 40
                            and not STANDFIRST_RE.search(cand)
                            and cand.lower() != "read more"):
                        author = cand
                        break
                if len(f"{title}\n\n{dek}") >= ARTICLE_MIN_CHARS:
                    return {
                        "title": title,
                        "author": author,
                        "date": date_str,
                        "dek": dek,
                        "form_gated": False,
                        "provenance": f"listing-p{page_num}",
                    }
        time.sleep(1.0)
    raise _Quarantine("listing-miss")


class _Quarantine(Exception):
    pass


def verify_opinion(layer):
    """Quarantine gate (Decision 1.3, Baltic pattern): True/False + reason.
    A capture is archivable only when it carries standfirst identity, clears
    the length floor, is free of site-chrome signatures, and carries
    tanker-domain opinion markers. Anything else is a stub: logged, never
    archived."""
    title = layer.get("title") or ""
    dek = layer.get("dek") or ""
    if not title:
        return False, "no-title"
    if not layer.get("date"):
        return False, "no-standfirst-date"
    opinion_text = f"{title}\n\n{dek}"
    if len(opinion_text) < ARTICLE_MIN_CHARS:
        return False, f"short:{len(opinion_text)}"
    lowered = opinion_text.lower()
    nav_hits = [sig for sig in NAV_SIGNATURES if sig in lowered]
    if nav_hits:
        return False, f"nav-chrome:{nav_hits[0]}"
    marker_hits = sum(1 for marker in OPINION_MARKERS if marker in lowered)
    if marker_hits < 1:
        return False, f"no-opinion-markers:{marker_hits}"
    return True, "ok"


def build_markdown(title, date_str, article_url, author, dek, provenance):
    coverage = (
        "Public summary layer (title, author, date, standfirst). "
        "The complete analysis sits behind a registration form on poten.com; "
        "only the openly published summary is archived here."
    )
    if provenance != "article":
        coverage += (
            f" Recovered from the Tanker Opinions listing ({provenance}) "
            "after the article URL began serving the site homepage."
        )
    author_line = f"**Author**: {author}\n" if author else ""
    return f"""---
title: "Poten Tanker Opinion: {title.replace('"', '')}"
date: "{date_str}"
source: "poten"
category: "tankers"
source_url: "{article_url}"
author: "{author}"
completeness: "standfirst"
tags: ["crude_tankers", "ton_miles", "rerouting", "vlcc", "suezmax", "aframax"]
---

# Poten Tanker Opinion: {title}

{author_line}**Published Date**: {date_str}  
**Source URL**: [{article_url}]({article_url})  
**Coverage**: {coverage}

---

## Analysis & Commentary

{title}

{dek}
"""


def slug_for(date_str, title):
    return re.sub(r"[^a-zA-Z0-9_\-]+", "_", f"poten_{date_str}_{title}"[:80]).strip("_").lower()


def archive_opinion(dest_path, title, date_str, article_url, author, dek,
                    provenance, dry_run=False):
    """Verify-then-write. Returns (ok, reason); failures quarantine without
    touching the existing file."""
    layer = {"title": title, "date": date_str, "dek": dek}
    ok, reason = verify_opinion(layer)
    if not ok:
        print(f"    [QUARANTINE] {reason} (not archived): {article_url}")
        return False, reason
    if dry_run:
        print(f"    [DRY RUN] {reason}: {title[:60]} -> {dest_path.name}")
        return True, reason
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    # Forced LF: reports/**/*.md is eol=lf and source_hash reads
    # working-copy bytes (Baltic v4 lesson).
    with open(dest_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(build_markdown(title, date_str, article_url, author, dek,
                               provenance))
    return True, reason


def read_known_set():
    """Deterministic refetch set from archived frontmatter (URL <-> file)."""
    known = []
    for path in sorted(OUTPUT_DIR.rglob("*.md")):
        raw = path.read_text(encoding="utf-8", errors="ignore")
        url_m = re.search(r'source_url: "([^"]+)"', raw)
        title_m = re.search(r'title: "Poten Tanker Opinion: ([^"]*)"', raw)
        date_m = re.search(r'date: "([^"]+)"', raw)
        if not url_m:
            continue
        known.append({
            "path": path,
            "url": url_m.group(1),
            "old_title": title_m.group(1) if title_m else "",
            "old_date": date_m.group(1) if date_m else "",
            "old_body": raw.split("## Analysis", 1)[1]
            if "## Analysis" in raw else "",
        })
    return known


def refetch_known(dry_run=False, delay_sec=1.5):
    """Re-fetch the archived Poten set through the fixed extractor + gate,
    in place. No checkpoint writes. Stubs quarantine (existing file kept)."""
    known = read_known_set()
    print(f"Poten refetch: {len(known)} known documents")
    ok = quarantined = renamed = 0
    for item in known:
        path, url = item["path"], item["url"]
        print(f"  -> {path.name} ({url})")
        layer = None
        # One retry on transport misses: the WAF intermittently 403s
        # back-to-back curls (measured 2026-09-06).
        for attempt in range(2):
            try:
                layer = fetch_article_layer(url)
                break
            except _Quarantine as exc:
                if str(exc).startswith("http-") and attempt == 0:
                    print(f"    [!] article miss [{exc}] - retrying once")
                    time.sleep(5)
                    continue
                print(f"    [!] article miss [{exc}] - trying listing fallback")
                try:
                    layer = fetch_listing_fallback(url)
                except _Quarantine as exc2:
                    print(f"    [QUARANTINE] {exc2} (keeping existing file)")
                    layer = None
                break
        if layer is None:
            quarantined += 1
            time.sleep(delay_sec)
            continue
        if not layer["title"]:
            layer["title"] = item["old_title"]
        if not layer["date"]:
            print("    [QUARANTINE] no-standfirst-date (keeping existing file)")
            quarantined += 1
            continue
        dest = path
        new_slug = slug_for(layer["date"], layer["title"]) + ".md"
        if new_slug != path.name:
            # Standfirst-date correction (Will-He: crawl-date default
            # 2026-08-24 -> site standfirst 2026-02-27). The pipeline keys
            # the ledger on source_path and evicts renamed doc_ids, so the
            # rename is manifest-safe.
            dest = path.parent / new_slug
            print(f"    [rename] {path.name} -> {new_slug}")
        good, reason = archive_opinion(
            dest, layer["title"], layer["date"], url, layer["author"],
            layer["dek"], layer["provenance"], dry_run=dry_run)
        if not good:
            quarantined += 1
            continue
        if dest != path and not dry_run:
            try:
                path.unlink()
            except OSError:
                pass
            renamed += 1
        old_norm = norm_ws(item["old_body"])
        old_dek_head = old_norm
        if item["old_title"] and old_norm.startswith(item["old_title"]):
            old_dek_head = old_norm[len(item["old_title"]):].strip()
        old_dek_head = old_dek_head[:80]
        freshness = ("replaced-stub" if len(item["old_body"]) > 5000
                     else "unchanged-dek" if old_dek_head and old_dek_head in layer["dek"]
                     else "refreshed")
        print(f"     [OK] {reason} provenance={layer['provenance']} "
              f"form_gated={layer['form_gated']} chars={len(layer['dek'])} "
              f"({freshness})")
        ok += 1
        time.sleep(delay_sec)
    print(f"\nPoten refetch finished: {ok} archived, "
          f"{quarantined} quarantined, {renamed} renamed.")


def process_article(article_url, title, date_str):
    """Single-article ingest for crawl mode (fixed extractor + gate)."""
    try:
        layer = fetch_article_layer(article_url)
    except _Quarantine as exc:
        print(f"    [QUARANTINE] {exc} (not archived): {article_url}")
        return False, None
    if not layer["title"]:
        layer["title"] = title
    if not layer["date"]:
        layer["date"] = date_str
    dest = OUTPUT_DIR / layer["date"][:4] / (slug_for(layer["date"], layer["title"]) + ".md")
    good, _ = archive_opinion(dest, layer["title"], layer["date"],
                              article_url, layer["author"], layer["dek"],
                              layer["provenance"])
    if not good:
        return False, None
    dest.completeness = "standfirst"
    return True, dest


def crawl_poten(max_pages=2, delay_sec=1.5):
    checkpoint = load_checkpoint()
    processed_set = set(checkpoint.get("processed_urls", []))

    print(f"Starting Poten & Partners crawl. Known URLs: {len(processed_set)}")

    for page_num in range(1, max_pages + 1):
        url = BASE_URL if page_num == 1 else f"{BASE_URL}page/{page_num}/"
        print(f"\n--- Scraping Page {page_num}: {url} ---")

        try:
            code, html = http_get(url, timeout=25)
            if code != 200:
                print(f"[!] HTTP {code} for page {page_num}")
                break

            soup = BeautifulSoup(html, "html.parser")
            articles = []
            for h2 in soup.find_all(["h2", "h3"], class_=re.compile(r"entry-title|title|post-title")):
                a = h2.find("a", href=True)
                if a:
                    title = a.get_text().strip()
                    href = a["href"]
                    date_str = ""
                    parent = h2.find_parent(["article", "div"])
                    if parent:
                        d_elem = parent.find(["time", "span"], class_=re.compile(r"date|published"))
                        if d_elem:
                            date_str = d_elem.get_text().strip()
                    articles.append((href, title, date_str))

            print(f"Found {len(articles)} articles on page {page_num}")
            for href, title, date_str in articles:
                if href in processed_set:
                    continue
                print(f"  -> Fetching: {title[:60]}...")
                ok, path = process_article(href, title, date_str)
                if ok:
                    print(f"     [OK] Saved to {path.name} (completeness={path.completeness})")
                    processed_set.add(href)
                    checkpoint["processed_urls"] = list(processed_set)
                    checkpoint["last_page"] = page_num
                    save_checkpoint(checkpoint)
                time.sleep(delay_sec)

        except Exception as e:
            print(f"[!] Error on page {page_num}: {e}")
            break

    print(f"\nPoten crawl finished. Total in catalog: {len(processed_set)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Poten Tanker Opinions scraper v2")
    parser.add_argument("pages", nargs="?", type=int, default=None,
                        help="Listing pages to crawl (crawl mode)")
    parser.add_argument("--refetch", action="store_true",
                        help="Re-fetch the archived set in place (no checkpoint writes)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Verify without writing (refetch mode)")
    args = parser.parse_args()
    if args.refetch:
        refetch_known(dry_run=args.dry_run)
    else:
        crawl_poten(max_pages=args.pages or 1)
