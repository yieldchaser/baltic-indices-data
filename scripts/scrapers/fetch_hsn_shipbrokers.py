"""
Master Multi-Broker Weekly Shipbrokers Report Ingestion Engine.
Crawls Hellenic Shipping News Weekly Shipbrokers category to ingest
Allied, Banchero Costa, Intermodal, Xclusiv, Advanced, Compass, Lion,
Optimaship, and Anchor weekly market reports into structured Markdown.
"""

import os
import re
import json
import time
import io
import urllib.request
import urllib.parse
from pathlib import Path
from bs4 import BeautifulSoup
try:
    import anydoc  # optional accelerator: pip install firecrawl-anydoc
except ImportError:  # fall back to pypdf extraction
    anydoc = None
import pypdf
import tempfile

BASE_URL = "https://www.hellenicshippingnews.com/category/weekly-shipbrokers-reports/"
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = REPO_ROOT / "reports" / "broker_reports"
CHECKPOINT_FILE = REPO_ROOT / "data" / "derived" / "broker_reports_checkpoint.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

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

def fetch_url(url, timeout=25):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()

def parse_pdf_stream(pdf_bytes):
    """Extract clean structured text from in-memory PDF stream using pypdf with AnyDoc fallback."""
    try:
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes), strict=False)
        pages_text = []
        for page in reader.pages:
            t = page.extract_text()
            if t:
                pages_text.append(t)
        combined = "\n\n".join(pages_text)
        if combined and len(combined.strip()) > 100:
            return combined
    except Exception:
        pass

    try:
        if anydoc is not None:
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tf:
                tf.write(pdf_bytes)
                tf_name = tf.name
            try:
                md = anydoc.to_markdown(tf_name)
                if md and len(md.strip()) > 50:
                    return md
            finally:
                if os.path.exists(tf_name):
                    os.remove(tf_name)
    except Exception:
        pass

    return "[PDF Extraction Error: no extractable text]"

def identify_broker(title, text):
    t_lower = (title + " " + text[:500]).lower()
    if "allied" in t_lower:
        return "allied"
    elif "bancosta" in t_lower or "banchero costa" in t_lower:
        return "bancosta"
    elif "intermodal" in t_lower:
        return "intermodal"
    elif "xclusiv" in t_lower:
        return "xclusiv"
    elif "advanced" in t_lower or "advansh" in t_lower:
        return "advanced_shipping"
    elif "compass" in t_lower:
        return "compass_maritime"
    elif "lion" in t_lower:
        return "lion_shipbrokers"
    elif "optima" in t_lower:
        return "optimaship"
    elif "anchor" in t_lower:
        return "anchor_shipbroking"
    return "general_broker"

def process_article(article_url, title, date_str):
    try:
        html = fetch_url(article_url).decode("utf-8", errors="ignore")
        soup = BeautifulSoup(html, "html.parser")
        
        content_div = soup.find("div", class_="entry-content") or soup.find("article") or soup
        pdf_links = []
        for a in content_div.find_all("a", href=True):
            if a["href"].lower().endswith(".pdf"):
                pdf_links.append(a["href"])
        
        extracted_text = ""
        pdf_source_url = ""
        if pdf_links:
            pdf_url = pdf_links[0]
            pdf_source_url = pdf_url
            try:
                pdf_bytes = fetch_url(pdf_url)
                extracted_text = parse_pdf_stream(pdf_bytes)
            except Exception as e:
                extracted_text = f"[Failed to fetch PDF {pdf_url}: {e}]"
        
        if not extracted_text or len(extracted_text) < 100:
            p_texts = [p.get_text().strip() for p in content_div.find_all("p") if p.get_text().strip()]
            extracted_text = "\n\n".join(p_texts)
        
        broker = identify_broker(title, extracted_text)
        
        year_match = re.search(r'\b(202[0-6])\b', date_str + " " + title)
        year = year_match.group(1) if year_match else "2026"
        
        slug = re.sub(r'[^a-zA-Z0-9_\-]+', '_', f"{broker}_{date_str}_{title}"[:80]).strip('_').lower()
        
        out_year_dir = OUTPUT_DIR / year
        out_year_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_year_dir / f"{slug}.md"
        
        md_content = f"""---
title: "{title.replace('"', '')}"
date: "{date_str}"
source: "{broker}"
category: "broker_report"
source_url: "{article_url}"
pdf_url: "{pdf_source_url}"
---

# {title}

**Broker**: {broker.replace('_', ' ').title()}  
**Published Date**: {date_str}  
**Source URL**: [{article_url}]({article_url})  

---

## Market Report Content

{extracted_text}
"""
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(md_content)
        
        return True, out_file
    except Exception as e:
        print(f"  [!] Error processing article {article_url}: {e}")
        return False, None

def crawl_reports(max_pages=3, delay_sec=1.0):
    checkpoint = load_checkpoint()
    processed_set = set(checkpoint.get("processed_urls", []))
    
    print(f"Starting Multi-Broker crawl. Known processed URLs: {len(processed_set)}")
    
    for page_num in range(1, max_pages + 1):
        page_url = BASE_URL if page_num == 1 else f"{BASE_URL}page/{page_num}/"
        print(f"\n--- Scraping Page {page_num}: {page_url} ---")
        
        try:
            html = fetch_url(page_url).decode("utf-8", errors="ignore")
            soup = BeautifulSoup(html, "html.parser")
            
            articles = []
            for h2 in soup.find_all(["h2", "h3"], class_=re.compile(r"entry-title|title")):
                a = h2.find("a", href=True)
                if a:
                    title = a.get_text().strip()
                    href = a["href"]
                    date_str = "2026-08-24"
                    parent = h2.find_parent(["article", "div"])
                    if parent:
                        date_elem = parent.find(["time", "span"], class_=re.compile(r"date|entry-date|published"))
                        if date_elem:
                            date_str = date_elem.get_text().strip()
                    articles.append((href, title, date_str))
            
            if not articles:
                for a in soup.find_all("a", href=True):
                    if "/weekly-shipbrokers-report" in a["href"] or "weekly" in a["href"]:
                        articles.append((a["href"], a.get_text().strip(), "2026-08-24"))
            
            print(f"Found {len(articles)} article links on page {page_num}")
            
            for href, title, date_str in articles:
                if href in processed_set:
                    continue
                print(f"  -> Ingesting: {title[:60]}... ({href})")
                success, out_path = process_article(href, title, date_str)
                if success:
                    processed_set.add(href)
                    checkpoint["processed_urls"] = list(processed_set)
                    checkpoint["last_page"] = page_num
                    save_checkpoint(checkpoint)
                time.sleep(delay_sec)
                
        except Exception as e:
            print(f"[!] Error on page {page_num}: {e}")
            break

    print(f"\nCrawl complete. Total processed articles in catalog: {len(processed_set)}")

if __name__ == "__main__":
    import sys
    pages = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    crawl_reports(max_pages=pages)
