"""
catalog_hellenic_shipbrokers.py
Rapidly catalogs all 3,573 historical weekly shipbroker reports from Hellenic Shipping News
using the high-speed WordPress REST API (bypassing all web ads and JS overlays).
Extracts: Broker, Date, Year, Week Number, Post URL, and PDF Download Link.
"""

import os
import re
import json
import urllib.request
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

OUT_DIR = "reports/shipbrokers"
os.makedirs(OUT_DIR, exist_ok=True)
CATALOG_CSV = os.path.join(OUT_DIR, "shipbrokers_manifest.csv")
CATALOG_JSON = os.path.join(OUT_DIR, "shipbrokers_manifest.json")

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
BASE_API = "https://www.hellenicshippingnews.com/wp-json/wp/v2/posts?categories=123&per_page=100&page="

KNOWN_BROKERS = {
    "affinity": "Affinity Research",
    "banchero": "Banchero Costa",
    "bancosta": "Banchero Costa",
    "advanced": "Advanced Shipping & Trading",
    "allied": "Allied Shipbroking",
    "intermodal": "Intermodal Shipbrokers",
    "xclusiv": "Xclusiv Shipbrokers",
    "gibson": "Gibson Shipbrokers",
    "clarksons": "Clarksons Hellas",
    "clarkson": "Clarksons Hellas",
    "lion": "Lion Shipbrokers",
    "agora": "Agora Shipbroking",
    "carriers": "Carriers Chartering",
    "optima": "Optima Shipbrokers",
    "intership": "Intership",
    "star asia": "Star Asia Shipbroking",
    "asiasis": "Star Asia Shipbroking",
    "cotzias": "Cotzias Intermodal",
    "weber": "WeberSeas",
    "golden destiny": "Golden Destiny",
    "anchor": "Anchor Shipbroking",
    "alibra": "Alibra Shipping",
    "fearnleys": "Fearnleys"
}

def detect_broker(title, content):
    text = (title + " " + content[:300]).lower()
    for k, v in KNOWN_BROKERS.items():
        if k in text:
            return v
    return "Other Broker"

def extract_week(title):
    m = re.search(r'(?:week|wk|w)\s*(\d{1,2})', title, re.I)
    if m:
        return f"W{int(m.group(1)):02d}"
    return "N/A"

def fetch_page(page_no):
    url = f"{BASE_API}{page_no}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    records = []
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            posts = json.loads(resp.read().decode("utf-8"))
            for p in posts:
                p_id = p["id"]
                title = p["title"]["rendered"]
                date_iso = p["date"]
                year = date_iso[:4]
                link = p["link"]
                content = p["content"]["rendered"]
                
                # Extract PDF URLs
                pdfs = re.findall(r'href=["\']\s*(https?://[^"\']+\.pdf)', content, re.I)
                pdf_url = pdfs[0].strip() if pdfs else ""
                
                # Broker & Week
                broker = detect_broker(title, content)
                week = extract_week(title)
                
                records.append({
                    "post_id": p_id,
                    "title": title,
                    "date": date_iso[:10],
                    "year": int(year),
                    "week": week,
                    "broker": broker,
                    "post_url": link,
                    "pdf_url": pdf_url,
                    "has_pdf": bool(pdf_url)
                })
    except Exception as e:
        print(f"[!] Error on page {page_no}: {e}")
    return records

def build_catalog():
    print("[*] Fetching total pages from WordPress REST API...")
    req = urllib.request.Request(f"{BASE_API}1", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=15) as resp:
        total_pages = int(resp.headers.get("X-WP-TotalPages", 36))
        total_posts = int(resp.headers.get("X-WP-Total", 3573))
        
    print(f"[+] Total Posts: {total_posts} across {total_pages} API pages.")
    all_records = []
    
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(fetch_page, p): p for p in range(1, total_pages + 1)}
        for fut in as_completed(futures):
            p_num = futures[fut]
            res = fut.result()
            all_records.extend(res)
            print(f"  [+] Page {p_num}/{total_pages} indexed ({len(res)} posts)")
            
    df = pd.DataFrame(all_records).sort_values("date", ascending=False)
    df.to_csv(CATALOG_CSV, index=False)
    with open(CATALOG_JSON, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2)
        
    print(f"\n=== CATALOG GENERATION COMPLETE ===")
    print(f"Total Reports Cataloged: {len(df)}")
    print(f"Reports with Direct PDF: {len(df[df['has_pdf']])} ({round(len(df[df['has_pdf']])/len(df)*100, 1)}%)")
    print(f"\nTop 10 Brokers:")
    print(df['broker'].value_counts().head(10))
    print(f"\nYear Breakdown:")
    print(df['year'].value_counts().sort_index(ascending=False))

if __name__ == "__main__":
    build_catalog()
