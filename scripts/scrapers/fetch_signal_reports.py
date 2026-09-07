"""
fetch_signal_reports.py
Production harvester for The Signal Group (Signal Ocean / Signal Maritime) Intelligence:
- Unions sitemap.xml with live collection pages (/weekly-market-monitor and /newsroom)
- Ingests all Weekly Market Monitors & Radars
- Ingests all Market Newsroom Research, Annual Reviews & Deep Dives
- Saves high-fidelity clean Markdown (with headings, tables, figures, image URLs)
- Preserves raw HTML snapshots
- Checks and downloads any linked PDF reports
- Idempotent and multi-threaded
"""

import os
import sys
import re
import time
import urllib.request
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

BASE_DIR = "reports/signal"
DIR_MONITORS = os.path.join(BASE_DIR, "monitors")
DIR_NEWSROOM = os.path.join(BASE_DIR, "newsroom")
DIR_HTML = os.path.join(BASE_DIR, "html")
DIR_PDFS = os.path.join(BASE_DIR, "pdfs")

for d in [DIR_MONITORS, DIR_NEWSROOM, DIR_HTML, DIR_PDFS]:
    os.makedirs(d, exist_ok=True)

MANIFEST_PATH = os.path.join(BASE_DIR, "signal_manifest.csv")
SITEMAP_URL = "https://www.thesignalgroup.com/sitemap.xml"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

def get_all_target_urls():
    print(f"[*] Fetching sitemap from {SITEMAP_URL}...")
    urls = set()
    try:
        req = urllib.request.Request(SITEMAP_URL, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=15) as resp:
            content = resp.read().decode("utf-8", errors="ignore")
        root = ET.fromstring(content)
        for elem in root.findall("{http://www.sitemaps.org/schemas/sitemap/0.9}url/{http://www.sitemaps.org/schemas/sitemap/0.9}loc"):
            urls.add(elem.text.strip())
    except Exception as e:
        print(f"[!] Sitemap error: {e}")

    # Also scrape live collection pages to catch newest posts not yet in sitemap
    for live_page in ["https://www.thesignalgroup.com/weekly-market-monitor", "https://www.thesignalgroup.com/newsroom"]:
        try:
            req = urllib.request.Request(live_page, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=15) as resp:
                html = resp.read().decode("utf-8", errors="ignore")
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                h = a["href"].strip()
                if "/weekly-market-monitor/" in h or "/newsroom/" in h:
                    full = "https://www.thesignalgroup.com" + h if h.startswith("/") else h
                    # strip query or anchor
                    clean = full.split("?")[0].split("#")[0]
                    urls.add(clean)
        except Exception as e:
            print(f"[!] Live page scrape error for {live_page}: {e}")

    # Ensure explicitly requested Week 35 2026 is present
    urls.add("https://www.thesignalgroup.com/weekly-market-monitor/weekly-dry-market-monitor-week-35-2026")

    monitors = [u for u in urls if "/weekly-market-monitor/" in u and u != "https://www.thesignalgroup.com/weekly-market-monitor"]
    newsroom = [u for u in urls if "/newsroom/" in u and u != "https://www.thesignalgroup.com/newsroom"]
    
    print(f"[+] Total Targets: {len(monitors)} Weekly Market Monitors and {len(newsroom)} Newsroom Reports.")
    return monitors, newsroom

def parse_and_save(url, section):
    slug = url.rstrip("/").split("/")[-1]
    html_filename = f"{slug}.html"
    md_filename = f"{slug}.md"
    
    html_path = os.path.join(DIR_HTML, html_filename)
    target_dir = DIR_MONITORS if section == "monitors" else DIR_NEWSROOM
    md_path = os.path.join(target_dir, md_filename)
    
    # Check if already harvested
    if os.path.exists(md_path) and os.path.exists(html_path) and os.path.getsize(md_path) > 500:
        return {
            "slug": slug,
            "url": url,
            "section": section,
            "status": "cached",
            "md_path": md_path,
            "html_path": html_path
        }
        
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        return {"slug": slug, "url": url, "section": section, "status": f"error: {e}"}

    # Save raw HTML
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
        
    soup = BeautifulSoup(html, "html.parser")
    
    # Title
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else slug.replace("-", " ").title()
    
    # Date extraction
    date_str = ""
    date_match = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+20\d{2}", html)
    if date_match:
        date_str = date_match.group(0)
        
    # Category extraction
    cat = "Dry Bulk" if "dry" in slug else ("Tankers" if "tanker" in slug else ("Commodity Radar" if "radar" in slug else "Market Intelligence"))
    cd_texts = soup.find_all(class_=re.compile(r"cd_texts", re.I))
    for cd in cd_texts:
        txt = cd.get_text(strip=True)
        if txt and not re.search(r"\d{4}", txt):
            cat = txt
            break

    # Main content body
    body_div = soup.find("div", class_="w-richtext")
    if not body_div:
        body_div = soup.find("div", class_=re.compile(r"article|post-body|content", re.I))
        
    md_lines = [
        f"# {title}\n",
        f"**Date**: {date_str} | **Category**: {cat} | **Section**: {section.title()} | **Source**: [{url}]({url})\n",
        "---\n"
    ]
    
    if body_div:
        for elem in body_div.find_all(["h2", "h3", "h4", "h5", "p", "figure", "table", "ul", "ol"]):
            if elem.name in ["h2", "h3", "h4", "h5"]:
                lvl = "#" * int(elem.name[1])
                md_lines.append(f"\n{lvl} {elem.get_text(strip=True)}\n")
            elif elem.name == "p":
                txt = elem.get_text(strip=True)
                if txt:
                    md_lines.append(f"{txt}\n")
            elif elem.name == "figure":
                img = elem.find("img")
                cap = elem.find("figcaption")
                img_src = img["src"] if img and img.has_attr("src") else ""
                cap_txt = cap.get_text(strip=True) if cap else "Signal Figure"
                if img_src:
                    md_lines.append(f"\n![{cap_txt}]({img_src})\n*{cap_txt}*\n")
            elif elem.name in ["ul", "ol"]:
                for li in elem.find_all("li"):
                    md_lines.append(f"- {li.get_text(strip=True)}")
                md_lines.append("")
            elif elem.name == "table":
                rows = elem.find_all("tr")
                for r_idx, row in enumerate(rows):
                    cols = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
                    if cols:
                        md_lines.append("| " + " | ".join(cols) + " |")
                        if r_idx == 0:
                            md_lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
                md_lines.append("")
                
    full_md = "\n".join(md_lines).strip()
    
    # Stub gate: do not save headline-only shells (e.g. Media Mentions link-outs with no article body)
    body_content = "\n".join(md_lines[3:]).strip()
    if len(body_content) < 200:
        return {
            "slug": slug,
            "url": url,
            "section": section,
            "status": "skipped_media_mention_stub",
            "title": title,
            "date": date_str,
            "category": cat,
            "md_path": None,
            "html_path": html_path
        }

    # Save clean Markdown
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(full_md + "\n")
        
    # Check for direct PDF links to download
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" in href.lower():
            pdf_name = href.split("/")[-1].split("?")[0]
            if not pdf_name.endswith(".pdf"):
                pdf_name += ".pdf"
            pdf_path = os.path.join(DIR_PDFS, pdf_name)
            if not os.path.exists(pdf_path):
                try:
                    pdf_req = urllib.request.Request(href, headers={"User-Agent": USER_AGENT})
                    with urllib.request.urlopen(pdf_req, timeout=15) as p_resp:
                        with open(pdf_path, "wb") as pf:
                            pf.write(p_resp.read())
                except:
                    pass

    return {
        "slug": slug,
        "url": url,
        "section": section,
        "title": title,
        "date": date_str,
        "category": cat,
        "md_path": md_path,
        "html_path": html_path,
        "char_count": len(full_md),
        "status": "downloaded"
    }

def main():
    print("=== STARTING THE SIGNAL GROUP HARVESTER ===")
    monitors, newsroom = get_all_target_urls()
    
    all_targets = [(u, "monitors") for u in monitors] + [(u, "newsroom") for u in newsroom]
    print(f"[*] Total target articles to ingest: {len(all_targets)}")
    
    manifest_records = []
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(parse_and_save, u, sec): (u, sec) for u, sec in all_targets}
        completed = 0
        total = len(futures)
        for future in as_completed(futures):
            res = future.result()
            manifest_records.append(res)
            completed += 1
            if completed % 25 == 0 or completed == total:
                print(f"  [{completed}/{total}] Ingested: {res.get('title', res.get('slug'))[:50]}...")
                
    df_manifest = pd.DataFrame(manifest_records)
    df_manifest.to_csv(MANIFEST_PATH, index=False)
    print(f"\n[+] Successfully generated master manifest: {MANIFEST_PATH}")
    print(f"[+] Total Reports Harvested: {len(df_manifest)}")
    print(f"[+] Monitors: {len(df_manifest[df_manifest['section'] == 'monitors'])}")
    print(f"[+] Newsroom: {len(df_manifest[df_manifest['section'] == 'newsroom'])}")

if __name__ == "__main__":
    main()
