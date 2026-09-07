"""
download_shipbroker_pdfs.py
High-speed production downloader for the 3,427 Hellenic Weekly Shipbrokers Reports.
Downloads direct static PDFs from wp-content/uploads/ bypassing all site ads and overlays.
Organizes into:
  reports/shipbrokers/<broker_slug>/<year>/<filename>.pdf
Updates reports/shipbrokers/shipbrokers_manifest.csv with local paths and statuses.
"""

import os
import re
import time
import urllib.request
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

BASE_DIR = "reports/shipbrokers"
MANIFEST_CSV = os.path.join(BASE_DIR, "shipbrokers_manifest.csv")
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

BROKER_SLUGS = {
    "Affinity Research": "affinity",
    "Banchero Costa": "banchero_costa",
    "Advanced Shipping & Trading": "advanced_shipping",
    "Allied Shipbroking": "allied",
    "Intermodal Shipbrokers": "intermodal",
    "Xclusiv Shipbrokers": "xclusiv",
    "Gibson Shipbrokers": "gibson",
    "Clarksons Hellas": "clarksons",
    "Lion Shipbrokers": "lion",
    "Agora Shipbroking": "agora",
    "Carriers Chartering": "carriers",
    "Optima Shipbrokers": "optima",
    "Intership": "intership",
    "Star Asia Shipbroking": "star_asia",
    "Cotzias Intermodal": "cotzias",
    "WeberSeas": "weberseas",
    "Golden Destiny": "golden_destiny",
    "Anchor Shipbroking": "anchor",
    "Alibra Shipping": "alibra",
    "Fearnleys": "fearnleys",
    "Simpson Spence Young (SSY)": "ssy",
    "ISM (Intership Navigation)": "ism",
    "Other Broker": "other"
}

def clean_filename(broker_slug, year, week, original_url):
    raw_name = urlparse(original_url).path.split("/")[-1]
    raw_name = urllib.parse.unquote(raw_name)
    sanitized = re.sub(r'[\\/*?:"<>| ]', '_', raw_name)
    if not sanitized.lower().endswith(".pdf"):
        sanitized += ".pdf"
    wk_str = f"_{week}" if (week and str(week).lower() not in ['nan', 'none', 'n/a']) else ""
    return f"{broker_slug}_{year}{wk_str}_{sanitized}"

def download_one(idx, total, row):
    url = str(row["pdf_url"]).strip()
    broker = row["broker"]
    year = str(row["year"])
    week = str(row["week"])
    post_id = row["post_id"]
    
    if not url or not url.startswith("http"):
        return post_id, "", 0, "no_pdf"
        
    broker_slug = BROKER_SLUGS.get(broker, "other")
    target_dir = os.path.join(BASE_DIR, broker_slug, year)
    os.makedirs(target_dir, exist_ok=True)
    
    fname = clean_filename(broker_slug, year, week, url)
    local_path = os.path.join(target_dir, fname)
    
    # Check if already downloaded and valid
    if os.path.exists(local_path) and os.path.getsize(local_path) > 1024:
        return post_id, local_path, os.path.getsize(local_path), "cached"
        
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    retries = 3
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = resp.read()
                # Verify valid PDF header
                if data[:4] == b"%PDF":
                    with open(local_path, "wb") as f:
                        f.write(data)
                    return post_id, local_path, len(data), "downloaded"
                else:
                    return post_id, "", 0, "invalid_pdf_header"
        except Exception as e:
            if attempt == retries - 1:
                return post_id, "", 0, f"error: {e}"
            time.sleep(1.0)
            
    return post_id, "", 0, "failed"

def run_download(batch_size=None, workers=8):
    if not os.path.exists(MANIFEST_CSV):
        print(f"[!] Manifest not found at {MANIFEST_CSV}")
        return
        
    df = pd.read_csv(MANIFEST_CSV)
    has_pdf_df = df[df["has_pdf"] == True].copy()
    total_target = len(has_pdf_df)
    print(f"[*] Total target reports with PDFs: {total_target}")
    
    if batch_size:
        target_df = has_pdf_df.head(batch_size)
        print(f"[*] Processing batch of {batch_size} reports...")
    else:
        target_df = has_pdf_df
        print(f"[*] Processing all {total_target} reports...")

    results = {}
    completed = 0
    new_dl = 0
    cached = 0
    errs = 0
    total_bytes = 0
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(download_one, i, len(target_df), row): row["post_id"] 
                   for i, (_, row) in enumerate(target_df.iterrows())}
                   
        for fut in as_completed(futures):
            p_id, path, sz, status = fut.result()
            results[p_id] = (path, sz, status)
            completed += 1
            total_bytes += sz
            
            if status == "downloaded":
                new_dl += 1
            elif status == "cached":
                cached += 1
            else:
                errs += 1
                
            if completed % 50 == 0 or completed == len(target_df):
                elapsed = time.time() - start_time
                rate = round(completed / elapsed, 1) if elapsed > 0 else 0
                mb = round(total_bytes / (1024 * 1024), 1)
                print(f"  [{completed}/{len(target_df)}] {rate} reports/sec | {mb} MB | ({new_dl} new, {cached} cached, {errs} errs)")
                
    # Update dataframe
    path_map = {pid: v[0] for pid, v in results.items()}
    size_map = {pid: v[1] for pid, v in results.items()}
    status_map = {pid: v[2] for pid, v in results.items()}
    
    df["local_path"] = df["post_id"].map(path_map).fillna(df.get("local_path", ""))
    df["file_size_bytes"] = df["post_id"].map(size_map).fillna(df.get("file_size_bytes", 0))
    df["download_status"] = df["post_id"].map(status_map).fillna(df.get("download_status", "pending"))
    
    df.to_csv(MANIFEST_CSV, index=False)
    print(f"\n[+] Manifest updated with local download paths at {MANIFEST_CSV}")
    print(f"[+] Run complete: {new_dl} downloaded, {cached} cached, {errs} errors. Total Size: {round(total_bytes/(1024*1024), 2)} MB.")

if __name__ == "__main__":
    import sys
    batch = int(sys.argv[1]) if len(sys.argv) > 1 else None
    run_download(batch_size=batch, workers=8)
