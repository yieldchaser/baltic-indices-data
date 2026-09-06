import os
import sys
import glob
import json
import csv
import time
import subprocess
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(BASE_DIR, "knowledge", "assets", "drewry_opinions")
LINKS_PATH = os.path.join(OUT_DIR, "_all_discovered_links.json")
MANIFEST_PATH = os.path.join(OUT_DIR, "_manifest.csv")

def scrape_with_monid(url):
    q = json.dumps({"url": url})
    escaped_q = q.replace('"', '\\"')
    cmd = f'monid run -p context.dev -e /web/scrape/html --query "{escaped_q}" --wait -j'
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding='utf-8', errors='replace')
    
    stdout = res.stdout
    start = stdout.find('{')
    end = stdout.rfind('}') + 1
    if start == -1 or end <= start:
        return None, "FAILED: invalid JSON response from monid"
    
    try:
        data = json.loads(stdout[start:end])
        html = data.get('output', {}).get('html', '')
        if not html:
            return None, "FAILED: empty html"
        return html, "OK"
    except Exception as e:
        return None, f"FAILED: JSON parse error {e}"

def parse_html(url, html, card_title="", card_date=""):
    soup = BeautifulSoup(html, "html.parser")
    
    # Title
    title = card_title
    if not title:
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True).lower() not in ["login", "news", "sectors"]:
            title = h1.get_text(strip=True)
        else:
            og = soup.find("meta", property="og:title")
            title = og["content"] if og and og.get("content") else "Drewry Article"
            
    # Date
    date_span = soup.find("span", class_="aos-ArticleDate")
    date = date_span.get_text(strip=True) if date_span else card_date
    
    # Body
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
        
    return {
        "url": url,
        "title": title,
        "date": date,
        "body": "\n\n".join(paragraphs),
        "paragraph_count": len(paragraphs)
    }, "OK"

def main():
    existing_files = set(os.path.basename(f) for f in glob.glob(os.path.join(OUT_DIR, "*.md")))
    with open(LINKS_PATH, "r", encoding="utf-8") as f:
        all_links = json.load(f)
        
    target_items = []
    for it in all_links:
        slug = it['url'].rstrip('/').split('/')[-1]
        filename = f"{slug}.md"
        if filename not in existing_files and "maritime-research-opinion" in it['url']:
            target_items.append(it)
            
    print(f"Total target maritime opinions to scrape via Monid: {len(target_items)}")
    if not target_items:
        print("Nothing to scrape!")
        return

    manifest_fields = ["url", "title", "date", "paragraph_count"]
    manifest_file = open(MANIFEST_PATH, "a", newline="", encoding="utf-8")
    manifest_writer = csv.DictWriter(manifest_file, fieldnames=manifest_fields)

    success_count = 0
    fail_count = 0

    for i, it in enumerate(target_items, start=1):
        url = it['url']
        slug = url.rstrip('/').split('/')[-1]
        out_path = os.path.join(OUT_DIR, f"{slug}.md")
        card_title = it.get('title', '')
        card_date = it.get('date', '')
        
        print(f"[{i}/{len(target_items)}] Fetching: {slug}...")
        html, status = scrape_with_monid(url)
        if not html:
            print(f"  FAILED: {status}")
            fail_count += 1
            continue
            
        art, parse_status = parse_html(url, html, card_title=card_title, card_date=card_date)
        if not art:
            print(f"  PARSE FAILED: {parse_status}")
            fail_count += 1
            continue
            
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(f"# {art['title']}\n\n")
            if art["date"]:
                f.write(f"*{art['date']}*\n\n")
            f.write(art["body"])
            
        manifest_writer.writerow({k: art[k] for k in manifest_fields})
        manifest_file.flush()
        success_count += 1
        print(f"  OK -> {slug}.md ({art['paragraph_count']} paragraphs)")
        time.sleep(1.0)
        
    manifest_file.close()
    print("\n==========================================")
    print(f"Monid Scrape Run Completed:")
    print(f"  Successfully scraped & saved: {success_count}")
    print(f"  Failed: {fail_count}")
    print(f"  Total .md files now on disk: {len(glob.glob(os.path.join(OUT_DIR, '*.md')))}")
    print("==========================================\n")

if __name__ == "__main__":
    main()
