import requests
import re
from urllib.parse import urljoin

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Referer': 'https://shipandbunker.com/',
}

session = requests.Session()
session.headers.update(headers)

# 1. Fetch Singapore page
r = session.get('https://shipandbunker.com/prices/apac/sea/sg-sin-singapore')
print("Singapore page status:", r.status_code, "Length:", len(r.text))

# Find form or API endpoints
print("Looking for json endpoints or ajax calls in page...")
found_endpoints = re.findall(r'["\'](/[a-zA-Z0-9_\-\./]*\.json[a-zA-Z0-9_\-\./\?]*)["\']', r.text)
print("JSON endpoints found in page:", set(found_endpoints))

# Look for all JS files
js_files = re.findall(r'src=["\']([^"\']+\.js[^"\']*)["\']', r.text)
print(f"Found {len(js_files)} JS files:")
for j in js_files:
    print("  JS:", j)
    full_j = urljoin('https://shipandbunker.com', j)
    try:
        rj = session.get(full_j)
        # Search for .json or prices in js
        m = re.findall(r'["\'](/[a-zA-Z0-9_\-\./]*\.json[a-zA-Z0-9_\-\./\?]*)["\']', rj.text)
        if m:
            print("    -> Matched JSON endpoints in JS:", set(m))
        rpc_matches = re.findall(r'(prices[A-Za-z0-9_]+|fn\s*:\s*["\'][^"\']+["\'])', rj.text)
        if rpc_matches:
            print("    -> Matched RPC functions:", set(rpc_matches)[:10])
    except Exception as e:
        pass
