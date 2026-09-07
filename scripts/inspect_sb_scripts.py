import requests
import re
import json

headers = {'User-Agent': 'Mozilla/5.0'}
r = requests.get('https://shipandbunker.com/prices/apac/sea/sg-sin-singapore', headers=headers)
scripts = re.findall(r'<script[^>]*>(.*?)</script>', r.text, re.DOTALL)
print(f"Total scripts: {len(scripts)}")

for i, s in enumerate(scripts):
    print(f"\n--- Script {i} (Length {len(s)}) ---")
    lines = s.splitlines()
    for l in lines:
        l_str = l.strip()
        if any(term in l_str.lower() for term in ['ajax', 'fetch', 'prices', 'json', 'post', '.php', '/a/']):
            if len(l_str) < 160:
                print(" ", l_str)
