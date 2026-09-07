import re
import json

with open('index.html', 'r', encoding='utf-8', errors='ignore') as f:
    text = f.read()

print('File size of index.html:', len(text), 'chars')

# Check tabs
tabs = re.findall(r'class="[^"]*tab-btn[^"]*"[^>]*data-tab="([^"]+)"', text)
if not tabs:
    tabs = re.findall(r'data-tab="([^"]+)"', text)
print('Tabs in UI:', list(dict.fromkeys(tabs)))

# Check all paths referenced
data_paths = set(re.findall(r'["\'](data/[^"\']+)["\']', text))
print(f'\nData paths referenced in index.html ({len(data_paths)}):')
for p in sorted(data_paths):
    print(' ', p)

# Check knowledge / report paths referenced
rep_paths = set(re.findall(r'["\'](reports/[^"\']+)["\']', text))
print(f'\nReport paths referenced in index.html ({len(rep_paths)}):')
for p in sorted(rep_paths):
    print(' ', p)

# Check knowledge chunks
know_paths = set(re.findall(r'["\'](knowledge/[^"\']+)["\']', text))
print(f'\nKnowledge paths referenced in index.html ({len(know_paths)}):')
for p in sorted(know_paths):
    print(' ', p)
