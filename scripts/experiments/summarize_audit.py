import json

with open('scratch_full_data_audit.json', 'r') as f:
    data = json.load(f)

datasets = data['datasets']
not_in_index = [d for d in datasets if not d['in_index']]
in_index = [d for d in datasets if d['in_index']]

print(f"Total Datasets: {len(datasets)}")
print(f"Datasets Referenced in index.html: {len(in_index)}")
print(f"Datasets NOT Referenced in index.html: {len(not_in_index)}")

by_cat = {}
for d in not_in_index:
    p = d['path']
    cat = 'Other'
    if 'bunker' in p:
        cat = 'Bunkers (Ship & Bunker / Bunker Index)'
    elif 'fearn' in p:
        cat = 'Fearnleys / Hasura GraphQL'
    elif 'seabroker' in p:
        cat = 'Seabrokers Offshore OSV & Rigs'
    elif 'capital_link' in p:
        cat = 'Capital Link Equity Indices'
    elif 'sgx' in p or 'iron_ore' in p:
        cat = 'SGX Iron Ore & Freight Derivatives'
    elif 'drewry' in p:
        cat = 'Drewry Container & Maritime Opinions'
    elif 'commodit' in p:
        cat = 'Commodities & Upstream Physical Flows'
    elif 'congestion' in p or 'port' in p:
        cat = 'Port Congestion & Activity'
    elif 'etf' in p or 'cftc' in p:
        cat = 'ETF & CFTC Institutional Positioning'
    elif 'derived' in p:
        cat = 'Derived Analytics & Matrices'
    by_cat.setdefault(cat, []).append(d)

print('\n=== BREAKDOWN OF UNRENDERED DATASETS BY CATEGORY ===')
for cat, items in sorted(by_cat.items()):
    total_mb = sum(x['size_bytes'] for x in items) / (1024 * 1024)
    total_rows = sum(x['rows'] or 0 for x in items if x['rows'] and x['rows'] > 0)
    print(f"\n[{cat}] - {len(items)} files | {total_mb:.2f} MB | {total_rows:,} rows")
    for item in sorted(items, key=lambda x: x['size_bytes'], reverse=True)[:8]:
        r_str = f"{item.get('rows')} rows" if item.get('rows') is not None else "N/A"
        d_str = f"{item.get('date_start')} to {item.get('date_end')}"
        print(f"    - {item['path']} ({r_str}, {d_str})")

print('\n=== REPORTS DIRECTORY SUMMARY ===')
for k, v in data['reports_summary'].items():
    print(f"  - {k}: {v['count']} files ({v['total_mb']:.2f} MB)")

print(f"\nAcademic Books in reports/: {len(data['pdf_books'])}")
for b in data['pdf_books']:
    print(f"  - {b}")
