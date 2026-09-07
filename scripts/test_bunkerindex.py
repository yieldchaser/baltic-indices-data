import requests
import re
import bs4
import json

headers = {'User-Agent': 'Mozilla/5.0'}

# 1. Indicator 2
r2 = requests.get('https://www.bunkerindex.com/indicators/indicator.php?i=2', headers=headers)
series = re.findall(r'series\s*:\s*\[\s*\{\s*name\s*:\s*[\'"][^\'"]+[\'"],\s*data\s*:\s*(\[\[[\s\S]*?\]\])', r2.text)
if series:
    print('Indicator 2 Highcharts series data extracted! Length:', len(series[0]))
    try:
        pts = json.loads(series[0])
        print(f'Total volume points parsed: {len(pts)}')
        print('Earliest point:', pts[0])
        print('Latest point:', pts[-1])
    except Exception as e:
        print('JSON parse error:', e)
else:
    print('Checking table for indicator 2...')
    soup = bs4.BeautifulSoup(r2.text, 'html.parser')
    table = soup.find('table', id='datatables_indicator_unlogged')
    if table:
        rows = table.find_all('tr')
        print('Indicator 2 table rows:', len(rows))
        for row in rows[:4]:
            print([td.get_text(strip=True) for td in row.find_all(['th', 'td'])])

# 2. BIX World
r3 = requests.get('https://www.bunkerindex.com/indices/world.php', headers=headers)
soup3 = bs4.BeautifulSoup(r3.text, 'html.parser')
tables3 = soup3.find_all('table')
print(f'\nTotal tables in BIX World: {len(tables3)}')
for i, t in enumerate(tables3[:3]):
    rows = t.find_all('tr')
    print(f'Table {i} rows: {len(rows)}')
    if rows:
        print('  Header:', [th.get_text(strip=True) for th in rows[0].find_all(['th', 'td'])])
        if len(rows) > 1:
            print('  Row 1:', [td.get_text(strip=True) for td in rows[1].find_all(['th', 'td'])])
