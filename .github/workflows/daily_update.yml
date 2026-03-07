import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import time

# Index mapping: URL code → filename
INDEXES = {
    'BDTI': 'dirtytanker_historical.csv',
    'BCTI': 'cleantanker_historical.csv',
    'BCI': 'cape_historical.csv',
    'BPI': 'panama_historical.csv',
    'BSI': 'suprama_historical.csv',
    'BDI': 'bdiy_historical.csv'
}

BASE_URL = "https://en.stockq.org/index/{}.php"  # Fixed: removed space

def scrape_index(code):
    """Scrape all data from stockq.org for one index"""
    url = BASE_URL.format(code)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        tables = soup.find_all('table')

        data = []
        for table in tables:
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header
                cols = row.find_all('td')
                if len(cols) >= 3:
                    date_text = cols[0].text.strip()
                    index_text = cols[1].text.strip().replace(',', '')
                    change_text = cols[2].text.strip()

                    try:
                        date = pd.to_datetime(date_text, format='%Y/%m/%d')
                        index_val = float(index_text)
                        if index_val <= 0:
                            continue  # skip zero/negative — sanity check
                        data.append({
                            'Date': date.strftime('%d-%m-%Y'),
                            'Index': index_val,
                            '% Change': change_text
                        })
                    except (ValueError, TypeError):
                        continue

        return pd.DataFrame(data)

    except Exception as e:
        print(f"Error scraping {code}: {e}")
        return pd.DataFrame()

def update_csv(filename, new_data):
    """Merge new data with existing CSV, deduplicate, sort correctly"""
    filepath = filename

    new_data = new_data.copy()
    new_data['Date_parsed'] = pd.to_datetime(new_data['Date'], format='%d-%m-%Y')

    if os.path.exists(filepath):
        existing = pd.read_csv(filepath)
        try:
            existing['Date_parsed'] = pd.to_datetime(existing['Date'], format='%d-%m-%Y')
        except Exception:
            existing['Date_parsed'] = pd.to_datetime(existing['Date'], dayfirst=True)

        combined = pd.concat([existing, new_data])
        combined = combined.drop_duplicates(subset=['Date_parsed'], keep='last')
    else:
        combined = new_data.copy()

    # Always sort by the parsed date column (correct chronological order)
    combined = combined.sort_values('Date_parsed')
    combined = combined.drop(columns=['Date_parsed'])

    combined.to_csv(filepath, index=False)
    print(f"{filename}: {len(new_data)} new rows, {len(combined)} total rows")

# ── NEW: Solactive freight futures index scraper ─────────────────────────────

SOLACTIVE_INDEXES = {
    'DE000SLA4BY3': 'bdryff_history.csv',  # Breakwave Dry Freight Futures
    'DE000SL0HLG3': 'bwetff_history.csv',  # Breakwave Wet Freight Futures
}

SOLACTIVE_HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "origin": "https://www.solactive.com",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
}

def fetch_latest_solactive(isin):
    """Fetch the latest row from Solactive API and return as a one-row DataFrame"""
    url = "https://www.solactive.com/_actions/getDayHistoryChartData/"
    payload = {
        "isin": isin,
        "indexCreatingTimeStamp": 0,
        "dayDate": int(time.time() * 1000)
    }
    headers = {**SOLACTIVE_HEADERS, "referer": f"https://www.solactive.com/index/{isin}/"}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Decode compressed format: data[0] holds pointer array, each points to schema+values
        records = []
        for p in data[0]:
            schema = data[p]
            timestamp = data[schema['timestamp']]
            value = data[schema['value']]
            records.append({'timestamp_ms': timestamp, 'value': value})

        df = pd.DataFrame(records)
        df['date'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.normalize()
        df = df[['date', 'value']].sort_values('date')

        # Return only the latest row
        return df.tail(1)

    except Exception as e:
        print(f"Error fetching Solactive {isin}: {e}")
        return pd.DataFrame()

def update_solactive_csv(filename, latest_row):
    """Append latest Solactive row only if date not already present"""
    if latest_row.empty:
        print(f"{filename}: No data returned, skipping")
        return

    latest_date = latest_row['date'].iloc[0]

    if os.path.exists(filename):
        existing = pd.read_csv(filename, parse_dates=['date'])
        existing['date'] = pd.to_datetime(existing['date']).dt.normalize()

        if latest_date in existing['date'].values:
            print(f"{filename}: {latest_date.date()} already present, nothing to append")
            return

        combined = pd.concat([existing, latest_row], ignore_index=True)
    else:
        combined = latest_row.copy()

    combined = combined.sort_values('date')
    combined.to_csv(filename, index=False)
    print(f"{filename}: Appended {latest_date.date()} → value {latest_row['value'].iloc[0]}")

# ── NEW: Amplify ETF premium/discount scraper ─────────────────────────────────

AMPLIFY_ETFS = {
    'BDRY': 'BDRY_Daily.csv',
    'BWET': 'BWET_Daily.csv',
}

AMPLIFY_URL = "https://amplifyetfs.com/wp-content/uploads/feeds/AmplifyWeb.40XL.XL_{ticker}_Daily.csv"

def fetch_latest_amplify(ticker):
    """Download Amplify ETF CSV and return only the latest row"""
    url = AMPLIFY_URL.format(ticker=ticker)
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        from io import StringIO
        df = pd.read_csv(StringIO(response.text))
        df['Rate Date'] = pd.to_datetime(df['Rate Date'], format='%m/%d/%Y')
        df = df.sort_values('Rate Date')

        return df.tail(1)

    except Exception as e:
        print(f"Error fetching Amplify {ticker}: {e}")
        return pd.DataFrame()

def update_amplify_csv(filename, latest_row):
    """Append latest Amplify row only if date not already present"""
    if latest_row.empty:
        print(f"{filename}: No data returned, skipping")
        return

    latest_date = latest_row['Rate Date'].iloc[0]  # already a datetime from fetch_latest_amplify

    if os.path.exists(filename):
        existing = pd.read_csv(filename)
        # Existing CSV uses DD-MM-YYYY — must specify format explicitly
        existing['Rate Date'] = pd.to_datetime(existing['Rate Date'], format='%d-%m-%Y')

        if latest_date in existing['Rate Date'].values:
            print(f"{filename}: {latest_date.date()} already present, nothing to append")
            return

        combined = pd.concat([existing, latest_row[['Rate Date', 'Premium/Discount']]], ignore_index=True)
    else:
        combined = latest_row[['Rate Date', 'Premium/Discount']].copy()

    combined = combined.sort_values('Rate Date')
    # Write dates as DD-MM-YYYY to stay consistent with dashboard parser
    combined['Rate Date'] = combined['Rate Date'].dt.strftime('%d-%m-%Y')
    combined.to_csv(filename, index=False)
    print(f"{filename}: Appended {latest_date.date()} → P/D {latest_row['Premium/Discount'].iloc[0]}")

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"Starting update at {datetime.now()}")

    # Existing: Baltic shipping indexes
    for code, filename in INDEXES.items():
        print(f"\nProcessing {code}...")
        new_data = scrape_index(code)

        if not new_data.empty:
            update_csv(filename, new_data)
        else:
            print(f"No data scraped for {code}")

    # New: Solactive freight futures indexes
    print("\n── Solactive Freight Futures ──")
    for isin, filename in SOLACTIVE_INDEXES.items():
        print(f"\nProcessing {isin}...")
        latest = fetch_latest_solactive(isin)
        update_solactive_csv(filename, latest)

    # New: Amplify ETF premium/discount
    print("\n── Amplify ETF Premium/Discount ──")
    for ticker, filename in AMPLIFY_ETFS.items():
        print(f"\nProcessing {ticker}...")
        latest = fetch_latest_amplify(ticker)
        update_amplify_csv(filename, latest)

    print(f"\nCompleted at {datetime.now()}")

if __name__ == "__main__":
    main()
