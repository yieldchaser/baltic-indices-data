import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, date, timedelta
import os
import time

try:
    import holidays as _holidays_lib
    _HAS_HOLIDAYS = True
except ImportError:
    _HAS_HOLIDAYS = False
    print("WARNING: 'holidays' library not installed — expiry skipping disabled. Run: pip install holidays")

# Index mapping: URL code → filename
INDEXES = {
    'BDTI': 'dirtytanker_historical.csv',
    'BCTI': 'cleantanker_historical.csv',
    'BCI': 'cape_historical.csv',
    'BPI': 'panama_historical.csv',
    'BSI': 'suprama_historical.csv',
    'BHI': 'handysize_historical.csv',
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

# ── Solactive freight futures index scraper ──────────────────────────────────

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
        existing = pd.read_csv(filename)
        try:
            existing['date'] = pd.to_datetime(existing['date'], format='%d-%m-%Y').dt.normalize()
        except ValueError:
            existing['date'] = pd.to_datetime(existing['date']).dt.normalize()

        if latest_date in existing['date'].values:
            print(f"{filename}: {latest_date.date()} already present, nothing to append")
            return

        combined = pd.concat([existing, latest_row], ignore_index=True)
    else:
        combined = latest_row.copy()

    combined = combined.sort_values('date')
    combined['date'] = combined['date'].dt.strftime('%d-%m-%Y')
    combined.to_csv(filename, index=False)
    print(f"{filename}: Appended {latest_date.date()} -> value {latest_row['value'].iloc[0]}")


def get_last_trading_day():
    """
    Return the most recent weekday (Mon-Fri) as a date object.
    Simple weekday check — if it's Sat/Sun walk back to Friday.
    Does not account for public holidays (Solactive also trades on most holidays).
    """
    d = date.today()
    while d.weekday() >= 5:  # 5=Saturday, 6=Sunday
        d -= timedelta(days=1)
    return d


def fetch_solactive_live(isin):
    """
    Fetch the live/current quote from Solactive's getIndexPerformance endpoint.
    This updates same-day at ~21:30 CET and is used as a fallback when the
    main getDayHistoryChartData batch lags behind by one trading day.

    Returns a one-row DataFrame with columns ['date', 'value'] or empty DataFrame on failure.
    The date is derived from the response timestamp in CET (UTC+1).
    """
    url = "https://www.solactive.com/_actions/getIndexPerformance/"
    payload = {"isin": isin}
    headers = {**SOLACTIVE_HEADERS, "referer": f"https://www.solactive.com/index/{isin}/"}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Response: [schema_dict, isin, timestamp_ms, level, yearHigh, yearLow, diff%, diff%4p]
        # schema_dict maps field names to their index in the data array
        if not isinstance(data, list) or len(data) < 4:
            print(f"  [{isin}] Live: unexpected response structure (len={len(data) if isinstance(data, list) else 'N/A'})")
            return pd.DataFrame()

        schema = data[0]
        if not isinstance(schema, dict) or 'timestamp' not in schema or 'level' not in schema:
            print(f"  [{isin}] Live: schema dict malformed: {schema}")
            return pd.DataFrame()

        timestamp_ms = data[schema['timestamp']]
        level_raw    = data[schema['level']]
        level = float(level_raw)

        # Solactive timestamps are in CET (UTC+1); convert to local CET date.
        from datetime import timezone, timedelta as _td
        cet = timezone(_td(hours=1))
        dt_cet = datetime.fromtimestamp(timestamp_ms / 1000, tz=cet)
        date_normalized = pd.Timestamp(dt_cet.date())

        return pd.DataFrame([{'date': date_normalized, 'value': level}])

    except Exception as e:
        print(f"  [{isin}] Live fetch error: {e}")
        return pd.DataFrame()


def update_solactive_with_fallback(isin, filename):
    """
    Full Solactive update pipeline with live-quote fallback.

    Step 1: try the batch endpoint (getDayHistoryChartData) — same as before.
    Step 2: if the CSV is still behind the last trading day, try the live
            endpoint (getIndexPerformance) which is published same-day ~21:30 CET.

    Safeguards before appending the live value:
      1. live value must be a valid positive float
      2. live date must be strictly after the latest batch date (no going backwards)
      3. live date must exactly equal the last trading day (expected date)
      4. live date must not already exist in the CSV
    """
    # ── Step 1: normal batch update ──────────────────────────────────────────
    print(f"  Fetching batch (getDayHistoryChartData)...")
    batch_row = fetch_latest_solactive(isin)
    update_solactive_csv(filename, batch_row)

    # ── Step 2: check if we still need the live fallback ────────────────────
    if not os.path.exists(filename):
        print(f"  {filename}: file missing after batch step, skipping live fallback")
        return

    existing = pd.read_csv(filename)
    try:
        existing['date'] = pd.to_datetime(existing['date'], format='%d-%m-%Y').dt.normalize()
    except Exception:
        existing['date'] = pd.to_datetime(existing['date']).dt.normalize()

    if existing.empty:
        return

    latest_in_csv = existing['date'].max()
    last_td = pd.Timestamp(get_last_trading_day())

    if latest_in_csv >= last_td:
        print(f"  {filename}: already up to date ({latest_in_csv.date()})")
        return

    # Batch is behind — try the live endpoint
    print(f"  {filename}: batch lags ({latest_in_csv.date()} < {last_td.date()}), fetching live quote...")
    live_row = fetch_solactive_live(isin)

    if live_row.empty:
        print(f"  {filename}: live fetch returned nothing — keeping batch result")
        return

    live_date = live_row['date'].iloc[0]
    live_val  = live_row['value'].iloc[0]

    # ── Safeguard 1: value must be a valid positive number ───────────────────
    if not isinstance(live_val, (int, float)) or live_val <= 0 or pd.isna(live_val):
        print(f"  {filename}: SKIP live — value {live_val!r} is not a valid positive number")
        return

    # ── Safeguard 2: live date must be strictly after the batch latest ───────
    if live_date <= latest_in_csv:
        print(f"  {filename}: SKIP live — live date {live_date.date()} is not after batch latest {latest_in_csv.date()}")
        return

    # ── Safeguard 3: live date must equal the expected last trading day ──────
    if live_date != last_td:
        print(f"  {filename}: SKIP live — live date {live_date.date()} != expected {last_td.date()} (holiday or weekend?)")
        return

    # ── Safeguard 4: live date must not already be in the CSV ────────────────
    if live_date in existing['date'].values:
        print(f"  {filename}: SKIP live — {live_date.date()} already in CSV")
        return

    # All checks passed — append the live row
    new_row = pd.DataFrame([{'date': live_date.strftime('%d-%m-%Y'), 'value': live_val}])
    existing_str = existing.copy()
    existing_str['date'] = existing_str['date'].dt.strftime('%d-%m-%Y')
    combined = pd.concat([existing_str, new_row], ignore_index=True)
    combined.to_csv(filename, index=False)
    print(f"  {filename}: live fallback OK — appended {live_date.date()} -> {live_val}")

# ── NEW: Amplify ETF premium/discount scraper ─────────────────────────────────

AMPLIFY_ETFS = {
    'BDRY': 'BDRY_Daily.csv',
    'BWET': 'BWET_Daily.csv',
}

AMPLIFY_URL = "https://amplifyetfs.com/wp-content/uploads/feeds/AmplifyWeb.40XL.XL_{ticker}_Daily.csv"

def fetch_latest_amplify(ticker):
    """Download Amplify ETF CSV and return full history as DataFrame"""
    url = AMPLIFY_URL.format(ticker=ticker)
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        from io import StringIO
        df = pd.read_csv(StringIO(response.text))
        # Amplify renamed the column — normalise to the name used in our CSVs
        if 'Premium/Discount Percentage' in df.columns and 'Premium/Discount' not in df.columns:
            df = df.rename(columns={'Premium/Discount Percentage': 'Premium/Discount'})
        df['Rate Date'] = pd.to_datetime(df['Rate Date'], format='%m/%d/%Y')
        df = df.sort_values('Rate Date')

        return df

    except Exception as e:
        print(f"Error fetching Amplify {ticker}: {e}")
        return pd.DataFrame()

def update_amplify_csv(filename, new_data):
    """Merge full Amplify history with existing CSV, backfilling any missing dates"""
    if new_data.empty:
        print(f"{filename}: No data returned, skipping")
        return

    new_data = new_data[['Rate Date', 'Premium/Discount']].copy()

    if os.path.exists(filename):
        existing = pd.read_csv(filename)
        existing['Rate Date'] = pd.to_datetime(existing['Rate Date'], format='%d-%m-%Y')
        combined = pd.concat([existing, new_data], ignore_index=True)
    else:
        combined = new_data

    combined = combined.drop_duplicates(subset=['Rate Date'], keep='last')
    combined = combined.sort_values('Rate Date')
    before = len(existing) if os.path.exists(filename) else 0
    added = len(combined) - before
    combined['Rate Date'] = combined['Rate Date'].dt.strftime('%d-%m-%Y')
    combined.to_csv(filename, index=False)
    print(f"{filename}: {added} new rows added, {len(combined)} total")

# ── SGX FFA FUTURES ───────────────────────────────────────────────────────────

def get_expiry(month, year):
    """
    Return the last UK business day for a given month/year.
    For December: last UK business day on or before Dec 24
    (Baltic Exchange stops publishing Dec 25-31).
    Returns a datetime.date object.
    """
    if month == 12:
        d = date(year, 12, 24)
    else:
        # Last calendar day of month
        if month == 12:
            d = date(year, 12, 31)
        else:
            d = date(year, month + 1, 1) - timedelta(days=1)

    # Walk back to last UK business day
    if _HAS_HOLIDAYS:
        uk_hols = _holidays_lib.UnitedKingdom(years=year)
        while d.weekday() >= 5 or d in uk_hols:
            d -= timedelta(days=1)
    else:
        # Fallback: weekdays only (no holiday check)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
    return d


SGX_PRODUCTS = {
    'CWF': 'sgx_cape_futures.csv',
    'PWF': 'sgx_panamax_futures.csv',
    'SWF': 'sgx_supramax_futures.csv',
    'HWF': 'sgx_handysize_futures.csv',
}

# CME month codes → (month_index 1-12, name)
CME_MONTHS = {
    'F': (1,  'Jan'), 'G': (2,  'Feb'), 'H': (3,  'Mar'),
    'J': (4,  'Apr'), 'K': (5,  'May'), 'M': (6,  'Jun'),
    'N': (7,  'Jul'), 'Q': (8,  'Aug'), 'U': (9,  'Sep'),
    'V': (10, 'Oct'), 'X': (11, 'Nov'), 'Z': (12, 'Dec'),
}

SGX_HISTORY_URL = (
    "https://api.sgx.com/derivatives/v1.0/history/symbol/{ticker}"
    "?days=5d&category=futures"
    "&params=base-date%2Ctotal-volume%2Cdaily-settlement-price-abs"
)

SGX_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://www.sgx.com/',
    'Origin': 'https://www.sgx.com',
}


def generate_sgx_tickers(product_code):
    """
    Generate all possible tickers from current month to Dec 2032.
    e.g. CWF + J + 26 = CWFJ26 (Capesize Apr 2026)
    Returns list of (ticker, month_num, year, month_name, expiry_date_str)
    """
    now = datetime.now()
    today = date.today()
    tickers = []
    for year in range(now.year, 2033):
        year2 = str(year)[-2:]
        for code, (month_num, month_name) in CME_MONTHS.items():
            if year == now.year and month_num < now.month:
                continue
            expiry = get_expiry(month_num, year)
            expiry_str = expiry.strftime('%d-%m-%Y')
            tickers.append((f"{product_code}{code}{year2}", month_num, year, month_name, expiry_str, expiry))
    return tickers


def fetch_sgx_latest(ticker):
    """
    Fetch last 5 days for ticker. Returns list of (date DD-MM-YYYY, price, volume)
    for all available days, or None if contract has no data.
    """
    url = SGX_HISTORY_URL.format(ticker=ticker)
    try:
        r = requests.get(url, headers=SGX_HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json().get('data', [])
        if not data:
            return None
        rows = []
        for entry in data:
            price     = entry.get('daily-settlement-price-abs')
            volume    = entry.get('total-volume')
            base_date = entry.get('base-date')  # "20260303"
            if price is None or base_date is None:
                continue
            d = datetime.strptime(str(base_date), '%Y%m%d')
            rows.append((d.strftime('%d-%m-%Y'), float(price), float(volume or 0)))
        return rows if rows else None
    except Exception as e:
        print(f"  {ticker}: error — {e}")
        return None


def update_sgx_csv(filename, product_code):
    """
    Fetch latest price for all active contracts and append new rows to CSV.
    CSV schema: contract, expiry_month, expiry_year, date, price, volume, expiry_date
    A contract is 'active' if today <= expiry date AND the API returns data for it.
    """
    today = date.today()

    if os.path.exists(filename):
        existing = pd.read_csv(filename)
        existing['date'] = pd.to_datetime(existing['date'], format='%d-%m-%Y')
        # Back-fill expiry_date column if it was added after initial creation
        if 'expiry_date' not in existing.columns:
            existing['expiry_date'] = ''
    else:
        existing = pd.DataFrame(columns=['contract','expiry_month','expiry_year','date','price','volume','expiry_date'])

    tickers = generate_sgx_tickers(product_code)
    new_rows = []
    active_count = 0
    skipped_expired = 0

    for ticker, month_num, year, month_name, expiry_str, expiry_date in tickers:
        # Skip contracts that have already expired
        if today > expiry_date:
            skipped_expired += 1
            continue

        results = fetch_sgx_latest(ticker)
        if results is None:
            continue  # not active — skip silently

        active_count += 1
        for date_str, price, volume in results:
            date_dt = pd.Timestamp(datetime.strptime(date_str, '%d-%m-%Y'))

            already = (
                not existing.empty and
                ((existing['contract'] == ticker) & (existing['date'] == date_dt)).any()
            )
            if already:
                continue

            new_rows.append({
                'contract':     ticker,
                'expiry_month': f"{month_name} {year}",
                'expiry_year':  year,
                'date':         date_str,
                'price':        price,
                'volume':       volume,
                'expiry_date':  expiry_str,
            })
        print(f"  {ticker} ({month_name} {year}, exp {expiry_str}): {price}  vol={volume}")
        time.sleep(0.15)  # polite delay between calls

    if skipped_expired:
        print(f"  Skipped {skipped_expired} already-expired contracts")

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        if existing.empty:
            combined = new_df
        else:
            existing['date'] = existing['date'].dt.strftime('%d-%m-%Y')
            combined = pd.concat([existing, new_df], ignore_index=True)
        combined.to_csv(filename, index=False)
        print(f"{filename}: +{len(new_rows)} new rows ({active_count} active contracts)")
    else:
        print(f"{filename}: nothing new ({active_count} active contracts checked)")


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

    # New: Solactive freight futures indexes (with live-quote fallback for same-day data)
    print("\n--- Solactive Freight Futures ---")
    for isin, filename in SOLACTIVE_INDEXES.items():
        print(f"\nProcessing {isin}...")
        update_solactive_with_fallback(isin, filename)

    # New: Amplify ETF premium/discount
    print("\n--- Amplify ETF Premium/Discount ---")
    for ticker, filename in AMPLIFY_ETFS.items():
        print(f"\nProcessing {ticker}...")
        latest = fetch_latest_amplify(ticker)
        update_amplify_csv(filename, latest)

    # New: SGX FFA Futures (Capesize, Panamax, Supramax)
    print("\n--- SGX FFA Futures ---")
    for product_code, filename in SGX_PRODUCTS.items():
        print(f"\nProcessing {product_code}...")
        update_sgx_csv(filename, product_code)

    print(f"\nCompleted at {datetime.now()}")

if __name__ == "__main__":
    main()
