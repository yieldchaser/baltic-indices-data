import requests
import pandas as pd
from datetime import datetime
import os
import re

# Direct CSV feed URL (contains all Amplify ETF holdings)
CSV_URL = 'https://amplifyetfs.com/wp-content/uploads/feeds/AmplifyWeb.40XL.XL_Holdings.csv'

# ETFs we want to extract
TARGET_ETFS = {
    'BDRY': 'bdry_holdings.csv',
    'BWET': 'bwet_holdings.csv'
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/csv,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

# Category priority order (lower number = higher priority)
CATEGORY_ORDER = {
    'capesize': 1,
    'panamax': 2,
    'supramax': 3,
    'td3c': 3.5,      # For BWET - TD3C route
    'td20': 3.6,      # For BWET - TD20 route
    'cash': 4,
    'invesco': 5,
    'other': 99
}

# Month mapping for sorting
MONTH_MAP = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}

def categorize_holding(name, etf_code):
    """Determine category priority based on holding name"""
    name_lower = name.lower()
    
    # For BWET (tanker routes)
    if etf_code == 'BWET':
        if 'td3c' in name_lower or 'middle east gulf to china' in name_lower:
            return 'td3c', CATEGORY_ORDER['td3c']
        elif 'td20' in name_lower or 'west africa to continent' in name_lower:
            return 'td20', CATEGORY_ORDER['td20']
        elif 'cash' in name_lower:
            return 'cash', CATEGORY_ORDER['cash']
        elif 'invesco' in name_lower:
            return 'invesco', CATEGORY_ORDER['invesco']
        else:
            return 'other', CATEGORY_ORDER['other']
    
    # For BDRY (dry bulk ship sizes)
    if 'capesize' in name_lower:
        return 'capesize', CATEGORY_ORDER['capesize']
    elif 'panamax' in name_lower:
        return 'panamax', CATEGORY_ORDER['panamax']
    elif 'supramax' in name_lower:
        return 'supramax', CATEGORY_ORDER['supramax']
    elif 'cash' in name_lower:
        return 'cash', CATEGORY_ORDER['cash']
    elif 'invesco' in name_lower:
        return 'invesco', CATEGORY_ORDER['invesco']
    else:
        return 'other', CATEGORY_ORDER['other']

def extract_month_year(name):
    """
    Extract month and year from holding name
    Returns: (month_num, year) or (99, 9999) for non-dated items
    """
    name_lower = name.lower()
    
    # Look for month abbreviation + year pattern (e.g., "Mar 26", "Feb 2026", "M Mar 26")
    month_pattern = r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[\s\-]?(\d{2,4})'
    match = re.search(month_pattern, name_lower)
    
    if match:
        month_abbr = match.group(1)
        year_str = match.group(2)
        
        month_num = MONTH_MAP.get(month_abbr, 99)
        
        # Handle 2-digit vs 4-digit year
        if len(year_str) == 2:
            year = 2000 + int(year_str)
        else:
            year = int(year_str)
            
        return month_num, year
    
    # For non-dated items (Cash, Invesco, etc.), return high values so they sort last
    return 99, 9999

def sort_holdings(df, etf_code):
    """
    Sort holdings by:
    1. Category priority (Capesize → Panamax → Supramax → Cash → Invesco → Other)
    2. Within each category: by month/year (nearest first)
    """
    if df.empty:
        return df
    
    # Create sorting columns
    sort_data = []
    for idx, row in df.iterrows():
        name = str(row.get('SecurityName', row.get('Name', '')))
        category, cat_priority = categorize_holding(name, etf_code)
        month, year = extract_month_year(name)
        
        sort_data.append({
            'index': idx,
            'cat_priority': cat_priority,
            'year': year,
            'month': month,
            'category': category
        })
    
    # Create sort dataframe
    sort_df = pd.DataFrame(sort_data)
    
    # Merge with original data
    df_with_sort = df.copy()
    df_with_sort['_sort_idx'] = range(len(df))
    df_with_sort = df_with_sort.merge(sort_df, left_on='_sort_idx', right_on='index', how='left')
    
    # Sort: category priority → year → month
    df_sorted = df_with_sort.sort_values(
        by=['cat_priority', 'year', 'month'],
        ascending=[True, True, True]
    )
    
    # Drop temporary columns
    df_sorted = df_sorted.drop(columns=['_sort_idx', 'index', 'cat_priority', 'year', 'month', 'category'], errors='ignore')
    
    return df_sorted

def download_master_csv():
    """Download the master CSV file containing all ETF holdings"""
    try:
        print(f"Downloading master CSV from Amplify ETFs...")
        response = requests.get(CSV_URL, headers=HEADERS, timeout=30)
        
        if response.status_code != 200:
            print(f"ERROR: Failed to download (Status {response.status_code})")
            return None
        
        # Save to temp file
        temp_file = 'temp_master_holdings.csv'
        with open(temp_file, 'wb') as f:
            f.write(response.content)
        
        # Read CSV
        df = pd.read_csv(temp_file)
        os.remove(temp_file)
        
        print(f"✓ Downloaded {len(df)} total holdings")
        return df
        
    except Exception as e:
        print(f"ERROR downloading CSV: {str(e)}")
        return None

def process_etf(df, etf_code, output_file):
    """Extract and process single ETF from master data"""
    try:
        print(f"\nProcessing {etf_code}...")
        
        # Filter by ETF code (Account column contains ETF ticker)
        etf_df = df[df['Account'].str.upper() == etf_code].copy()
        
        if etf_df.empty:
            print(f"  WARNING: No holdings found for {etf_code}")
            return False
        
        print(f"  Found {len(etf_df)} holdings")
        
        # Rename columns to match your Excel format
        column_mapping = {
            'Date': 'Date',
            'Account': 'ETF',
            'StockTicker': 'Ticker',
            'CUSIP': 'CUSIP',
            'SecurityName': 'Name',
            'Shares': 'Lots',
            'Price': 'Price',
            'MarketValue': 'Market_Value',
            'Weightings': 'Weightings'
        }
        
        etf_df = etf_df.rename(columns=column_mapping)
        
        # Select ONLY the columns you want
        desired_cols = ['Name', 'Ticker', 'CUSIP', 'Lots', 'Price', 
                       'Market_Value', 'Weightings']
        available_cols = [c for c in desired_cols if c in etf_df.columns]
        etf_df = etf_df[available_cols]
        
        # Sort holdings
        print(f"  Sorting holdings...")
        etf_df = sort_holdings(etf_df, etf_code)
        
        # Save to CSV
        etf_df.to_csv(output_file, index=False)
        print(f"  ✓ Saved {len(etf_df)} rows to {output_file}")
        
        # Print summary
        print_summary(etf_df, etf_code)
        
        return True
        
    except Exception as e:
        print(f"  ERROR processing {etf_code}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def print_summary(df, etf_code):
    """Print summary by category"""
    print(f"\n  Summary by Category:")
    print(f"  {'-'*50}")
    
    total_value = df['Market_Value'].sum()
    
    # Determine categories based on ETF type
    if etf_code == 'BDRY':
        categories = ['capesize', 'panamax', 'supramax', 'cash', 'invesco']
    else:  # BWET
        categories = ['td3c', 'td20', 'cash', 'invesco']
    
    for cat in categories:
        mask = df['Name'].str.lower().str.contains(cat, na=False)
        cat_df = df[mask]
        
        if not cat_df.empty:
            cat_value = cat_df['Market_Value'].sum()
            cat_pct = (cat_value / total_value * 100) if total_value > 0 else 0
            print(f"  {cat.capitalize():12} : ${cat_value:>15,.2f} ({cat_pct:5.2f}%) - {len(cat_df)} holdings")

def main():
    print(f"Starting ETF Holdings Update")
    print(f"Time: {datetime.now()}")
    print("=" * 60)
    
    # Download master CSV
    master_df = download_master_csv()
    if master_df is None:
        print("FAILED: Could not download master CSV")
        return 1
    
    # Process each target ETF
    success_count = 0
    for etf_code, output_file in TARGET_ETFS.items():
        if process_etf(master_df, etf_code, output_file):
            success_count += 1
    
    print("\n" + "=" * 60)
    print(f"Completed: {success_count}/{len(TARGET_ETFS)} ETFs updated successfully")
    
    if success_count < len(TARGET_ETFS):
        print("WARNING: Some updates failed")
        return 1
    return 0

if __name__ == "__main__":
    exit(main())
