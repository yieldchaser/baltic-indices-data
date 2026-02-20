import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime

# Test URLs
urls = {
    'BDRY': 'https://www.solactive.com/Indices/?index=DE000SLA4BY3',
    'BWET': 'https://www.solactive.com/Indices/?index=DE000SL0HLG3'
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

def test_scrape(name, url):
    print(f"\n{'='*60}")
    print(f"Testing {name}: {url}")
    print('='*60)
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        print(f"Status Code: {response.status_code}")
        print(f"Content Length: {len(response.text)} characters")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        page_text = soup.get_text()
        
        # Check if CURRENT QUOTES exists
        if 'CURRENT QUOTES' not in page_text:
            print("✗ 'CURRENT QUOTES' not found in page")
            return False
        
        print("✓ Found 'CURRENT QUOTES' section")
        
        # Extract snippet around CURRENT QUOTES
        idx = page_text.find('CURRENT QUOTES')
        snippet = page_text[idx:idx+1000]
        
        print(f"\n--- Raw Snippet ---\n{snippet}\n--- End Snippet ---\n")
        
        # Try to extract values
        data = {}
        
        # Last quote (format: "Last quote (19 Feb 2026): 2376.36")
        match = re.search(r'Last quote\s*\(([^)]+)\):\s*([\d.,]+)', snippet)
        if match:
            data['Last_Quote_Date'] = match.group(1).strip()
            data['Last_Quote_Value'] = match.group(2).strip()
            print(f"✓ Last Quote: {data['Last_Quote_Date']} = {data['Last_Quote_Value']}")
        else:
            print("✗ Could not parse Last quote")
        
        # Day range (format: "Day range: 2376.36 / 2384.78")
        match = re.search(r'Day range:\s*([\d.,]+)\s*/\s*([\d.,]+)', snippet)
        if match:
            data['Day_Range'] = f"{match.group(1)} / {match.group(2)}"
            print(f"✓ Day Range: {data['Day_Range']}")
        else:
            print("✗ Could not parse Day range")
        
        # Change abs./rel. (format: "Change abs./rel.: -0.32 / -0.01%" or "191.58 / 7.43%")
        match = re.search(r'Change abs\./rel\.:\s*([-\d.,]+)\s*/\s*([-\d.,]+)%?', snippet)
        if match:
            data['Change_Abs'] = match.group(1).strip()
            data['Change_Rel'] = match.group(2).strip()
            print(f"✓ Change: {data['Change_Abs']} / {data['Change_Rel']}%")
        else:
            print("✗ Could not parse Change abs./rel.")
        
        # Year range (format: "Year range: 1333.82 / 2384.78")
        match = re.search(r'Year range:\s*([\d.,]+)\s*/\s*([\d.,]+)', snippet)
        if match:
            data['Year_Range'] = f"{match.group(1)} / {match.group(2)}"
            print(f"✓ Year Range: {data['Year_Range']}")
        else:
            print("✗ Could not parse Year range")
        
        return len(data) > 0
        
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print(f"Solactive Index Scraper Test")
    print(f"Time: {datetime.now()}")
    
    results = {}
    for name, url in urls.items():
        results[name] = test_scrape(name, url)
    
    print(f"\n{'='*60}")
    print("SUMMARY")
    print('='*60)
    for name, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        print(f"{name}: {status}")
    
    if all(results.values()):
        print("\n🎉 All tests passed! Ready to deploy to GitHub.")
    else:
        print("\n⚠️  Some tests failed. Check the output above.")

if __name__ == "__main__":
    main()
