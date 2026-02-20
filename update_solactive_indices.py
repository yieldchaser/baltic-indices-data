import pandas as pd
from datetime import datetime
import json
import re

# For Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Indices to scrape
INDICES = {
    'BDRY': {
        'url': 'https://www.solactive.com/Indices/?index=DE000SLA4BY3',
        'output': 'solactive_bdry.csv'
    },
    'BWET': {
        'url': 'https://www.solactive.com/Indices/?index=DE000SL0HLG3',
        'output': 'solactive_bwet.csv'
    }
}

def setup_driver():
    """Setup headless Chrome driver"""
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    
    # For GitHub Actions
    try:
        driver = webdriver.Chrome(options=chrome_options)
    except:
        # Try with chromedriver path for Linux
        service = Service('/usr/bin/chromedriver')
        driver = webdriver.Chrome(service=service, options=chrome_options)
    
    return driver

def scrape_index(name, config):
    """Scrape index data using Selenium"""
    driver = None
    try:
        print(f"\nProcessing {name}...")
        driver = setup_driver()
        
        # Load page
        driver.get(config['url'])
        
        # Wait for CURRENT QUOTES section to load
        wait = WebDriverWait(driver, 10)
        
        # Try to find the CURRENT QUOTES heading or data
        try:
            # Wait for any of these elements
            wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'CURRENT QUOTES')]")))
        except:
            print(f"  Warning: CURRENT QUOTES not found, continuing anyway...")
        
        # Get page source after JavaScript execution
        page_source = driver.page_source
        
        # Parse with regex
        data = {
            'Index': name,
            'Last_Updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Last quote (format: "Last quote (19 Feb 2026): 2376.36")
        match = re.search(r'Last quote\s*\(([^)]+)\):\s*([\d.,]+)', page_source)
        if match:
            data['Last_Quote_Date'] = match.group(1).strip()
            data['Last_Quote_Value'] = float(match.group(2).replace(',', ''))
            print(f"  ✓ Last Quote: {data['Last_Quote_Value']}")
        else:
            print(f"  ✗ Last Quote not found")
        
        # Day range
        match = re.search(r'Day range:\s*([\d.,]+)\s*/\s*([\d.,]+)', page_source)
        if match:
            data['Day_Range_Low'] = float(match.group(1).replace(',', ''))
            data['Day_Range_High'] = float(match.group(2).replace(',', ''))
            print(f"  ✓ Day Range: {data['Day_Range_Low']} / {data['Day_Range_High']}")
        else:
            print(f"  ✗ Day Range not found")
        
        # Change abs./rel.
        match = re.search(r'Change abs\./rel\.:\s*([-\d.,]+)\s*/\s*([-\d.,]+)%?', page_source)
        if match:
            data['Change_Abs'] = float(match.group(1).replace(',', ''))
            data['Change_Rel'] = float(match.group(2).replace(',', ''))
            print(f"  ✓ Change: {data['Change_Abs']} / {data['Change_Rel']}%")
        else:
            print(f"  ✗ Change not found")
        
        # Year range
        match = re.search(r'Year range:\s*([\d.,]+)\s*/\s*([\d.,]+)', page_source)
        if match:
            data['Year_Range_Low'] = float(match.group(1).replace(',', ''))
            data['Year_Range_High'] = float(match.group(2).replace(',', ''))
            print(f"  ✓ Year Range: {data['Year_Range_Low']} / {data['Year_Range_High']}")
        else:
            print(f"  ✗ Year Range not found")
        
        # Save to CSV
        df = pd.DataFrame([data])
        df.to_csv(config['output'], index=False)
        print(f"  ✓ Saved to {config['output']}")
        
        return True
        
    except Exception as e:
        print(f"  ✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if driver:
            driver.quit()

def main():
    print(f"Starting Solactive Indices Update")
    print(f"Time: {datetime.now()}")
    print("=" * 60)
    
    success_count = 0
    for name, config in INDICES.items():
        if scrape_index(name, config):
            success_count += 1
    
    print("\n" + "=" * 60)
    print(f"Completed: {success_count}/{len(INDICES)} indices updated")
    
    if success_count < len(INDICES):
        return 1
    return 0

if __name__ == "__main__":
    exit(main())
