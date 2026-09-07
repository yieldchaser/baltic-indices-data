import requests
import re

url = 'https://shipandbunker.cdn.speedyrails.net/res/js/v2/block-helpers/MarketPriceGraph_Block.v.652.js'
r = requests.get(url)

methods = re.findall(r'method\s*=\s*["\']([^"\']+)["\']', r.text)
print('Methods found in MarketPriceGraph_Block:', set(methods))
resources = re.findall(r'resource\s*=\s*["\']([^"\']+)["\']', r.text)
print('Resources found in MarketPriceGraph_Block:', set(resources))

# Search for all occurrences of api-method in the entire script
api_methods = re.findall(r'api-method["\'=:\s]+([a-zA-Z0-9_]+)', r.text)
print('api-methods found:', set(api_methods))

# Search for daynum or range in queries
daynum_queries = re.findall(r'query\s*\+?=\s*([^;]+);', r.text)
print('All query additions:')
for q in daynum_queries:
    print('  ', q.strip())
