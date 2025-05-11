import os

import pandas as pd
import requests

API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY")

# At the time of writing, there is a 500 request per day limit with the free API key
# 75 requests per minute with minimum premium API key, $50/month

# example code
url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey={API_KEY}"
r = requests.get(url)
data = r.json()

df = pd.DataFrame(data)
