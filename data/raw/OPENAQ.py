import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("OPENAQ_URL")
API_KEY = os.getenv("OPENAQ_KEY")

headers = {"X-API-Key": API_KEY}

endpoint = "latest"
params = {"country": "US", "limit": 10}

url = f"{BASE_URL.rstrip('/')}/{endpoint}"

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
    results = data.get("results", [])
    df = pd.DataFrame(results)
    print(df.head())
else:
    print("Error:", response.status_code, response.text)
