import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

#Endpoints
datasets = "/datasets"
datacategories = "/datacategories"
datatypes = "/datatypes"
locationcategories = "/locationcategories"
locations = "/locations"
stations = "/stations"
data = "/data"

BASE_KEY = os.getenv("NCDC_CDO_KEY")
BASE_URL = os.getenv("NCDC_CDO_URL")
headers = {"token": BASE_KEY}
params = {"limit": 1000, "offset": 1}
url = f"{BASE_URL}{locations}"

#r = requests.get(url=url, headers=headers, params=params)
#data = r.json()
#r.raise_for_status()
#print(data)
#results = data.get("results", [])
#print(results)
offset = 1
limit = 1000
all_results = []
while True:
    params = {"limit": limit, "offset": offset}
    r = requests.get(url, headers=headers, params=params)
    r.raise_for_status()
    data = r.json()
    results = data.get("results", [])
    if not results:
        break
    all_results.extend(results)
    print(f"Fetched {len(results)} locations (offset={offset})")
    offset += limit

print(all_results)
print(len(all_results)) 