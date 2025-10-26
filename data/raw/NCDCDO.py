import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Endpoints
datasets = "/datasets"
datacategories = "/datacategories"
datatypes = "/datatypes"
locationcategories = "/locationcategories"
locations = "/locations"
stations = "/stations"
data = "/data"

# ENV Constants
BASE_KEY = os.getenv("NCDC_CDO_KEY")
BASE_URL = os.getenv("NCDC_CDO_URL")

# url info
headers = {"token": BASE_KEY}
params = {"limit": 1000, "offset": 1}

"""
Function requests datasets endpoint information
"""
def get_datasets() -> list:

    offset = 1
    limit = 1000
    all_results = []
    sum = 0
    while True:
        url = f"{BASE_URL}{datasets}"
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
        sum +=1
    return all_results

"""
Function requests datacategories endpoint information
"""
def get_datacategories() -> list:

    offset = 1
    limit = 1000
    all_results = []
    sum = 0
    while True:
        url = f"{BASE_URL}{datacategories}"
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
        sum +=1
    return all_results

"""
Function requests datatypes endpoint information
"""
def get_datatypes() -> list:

    offset = 1
    limit = 1000
    all_results = []
    sum = 0
    while True:
        url = f"{BASE_URL}{datatypes}"
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
        sum +=1
    return all_results

"""
Function requests locationcategories endpoint information
"""
def get_locationcategories() -> list:

    offset = 1
    limit = 1000
    all_results = []
    sum = 0
    while True:
        url = f"{BASE_URL}{locationcategories}"
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
        sum +=1
    return all_results

"""
Function requests locations endpoint information
"""
def get_locations() -> list:

    offset = 1
    limit = 1000
    all_results = []
    sum = 0
    headers = {"token": BASE_KEY}
    params = {"limit": 1000, "offset": 1}
    url = f"{BASE_URL}{locations}"
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
        sum +=1
    return all_results

"""
Function requests stations endpoint information
"""
def get_stations() -> list:

    offset = 1
    limit = 1000
    all_results = []
    sum = 0
    while True:
        url = f"{BASE_URL}{stations}"
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
        sum +=1
    return all_results

"""
Function requests data endpoint information
"""
def get_data() -> list:

    offset = 1
    limit = 1000
    all_results = []
    sum = 0
    while True:
        url = f"{BASE_URL}{data}"
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
        sum +=1
    return all_results

