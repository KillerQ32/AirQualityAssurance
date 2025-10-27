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
data_endpoint = "/data"

# ENV Constants
BASE_KEY = os.getenv("NCDC_CDO_KEY")
BASE_URL = os.getenv("NCDC_CDO_URL")

# url info
headers = {"token": BASE_KEY}


"""
Function requests datasets endpoint information
"""
def get_datasets() -> list:

    offset = 1
    limit = 1000
    all_results = []
    #sum = 0
    params = {"limit": 1000, "offset": 1}
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
        #sum +=1
    return all_results

def get_dataset_ids():
    dataset_df = pd.DataFrame(get_datasets())
    id_list = dataset_df["id"].to_list()
    return id_list

id_list = get_dataset_ids()

"""
Function requests datacategories endpoint information
"""
def get_datacategories() -> list:

    offset = 1
    limit = 1000
    all_results = []
    #sum = 0
    params = {"limit": 1000, "offset": 1}
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
        #sum +=1
    return all_results

"""
Function requests datatypes endpoint information
"""
def get_datatypes() -> list:

    offset = 1
    limit = 1000
    all_results = []
    #sum = 0
    params = {"limit": 1000, "offset": 1}
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
        #sum +=1
    return all_results

"""
Function requests locationcategories endpoint information
"""
def get_locationcategories() -> list:

    offset = 1
    limit = 1000
    all_results = []
    #sum = 0
    params = {"limit": 1000, "offset": 1}
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
        #sum +=1
    return all_results

"""
Function requests locations endpoint information
"""
def get_locations() -> list:

    offset = 1
    limit = 1000
    all_results = []
    sum = 0
    params = {"limit": 1000, "offset": 1}
    url = f"{BASE_URL}{locations}"
    while sum < 5:
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


def get_location_ids():
    location_df = pd.DataFrame(get_locations())
    id_list = location_df["id"].to_list()
    return id_list

location_id_list = get_location_ids()
print(location_id_list)
"""
Function requests stations endpoint information
"""
def get_stations() -> list:

    offset = 1
    limit = 1000
    all_results = []
    #sum = 0
    params = {"limit": 1000, "offset": 1}
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
        print(f"Fetched {len(results)} stations (offset={offset})")
        offset += limit
        #sum +=1
    return all_results

"""
Function requests data endpoint information
"""
def get_data() -> list:

    offset = 1
    limit = 1000
    all_results = []
    sum = 0
    params1 = {
            "datasetid": id_list[0],
            "locationid": location_id_list[0],
            "startdate": "2023-01-01",
            "enddate": "2023-12-31",
            "limit": limit,
            "offset": offset,
            "datatypeid": ["TMAX", "TMIN", "PRCP", "AWND"],
            "units": "standard"}
    while sum < 1:
        url = f"{BASE_URL}{data_endpoint}"
        params = {"limit": limit, "offset": offset}
        r = requests.get(url, headers=headers, params=params1)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        if not results:
            break
        all_results.extend(results)
        print(f"Fetched {len(results)} data (offset={offset})")
        offset += limit
        sum +=1
    return all_results


print(id_list)

results = get_data()
print(results)
df = pd.DataFrame(results)
print(df["datatype"])