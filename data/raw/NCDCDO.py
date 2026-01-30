import requests
import pandas as pd
import os
import time
from openpyxl import load_workbook, Workbook
from dotenv import load_dotenv
from requests.exceptions import RequestException, HTTPError

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

#Station ABV Constants
MSCABV = "GHCND:USW00093784"
BWIABV = "GHCND:USW00093721"

#Station Constants
BWI = "BALTIMORE WASHINGTON INTERNATIONAL AIRPORT"
MSC = "MARYLAND SCIENCE CENTER"

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
        #sum +=1'
    return all_results

def get_dataset_ids():
    dataset_df = pd.DataFrame(get_datasets())
    id_list = dataset_df["id"].to_list()
    return id_list

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
        time.sleep(1)
        #sum +=1
    return all_results

def get_datatypes_ids() -> list:
    list1 = get_datatypes()
    df = pd.DataFrame(list1)
    list1 = df["id"].to_list()
    return list1

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
    url = f"{BASE_URL}{locations}"
    while True:
        params = {"limit": limit, "offset": offset, "locationcategoryid": "CITY"}
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        if not results:
            break
        all_results.extend(results)
        print(f"Fetched {len(results)} locations (offset={offset})")
        offset += limit
        time.sleep(.2)
        if len(results) < limit:
            break
    return all_results

def get_location_ids():
    location_df = pd.DataFrame(get_locations())
    id_list = location_df["id"].to_list()
    return id_list

def get_location_names():
    location_df = pd.DataFrame(get_locations())
    return location_df["name"].to_list()
    
#list2 = get_location_names()
#print(list2)

#location_id_list = get_location_ids()
#print(location_id_list)

"""
Function requests stations endpoint information
"""
def get_stations() -> list:

    offset = 1
    limit = 1000
    all_results = []
    #sum = 0
    while True:
        url = f"{BASE_URL}{stations}"
        params = {"limit": 1000, "offset": 1, "locationid": "CITY:US240002", "datasetid": "GSOY"}
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        if not results:
            break
        all_results.extend(results)
        print(f"Fetched {len(results)} stations (offset={offset})")
        offset += limit
        time.sleep(.3)
        #sum +=1
        if len(results) < limit:
            break
    return all_results

"""
Function requests data endpoint information
"""
def get_data(
    dataset_id: str,
    date: str,
    station_id: str,
    datatype_id: list[str],
    max_retries: int = 3,
    retry_delay: float = 2.0,
) -> list:

    offset = 1
    limit = 1000
    all_results = []

    while True:
        url = f"{BASE_URL}{data_endpoint}"
        params = {
            "datasetid": dataset_id,
            "stationid": station_id,
            "startdate": f"{date}-01-01",
            "enddate": f"{date}-12-31",
            "limit": limit,
            "offset": offset,
            "datatypeid": datatype_id,
            "units": "standard",
        }

        attempt = 0
        while attempt < max_retries:
            try:
                r = requests.get(url, headers=headers, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                break  # success → exit retry loop
            except (HTTPError, RequestException) as e:
                attempt += 1
                print(
                    f"[WARN] NOAA request failed "
                    f"(station={station_id}, year={date}, offset={offset}, attempt={attempt}/{max_retries}): {e}"
                )

                if attempt >= max_retries:
                    print(
                        f"[ERROR] Skipping this page after {max_retries} failed attempts "
                        f"(station={station_id}, year={date}, offset={offset})"
                    )
                    return all_results  # ← do NOT crash entire script

                time.sleep(retry_delay * attempt)  # exponential-ish backoff

        results = data.get("results", [])
        if not results:
            break

        all_results.extend(results)
        print(f"Fetched {len(results)} rows (offset={offset})")

        offset += limit

        if len(results) < limit:
            break

        time.sleep(0.2)  # respect NOAA rate limits

    return all_results

def get_data_year(station_abv: str, station: str):

    all_results = []

    i = 1999
    while i <= 2025:
        results = get_data("GHCND", str(i), station_abv, ["TMAX", "TMIN", "PRCP"])    
        all_results.extend(results)
        print("fetched year", i)
        i += 1
    
    df = pd.DataFrame(all_results)
    df["station"] = station

    return df

if __name__ == "__main__":
    # your test calls here

    results = get_locations()
    df = pd.DataFrame(results)
    df.to_excel("locations1.xlsx", sheet_name="station")

    results = get_stations()
    df = pd.DataFrame(results)
    df.to_excel("newstations.xlsx")

    get_data_year(MSCABV, MSC)
    get_data_year(BWIABV, BWI)