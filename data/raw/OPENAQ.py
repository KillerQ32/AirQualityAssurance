import os, requests, time
import pandas as pd
from dotenv import load_dotenv
from openaq import OpenAQ

load_dotenv()

API_KEY = os.getenv("OPENAQ_KEY")
BASE_URL = os.getenv("OPENAQ_URL")
client = OpenAQ(api_key=API_KEY)

#LOCATIONS
BALTIMORE_COUNTY_CENTER = (39.4015, -76.6019)

#DATATYPES
PM25 = "pm25"
O3 = "o3"
NO2 = "no2"
SO2 = "so2"
CO = "co"

#TIME BASED AGGREGATION
DAILY = "/days"
HOURLY = "/hours"
YEARLY = "/years"

headers = {"X-API-Key": API_KEY}

def _to_int(x, default=0):
    try:
        return int(x)
    except (TypeError, ValueError):
        return default

def fetch_all(url, params=None, limit=1000, timeout=60, max_retries=6):
    params = dict(params or {})
    params["limit"] = limit
    page = 1
    out = []

    while True:
        params["page"] = page

        for attempt in range(max_retries):
            r = requests.get(url, headers=headers, params=params, timeout=timeout)

            if r.status_code == 429:
                wait_s = _to_int(r.headers.get("Retry-After"), 20) + 2
                print(f"Rate limited. Sleeping {wait_s}s...")
                time.sleep(wait_s)
                continue

            if r.status_code in (408, 500, 502, 503, 504):
                backoff = 2 * (attempt + 1)
                print(f"HTTP {r.status_code}. Retry in {backoff}s...")
                time.sleep(backoff)
                continue

            r.raise_for_status()
            break
        else:
            raise RuntimeError(f"Failed after retries: {url} params={params}")

        data = r.json()
        results = data.get("results", []) or []
        if not results:
            break

        out.extend(results)

        meta = data.get("meta") or {}
        found = _to_int(meta.get("found"), 0)

        if found > 0:
            if page * limit >= found:
                break
        else:
            if len(results) < limit:
                break

        page += 1

        time.sleep(0.2)

    return out

def get_locations(loc_coord: list[float]):
    locations = client.locations.list(
        coordinates=loc_coord,
        radius=24_000,
        limit=1000
    )
    return locations.results

all_locations = get_locations(BALTIMORE_COUNTY_CENTER)

def get_data_lvls(data_type: str, date_from: str, date_to: str):
    rows = []

    for loc in all_locations:
        for s in (loc.sensors or []):
            if s.parameter and s.parameter.name == data_type:

                page = 1
                limit = 1000

                while True:
                    meas = client.measurements.list(
                        sensors_id=s.id,
                        datetime_from=date_from,
                        datetime_to=date_to,
                        limit=limit,
                        page=page
                    )
                    meas = client.measurements.list()
                    results = meas.dict().get("results", [])
                    if not results:
                        break
                    
                    for r in results:
                        r["location_id"] = loc.id
                        r["location_name"] = loc.name
                        r["parameter"] = s.parameter.name
                        r["parameter_units"] = s.parameter.units
                        rows.append(r)

                    page += 1
            
    return rows

def get_data_lvls_agg(data_type: str, aggregation: str, date_from: str, date_to: str):
    
    rows = []

    for loc in all_locations:
        for s in (loc.sensors or []):
            if s.parameter and s.parameter.name == data_type:

                hourly = fetch_all(
                    f"{BASE_URL}/sensors/{s.id}{aggregation}",
                    {"datetime_from":date_from, "datetime_to":date_to}
                )

                for r in hourly:
                    
                    data = {
                        "sensor_id": s.id,
                        "location_id": loc.id,
                        "location_name": loc.name,
                        "parameter": s.parameter.name,
                        "parameter_units": s.parameter.units,
                        "value": r["value"]
                    }

                    rows.append(data)

    return rows

def main():
    rows = get_data_lvls_agg(PM25, YEARLY, "2018-01-01", "2019-01-01")
    df = pd.DataFrame(rows)
    print(df.head())
    print("rows:", len(df))
    df.to_excel("pm25year.xlsx", index=False)
    client.close()

main()