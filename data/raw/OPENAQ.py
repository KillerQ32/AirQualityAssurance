import os, requests, time
import pandas as pd
from dotenv import load_dotenv
from openaq import OpenAQ
from datetime import datetime

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
NOX = "nox"
PM10 = "pm10"
PM1 = "pm1"

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

            # handle rate limit (429)
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

def get_specific_loc(location: str):
    for loc in all_locations:
        if loc.name == location:
            return loc

def get_location_details():
    location_list = []
    for loc in all_locations:
        location_dict = {
            "id": loc.id,
            "name": loc.name,
            "timezone": loc.timezone,
            "country": loc.country.name       
        }
        location_list.append(location_dict)
    return location_list

def get_sensor_details():
    sensor_list = []
    for loc in all_locations:
        for s in (loc.sensors or []):
            sensor_dict = {
                "id": s.id,
                "name": s.name,
                "location_id": loc.id,
                "location_name": loc.name,
                "parameter": s.parameter.name
            }
            sensor_list.append(sensor_dict)
    return sensor_list

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

                agg = fetch_all(
                    url=f"{BASE_URL}/sensors/{s.id}{aggregation}",
                    params={"date_from":date_from, "date_to":date_to}
                )

                for r in agg:
                    
                    data = {
                        "sensor_id": s.id,
                        "location_id": loc.id,
                        "location_name": loc.name,
                        "parameter": s.parameter.name,
                        "parameter_units": s.parameter.units,
                        "value": r["value"],
                        "min": r["summary"]["q02"],
                        "q02": r["summary"]["q02"],
                        "q25": r["summary"]["q25"],
                        "median": r["summary"]["median"],
                        "q75": r["summary"]["q75"],
                        "q98": r["summary"]["q98"],
                        "max": r["summary"]["max"],
                        "avg": r["summary"]["avg"],
                        "sd": r["summary"]["sd"],
                        "date_from": datetime.fromisoformat(r["coverage"]["datetimeFrom"]["local"]).date(),
                        "date_to": datetime.fromisoformat(r["coverage"]["datetimeTo"]["local"]).date()
                    }

                    rows.append(data)

    return rows

def get_data_lvls_agg_loc(data_type: str, loc, aggregation: str, date_from: str, date_to: str):
    
    rows = []
    for s in (loc.sensors or []):
        if s.parameter and s.parameter.name == data_type:

            agg = fetch_all(
                f"{BASE_URL}/sensors/{s.id}{aggregation}",
                {"date_from":date_from, "date_to":date_to}
            )

            for r in agg:
                    
                data = {
                    "sensor_id": s.id,
                    "location_id": loc.id,
                    "location_name": loc.name,
                    "parameter": s.parameter.name,
                    "parameter_units": s.parameter.units,
                    "value": r["value"],
                    "min": r["summary"]["q02"],
                    "q02": r["summary"]["q02"],
                    "q25": r["summary"]["q25"],
                    "median": r["summary"]["median"],
                    "q75": r["summary"]["q75"],
                    "q98": r["summary"]["q98"],
                    "max": r["summary"]["max"],
                    "avg": r["summary"]["avg"],
                    "sd": r["summary"]["sd"],
                    "date_from": datetime.fromisoformat(r["coverage"]["datetimeFrom"]["local"]).date(),
                    "date_to": datetime.fromisoformat(r["coverage"]["datetimeTo"]["local"]).date()
                }

                rows.append(data)
            time.sleep(1/60)

    return rows

def Padonia():
    #Daily
    for i in range(0, 26):
        loc = get_specific_loc("Padonia")
        rows = get_data_lvls_agg_loc(PM25, loc, DAILY, f"20{i:02d}-01-01", f"20{i+1:02d}-01-01")
        df = pd.DataFrame(rows)
        df.to_excel(f"padonia_pm25_daily_20{i:02d}.xlsx", index=False)
    
    #ANUALLY
    for i in range(0, 26):
        loc = get_specific_loc("Padonia")
        rows = get_data_lvls_agg_loc(PM25, loc, YEARLY, f"20{i:02d}-01-01", f"20{i+1:02d}-01-01")
        df = pd.DataFrame(rows)
        df.to_excel(f"padonia_pm25_yearly_20{i:02d}.xlsx", index=False)

keys = ["location_id", "sensor_id", "parameter"]
value_cols = ["value","q02","q25","median","q75","q98","min","max","avg","sd"]

def add_missing_dates(g: pd.DataFrame, key_vals=None) -> pd.DataFrame:
    """
    Keep your behavior, but:
    - Works without groupby.apply (no deprecation issues)
    - Restores keys reliably
    - Prevents reindex duplicate-label crashes
    """
    g = g.copy()

    if key_vals is not None:
        if not isinstance(key_vals, tuple):
            key_vals = (key_vals,)
        for k, v in zip(keys, key_vals):
            g[k] = v

    g["date"] = pd.to_datetime(g["date"]).dt.normalize()
    g = g.sort_values("date")

    g = g.drop_duplicates(subset=["date"], keep="last")

    g = g.set_index("date")

    full = pd.date_range(g.index.min(), g.index.max(), freq="D")
    g2 = g.reindex(full)

    for c in keys + ["location_name", "parameter_units", "location_id", "sensor_id"]:
        if c in g2.columns:
            g2[c] = g2[c].ffill().bfill()

    g2 = g2.reset_index().rename(columns={"index": "date"})
    g2["has_measurement"] = g2["avg"].notna() if "avg" in g2.columns else False
    return g2


def fill_missing_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Replacement for df.groupby(...).apply(add_missing_dates, include_groups=False)."""
    out = []
    for key_vals, g in df.groupby(keys, sort=False, dropna=False):
        out.append(add_missing_dates(g, key_vals))
    return pd.concat(out, ignore_index=True) if out else df


def pm25():
    all_daily = []
    for i in range(16, 26):
        rows = get_data_lvls_agg(PM25, DAILY, f"20{i:02d}-01-01", f"20{i+1:02d}-01-01")
        all_daily.extend(rows)

    df = pd.DataFrame(all_daily)
    df["date"] = pd.to_datetime(df["date_from"]).dt.normalize()

    df_full = fill_missing_dates(df)
    df_full.drop(columns=["date_from", "date_to"], inplace=True, errors="raise")

    return df_full, None


def co():
    all_daily = []
    for i in range(16, 26):
        rows = get_data_lvls_agg(CO, DAILY, f"20{i:02d}-01-01", f"20{i+1:02d}-01-01")
        all_daily.extend(rows)

    df = pd.DataFrame(all_daily)
    df["date"] = pd.to_datetime(df["date_from"]).dt.normalize()

    df_full = fill_missing_dates(df)
    df_full.drop(columns=["date_from", "date_to"], inplace=True, errors="raise")

    return df_full, None


def o3():
    all_daily = []
    for i in range(16, 26):
        rows = get_data_lvls_agg(O3, DAILY, f"20{i:02d}-01-01", f"20{i+1:02d}-01-01")
        all_daily.extend(rows)

    df = pd.DataFrame(all_daily)
    df["date"] = pd.to_datetime(df["date_from"]).dt.normalize()

    df_full = fill_missing_dates(df)
    df_full.drop(columns=["date_from", "date_to"], inplace=True, errors="raise")

    return df_full, None


def so2():
    all_daily = []
    for i in range(16, 26):
        rows = get_data_lvls_agg(SO2, DAILY, f"20{i:02d}-01-01", f"20{i+1:02d}-01-01")
        all_daily.extend(rows)

    df = pd.DataFrame(all_daily)
    df["date"] = pd.to_datetime(df["date_from"]).dt.normalize()

    df_full = fill_missing_dates(df)
    df_full.drop(columns=["date_from", "date_to"], inplace=True, errors="raise")

    return df_full, None


def no2():
    all_daily = []
    for i in range(16, 26):
        rows = get_data_lvls_agg(NO2, DAILY, f"20{i:02d}-01-01", f"20{i+1:02d}-01-01")
        all_daily.extend(rows)

    df = pd.DataFrame(all_daily)
    df["date"] = pd.to_datetime(df["date_from"]).dt.normalize()

    df_full = fill_missing_dates(df)
    df_full.drop(columns=["date_from", "date_to"], inplace=True, errors="raise")

    return df_full, None


def nox():
    all_daily = []
    for i in range(16, 26):
        rows = get_data_lvls_agg(NOX, DAILY, f"20{i:02d}-01-01", f"20{i+1:02d}-01-01")
        all_daily.extend(rows)

    df = pd.DataFrame(all_daily)
    df["date"] = pd.to_datetime(df["date_from"]).dt.normalize()

    df_full = fill_missing_dates(df)
    df_full.drop(columns=["date_from", "date_to"], inplace=True, errors="raise")

    return df_full, None


def pm1():
    all_daily = []
    for i in range(16, 26):
        rows = get_data_lvls_agg(PM1, DAILY, f"20{i:02d}-01-01", f"20{i+1:02d}-01-01")
        all_daily.extend(rows)

    df = pd.DataFrame(all_daily)
    df["date"] = pd.to_datetime(df["date_from"]).dt.normalize()

    df_full = fill_missing_dates(df)
    df_full.drop(columns=["date_from", "date_to"], inplace=True, errors="raise")

    return df_full, None


def pm10():
    all_daily = []
    for i in range(16, 26):
        rows = get_data_lvls_agg(PM10, DAILY, f"20{i:02d}-01-01", f"20{i+1:02d}-01-01")
        all_daily.extend(rows)

    df = pd.DataFrame(all_daily)
    df["date"] = pd.to_datetime(df["date_from"]).dt.normalize()

    df_full = fill_missing_dates(df)
    df_full.drop(columns=["date_from", "date_to"], inplace=True, errors="raise")

    return df_full, None

client.close()