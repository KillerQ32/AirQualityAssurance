import pandas as pd
from sqlalchemy import text

from sql.engine import engine

import data.raw.OPENAQ as openaq
import data.raw.NOAACO2 as co2
import data.raw.NCDCDO as ncdc

PUBLIC_SCHEMA = "public"


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def create_and_load(df: pd.DataFrame, table_name: str, engine, if_exists: str = "replace"):
    """
    Option B:
      - Create table schema from df (0 rows)
      - Append full df
    """
    df = normalize_columns(df)

    df.head(0).to_sql(
        table_name,
        engine,
        schema=PUBLIC_SCHEMA,
        if_exists=if_exists,
        index=False,
    )

    df.to_sql(
        table_name,
        engine,
        schema=PUBLIC_SCHEMA,
        if_exists="append",
        index=False,
        chunksize=10_000,
        method="multi",
    )

    print(f"Loaded {len(df):,} rows -> {PUBLIC_SCHEMA}.{table_name}")


def load_openaq(engine):
    pollutant_fns = [
        openaq.pm25,
        openaq.co,
        openaq.o3,
        openaq.so2,
        openaq.no2,
        openaq.nox,
        openaq.pm1,
        openaq.pm10,
    ]

    daily_frames = []
    for fn in pollutant_fns:
        df_daily, _ = fn()
        daily_frames.append(df_daily)

    df_all = pd.concat(daily_frames, ignore_index=True)

    df_all["date"] = pd.to_datetime(df_all["date"]).dt.normalize()

    create_and_load(df_all, "openaq_daily", engine, if_exists="replace")



def load_noaa_co2(engine):
    """
    Loads CO2 daily, monthly, annual from NOAACO2.py
    """
    df_daily = co2.get_daily_co2()
    df_monthly = co2.get_monthly_co2()
    df_annual = co2.get_annual_co2()

    create_and_load(df_daily, "noaa_co2_daily_mlo", engine, if_exists="replace")
    create_and_load(df_monthly, "noaa_co2_monthly_mlo", engine, if_exists="replace")
    create_and_load(df_annual, "noaa_co2_annual_mlo", engine, if_exists="replace")


def load_ncdc_ghcn(engine):
    """
    Loads NOAA NCDC CDO daily data (GHCND) for your two stations from NCDCDO.py.
    Your get_data_year(...) already adds a 'station' name column.
    """

    df_msc = ncdc.get_data_year(ncdc.MSCABV, ncdc.MSC)
    df_bwi = ncdc.get_data_year(ncdc.BWIABV, ncdc.BWI)

    df_all = pd.concat([df_msc, df_bwi], ignore_index=True)

    if "date" in df_all.columns:
        df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce")

    create_and_load(df_all, "noaa_ncdc_ghcnd_daily", engine, if_exists="replace")

def load_openaq_locations():
    rows = openaq.get_location_details()
    df = pd.DataFrame(rows)

    df.to_sql(
        "openaq_locations",
        engine,
        schema="public",
        if_exists="replace",
        index=False
    )

    print(f"Loaded {len(df)} OpenAQ locations")

def load_openaq_sensors():
    rows = openaq.get_sensor_details()
    df = pd.DataFrame(rows)

    df.to_sql(
        "openaq_sensors",
        engine,
        schema="public",
        if_exists="replace",
        index=False
    )

    print(f"Loaded {len(df)} OpenAQ sensors")

def load_ncdc_stations():
    rows = ncdc.get_stations()
    df = pd.DataFrame(rows)

    df.to_sql(
        "ncdc_stations",
        engine,
        schema="public",
        if_exists="replace",
        index=False
    )

    print(f"Loaded {len(df)} NCDC stations")


def main():

    # Quick connection test
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        print("DB connection OK")

    # Load everything
    load_openaq(engine)
    load_openaq_locations()
    load_openaq_sensors()
    load_ncdc_stations()
    load_noaa_co2(engine)
    load_ncdc_ghcn(engine)

    print("All loads complete.")


if __name__ == "__main__":
    main()
