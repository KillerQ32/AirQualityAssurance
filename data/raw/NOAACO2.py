import pandas as pd

DAILY_URL = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.csv"
MONTHLY_URL = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_mm_mlo.csv"
ANNUAL_URL = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_annmean_mlo.csv"

def get_monthly_co2():
    df = pd.read_csv(
        MONTHLY_URL,
        comment="#",      # skips metadata lines
        skip_blank_lines=True
    )

    df["date"] = pd.to_datetime(
        df["year"].astype(str) + "-" +
        df["month"].astype(str).str.zfill(2)
    )

    df["unit"] = "ppm"
    df.drop(columns=["year", "month", "decimal date"], inplace=True, errors="raise")

    return df

def get_daily_co2():
    df = pd.read_csv(
        DAILY_URL,
        comment="#",
        skip_blank_lines=True,
        header=None,
        names=["year", "month", "day", "decimal_date", "co2_ppm"]
    )

    df["date"] = pd.to_datetime(dict(year=df["year"], month=df["month"], day=df["day"]))
    df["unit"] = "ppm"
    df = df.rename(columns={"co2_ppm": "co2"})

    df.drop(columns=["year", "month", "day", "decimal_date"], inplace=True)

    return df

def get_annual_co2():
    df = pd.read_csv(
        ANNUAL_URL,
        comment="#",
        skip_blank_lines=True
    )

    df["date"] = pd.to_datetime(
        df["year"].astype(str)
    )
    
    return df

get_annual_co2()