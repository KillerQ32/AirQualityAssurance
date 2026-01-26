import os
import pandas as pd
from dotenv import load_dotenv
from openaq import OpenAQ

load_dotenv()
client = OpenAQ(api_key=os.getenv("OPENAQ_KEY"))

BALTIMORE = (39.2904, -76.6122)
BALTIMORE_COUNTY_CENTER = (39.4015, -76.6019)


locations = client.locations.list(coordinates=BALTIMORE_COUNTY_CENTER, radius=20_000, limit=1000)

rows = []

for loc in locations.results:
    # sensors are already here:
    for s in (loc.sensors or []):
        # example: only PM2.5
        if s.parameter and s.parameter.name == "pm25":
            meas = client.measurements.list(
                sensors_id=s.id,
                datetime_from="2015-01-01",
                datetime_to="2025-01-01",
                limit=1000
            )
            for r in meas.dict().get("results", []):
                r["location_id"] = loc.id
                r["location_name"] = loc.name
                r["parameter"] = s.parameter.name
                r["parameter units"] = s.parameter.units

                rows.append(r)

client.close()

df = pd.DataFrame(rows)
print(df.head())
print("rows:", len(df))
df.to_excel("openaq.xlsx")