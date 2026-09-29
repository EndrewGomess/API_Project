# Databricks notebook source
# MAGIC %pip install openmeteo-requests
# MAGIC %pip install requests-cache retry-requests numpy pandas
# MAGIC
# MAGIC

# COMMAND ----------

import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import json
import time
import sys
import os

sys.path.append(os.path.abspath("../../.."))
from utils.bronze_ingestion import create_batches, load_cities
from datetime import date, timedelta

# COMMAND ----------

API_URL = "https://archive-api.open-meteo.com/v1/archive"

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "wind_direction_10m",
    "wind_speed_10m",
    "shortwave_radiation",
    "precipitation",
    "soil_moisture_0_to_7cm",
    "surface_pressure",
    "dew_point_2m", 
    "vapour_pressure_deficit",
    "weather_code"
]

CITIES_PATH = "/Volumes/clima/01_bronze/location_data/cities.json"
OUTPUT_PATH = "/Volumes/clima/01_bronze/weather_data/weather_historical"
os.makedirs(OUTPUT_PATH, exist_ok=True)

START_DATE = "2020-01-01"
END_DATE = (date.today() - timedelta(days=1)).isoformat()

START_YEAR = 2020
END_YEAR = date.today().year

BATCH_SIZE = 50

# COMMAND ----------

cache_session = requests_cache.CachedSession(".cache", expire_after=-1)

retry_session = retry(cache_session, retries=5, backoff_factor=0.2)

openmeteo = openmeteo_requests.Client(session=retry_session)

# COMMAND ----------

cities = load_cities(CITIES_PATH)
print(f"Total number of cities: {len(cities)}")

# COMMAND ----------

city_batches = list(create_batches(cities, BATCH_SIZE))
print(f"Total number of batches: {len(city_batches)}")

# COMMAND ----------

for year in range(START_YEAR, END_YEAR + 1):

    year_start = pd.Timestamp(f"{year}-01-01")
    year_end = pd.Timestamp(f"{year}-12-31")

    #Don't let yesterday overtake you
    global_end = pd.Timestamp(END_DATE)

    if year_end > global_end:
        year_end = global_end

    # Year completely out of range
    if year_start > global_end:
        break

    print("")
    print(f"YEAR: {year}")

    current_start = year_start

    while current_start <= year_end:

        # Period of approximately 3 months
        current_end = current_start + pd.DateOffset(months=3) - pd.Timedelta(days=1)

        if current_end > year_end:
            current_end = year_end

        start_date = current_start.strftime("%Y-%m-%d")
        end_date = current_end.strftime("%Y-%m-%d")

        print("")
        print(
            f"Period: {start_date} → {end_date}"
        )

        for batch_index, batch in enumerate(city_batches, start=1):

            print(
                f"\nBatch {batch_index}/"
                f"{len(city_batches)}"
            )

            latitudes = [
                city["latitude"]
                for city in batch
            ]

            longitudes = [
                city["longitude"]
                for city in batch
            ]

            params = {
                "latitude": latitudes,
                "longitude": longitudes,
                "start_date": start_date,
                "end_date": end_date,
                "hourly": ",".join(HOURLY_VARIABLES),
                "timezone": "auto"
            }

            try:

                responses = openmeteo.weather_api(API_URL, params=params)

                print(
                    f"Responses received: "
                    f"{len(responses)}"
                )

                batch_data = []

                for city, response in zip(
                    batch,
                    responses
                ):

                    city_name = city["name"]
                    latitude = city["latitude"]
                    longitude = city["longitude"]

                    print(
                        f" Processing: "
                        f"{city_name}"
                    )

                    hourly = response.Hourly()

                    hourly_data = {
                        "date": pd.date_range(
                            start=pd.to_datetime(
                                hourly.Time(),
                                unit="s"
                            ),
                            end=pd.to_datetime(
                                hourly.TimeEnd(),
                                unit="s"
                            ),
                            freq=pd.Timedelta(
                                seconds=hourly.Interval()
                            ),
                            inclusive="left"
                        )
                    }

                    for variable_index, variable in enumerate(HOURLY_VARIABLES):
                        column_name = (
                            "soil_moisture"
                            if variable == "soil_moisture_0_to_7cm"
                            else variable
                        )

                        hourly_data[column_name] = (
                            hourly
                            .Variables(variable_index)
                            .ValuesAsNumpy()
                        )

                    hourly_data["city_name"] = city_name
                    hourly_data["latitude"] = latitude
                    hourly_data["longitude"] = longitude

                    city_dataframe = pd.DataFrame(
                        hourly_data
                    )

                    batch_data.append(
                        city_dataframe
                    )

                batch_dataframe = pd.concat(
                    batch_data,
                    ignore_index=True
                )

                output_file = (
                    f"{OUTPUT_PATH}/"
                    f"historical_{year}_"
                    f"{start_date}_{end_date}_"
                    f"batch_{batch_index:03d}.json"
                )

                batch_dataframe.to_json(
                    output_file,
                    orient="records",
                    date_format="iso",
                    force_ascii=False
                )

                print(
                    f"File saved: "
                    f"{output_file}"
                )

                print(
                    f"Records: "
                    f"{len(batch_dataframe):,}"
                )

            except Exception as error:

                print(
                    f"Batch error "
                    f"{batch_index}: {error}"
                )

                time.sleep(60)

            time.sleep(5)

        # Next period
        current_start = current_end + pd.Timedelta(days=1)