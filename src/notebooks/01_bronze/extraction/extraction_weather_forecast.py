# Databricks notebook source
# MAGIC %pip install openmeteo-requests
# MAGIC %pip install requests-cache retry-requests numpy pandas

# COMMAND ----------

import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import json
import time
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath("../../.."))
from utils.bronze_ingestion import load_cities, create_batches

# COMMAND ----------

API_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "wind_direction_10m",
    "wind_speed_10m",
    "shortwave_radiation",
    "precipitation",
    "soil_moisture_0_to_1cm", 
    "surface_pressure",
    "dew_point_2m", 
    "vapour_pressure_deficit",
    "weather_code"
]

FORECAST_DAYS = 16

CITIES_PATH = "/Volumes/clima/01_bronze/location_data/cities.json"
OUTPUT_PATH = "/Volumes/clima/01_bronze/weather_data/weather_forecast"
os.makedirs(OUTPUT_PATH, exist_ok=True)

BATCH_SIZE = 50

RUN_DATE = datetime.now().strftime("%Y_%m_%d")

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

for batch_index, batch in enumerate(city_batches, start=1):

    print(
        f"\nBatch {batch_index}/"
        f"{len(city_batches)}"
    )

    # Geographic coordinates of the batch
    latitudes = [city["latitude"] for city in batch]

    longitudes = [city["longitude"] for city in batch]

    params = {
        "latitude": latitudes,
        "longitude": longitudes,
        "hourly": ",".join(HOURLY_VARIABLES),
        "forecast_days": FORECAST_DAYS,
        "timezone": "auto"
    }

    try:

        # Request
        responses = openmeteo.weather_api(API_URL, params=params)

        print(
            f"Responses received: "
            f"{len(responses)}"
        )

        batch_data = []

        # Processes each city
        for city, response in zip(batch, responses):

            city_name = city["name"]
            latitude = city["latitude"]
            longitude = city["longitude"]

            print(
                f"  Processando: "
                f"{city_name}"
            )

            hourly = response.Hourly()

            # Date/time
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

            # Meteorological variables
            for variable_index, variable in enumerate(HOURLY_VARIABLES):
                column_name = (
                    "soil_moisture"
                    if variable == "soil_moisture_0_to_1cm"
                    else variable
                )
                hourly_data[column_name] = (
                    hourly
                    .Variables(variable_index)
                    .ValuesAsNumpy()
                )

            # City information
            hourly_data["city_name"] = city_name
            hourly_data["latitude"] = latitude
            hourly_data["longitude"] = longitude

            city_dataframe = pd.DataFrame(hourly_data)

            batch_data.append(city_dataframe)

        # Combines the cities from the batch
        batch_dataframe = pd.concat(batch_data, ignore_index=True)

        # File name
        output_file = (
            f"{OUTPUT_PATH}/"
            f"forecast_{RUN_DATE}_"
            f"batch_{batch_index:03d}.json"
        )

        # Save JSON
        batch_dataframe.to_json(
            output_file,
            orient="records",
            date_format="iso",
            force_ascii=False
        )

        print(f"File saved: {output_file}")

        print(
            f"Records: "
            f"{len(batch_dataframe):,}"
        )

    except Exception as error:

        print(
            f"Batch error"
            f"{batch_index}: {error}"
        )

        # Wait before continuing
        time.sleep(60)

    # Short pause between batches
    time.sleep(5)