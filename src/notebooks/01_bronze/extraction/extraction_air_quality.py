# Databricks notebook source
# MAGIC %pip install openmeteo-requests
# MAGIC %pip install requests-cache retry-requests numpy pandas

# COMMAND ----------

import json
import os
import time
from datetime import datetime, timedelta, timezone
import sys

import requests
import requests_cache
from retry_requests import retry

sys.path.append(os.path.abspath("../../.."))
from utils.bronze_ingestion import load_cities, create_batches

# COMMAND ----------

from datetime import datetime, timedelta

API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

CITIES_FILE = "/Volumes/clima/01_bronze/location_data/cities.json"

OUTPUT_PATH = "/Volumes/clima/01_bronze/air_quality/data"
os.makedirs(OUTPUT_PATH, exist_ok=True)

START_DATE = datetime.now().strftime("%Y-%m-%d")
END_DATE = (datetime.now() + timedelta(days=5)).strftime("%Y-%m-%d")

BATCH_SIZE = 50

REQUEST_DELAY = 1

# COMMAND ----------

HOURLY_VARIABLES = [
    "pm10",
    "pm2_5",

    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",

    "us_aqi",
]


# COMMAND ----------

cache_session = requests_cache.CachedSession(".cache_air_quality", expire_after=3600)

retry_session = retry(cache_session, retries=5, backoff_factor=0.2)

# COMMAND ----------

cities = load_cities(CITIES_FILE)
print(f"Total number of cities: {len(cities)}")

# COMMAND ----------

city_batches = list(create_batches(cities, BATCH_SIZE))
print(f"Total number of batches: {len(city_batches)}")

# COMMAND ----------

def generate_dates(start_date, end_date):
    current = datetime.strptime(start_date, "%Y-%m-%d")

    end = datetime.strptime(end_date, "%Y-%m-%d")

    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)

# COMMAND ----------

def save_json(data, date, batch_number):

    year = date[:4]
    month = date[5:7]

    # Year/month only
    output_dir = (
        f"{OUTPUT_PATH}/"
        f"{year}/"
        f"{month}"
    )

    os.makedirs(
        output_dir,
        exist_ok=True
    )

    file_name = (
        f"air_quality_"
        f"{date.replace('-', '')}_"
        f"batch_{batch_number:03d}.json"
    )

    file_path = (
        f"{output_dir}/"
        f"{file_name}"
    )

    with open(
        file_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            data,
            file,
            ensure_ascii=False
        )

    return file_path

# COMMAND ----------

def normalize_api_response(
    api_data,
    batch,
    date,
    batch_number
):

    normalized_data = []

    ingestion_timestamp = (
        datetime.now(timezone.utc)
        .isoformat()
    )

    for location_index, location_data in enumerate(api_data):

        hourly = location_data.get("hourly", {})

        times = hourly.get("time", [])

        for hour_index, timestamp in enumerate(times):

            record = {

                "location_id": location_index,

                "latitude": location_data.get("latitude"),

                "longitude": location_data.get("longitude"),

                "elevation": location_data.get("elevation"),

                "timezone": location_data.get("timezone"),

                "timezone_abbreviation": location_data.get("timezone_abbreviation"),

                "utc_offset_seconds": location_data.get("utc_offset_seconds"),

                "generationtime_ms": location_data.get("generationtime_ms"),

                "time": timestamp
            }

            # Adds all hourly variables
            for variable in HOURLY_VARIABLES:

                values = hourly.get(variable, [])

                if hour_index < len(values):

                    record[variable] = values[hour_index]

                else:
                    record[variable] = None

            # Ingestion metadata
            record["_ingestion_metadata"] = {

                "source": "Open-Meteo",

                "endpoint": API_URL,

                "dataset": "air_quality",

                "requested_date": date,

                "batch_number": batch_number,

                "ingestion_timestamp": (
                    ingestion_timestamp
                ),

                "city": batch[location_index]["name"]
            }

            normalized_data.append(record)

    return normalized_data

# COMMAND ----------

for date in generate_dates(
    START_DATE,
    END_DATE
):

    print(" ")
    print(f"DATE: {date}")
    print(" ")

    for batch_number, batch in enumerate(
        city_batches,
        start=1
    ):

        print(
            f"\nProcessing batch "
            f"{batch_number}/"
            f"{len(city_batches)}"
        )

        latitudes = [
            str(city["latitude"])
            for city in batch
        ]

        longitudes = [
            str(city["longitude"])
            for city in batch
        ]

        params = {

            "latitude": ",".join(latitudes),

            "longitude": ",".join(longitudes),

            "hourly": ",".join(HOURLY_VARIABLES),

            "start_date": START_DATE,

            "end_date": END_DATE,

            "timezone": "auto",

            "domains": "auto"
        }

        try:

            response = retry_session.get(
                API_URL,
                params=params,
                timeout=120
            )

            response.raise_for_status()

            api_data = response.json()

            # Normalizes the response
            normalized_data = normalize_api_response(
                api_data,
                batch,
                date,
                batch_number
            )

            # Saves the already normalized JSON
            file_path = save_json(
                normalized_data,
                date,
                batch_number
            )

            print(f"OK - {file_path}")

            print(
                f"Saved records: "
                f"{len(normalized_data)}"
            )

        except Exception as error:

            print(
                f"Batch error"
                f"{batch_number}: "
                f"{error}"
            )

        time.sleep(
            REQUEST_DELAY
        )


print("\Ingestion complete.")