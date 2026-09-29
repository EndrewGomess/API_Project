# Databricks notebook source
# MAGIC %pip install geobr

# COMMAND ----------

import requests
import json

# COMMAND ----------

from geobr import read_municipality

uf = "SP"
state = "São Paulo"
country_code = "BR"  

mun = read_municipality(code_muni=uf, year=2025)

print(mun[['name_muni', 'abbrev_state']])

cities = mun['name_muni'].head(15).tolist()

print(cities)

# COMMAND ----------

url = "https://geocoding-api.open-meteo.com/v1/search"

results = []

for city in cities:

    params = {
        "name": f"{city}, {uf}, {country_code}",
        "count": 1,
        "language": "pt",
        "format": "json"
    }

    response = requests.get(url, params=params)
    data = response.json()

    api_results = data.get("results", [])

    if api_results:
        result = api_results[0]

        results.append(result)

        print(city, result)
        print()

    else:
        print(f"No valid results found for {city}!")
        print()

# COMMAND ----------

df = results

OUTPUT_PATH = "/Volumes/clima/01_bronze/location_data/cities.json"

with open(OUTPUT_PATH, "w", encoding="utf-8") as file:
    json.dump(df, file, ensure_ascii=False, indent=4)