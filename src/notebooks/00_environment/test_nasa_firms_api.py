# Databricks notebook source
import requests
from pyspark.sql import SparkSession

# COMMAND ----------

api_key = dbutils.secrets.get(
    scope="apinasakey",
    key="nasakey"
)

if api_key:
    print("API key successfully retrieved from Secret Scope.")
else:
    print("API key was not retrieved.")

# COMMAND ----------

source = "VIIRS_SNPP_NRT"
area_coord = "-53,-25,-44,-19"
day_range = 1

url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{api_key}/{source}/{area_coord}/{day_range}"

# COMMAND ----------

response = requests.get(url)

if response.status_code == 200:
    print(response.text)
else:
    print(f"API Error: {response.status_code}")
    print(response.text)

# COMMAND ----------

print(len(api_key))
print(api_key[:4])