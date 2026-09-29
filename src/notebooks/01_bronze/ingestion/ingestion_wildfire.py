# Databricks notebook source
# 1. Read the files from the volume into a Spark dataframe
df = (spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/Volumes/clima/01_bronze/nasa_data/raw_nasa/"))

# 2. Write to the dataframe to a delta table (overwrite the table if it exists)
(df.write.mode("overwrite").saveAsTable(f"clima.{'01_bronze'}.wildfire"))