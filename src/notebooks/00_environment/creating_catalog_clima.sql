-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.sql("CREATE CATALOG IF NOT EXISTS clima;")
-- MAGIC
-- MAGIC spark.sql(""" COMMENT ON CATALOG clima IS 'Catalog containing the climate, environmental, and predictive analytics data for the project.' """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("USE CATALOG clima;")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS 01_bronze;")
-- MAGIC spark.sql(""" COMMENT ON SCHEMA clima.01_bronze IS 'Bronze layer containing raw data ingested from external data sources.' """)
-- MAGIC  
-- MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS 02_silver;")
-- MAGIC spark.sql(""" COMMENT ON SCHEMA clima.02_silver IS 'Silver layer containing cleaned, standardized, and transformed data.' """)
-- MAGIC
-- MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS 03_gold;")
-- MAGIC spark.sql(""" COMMENT ON SCHEMA clima.03_gold IS 'Gold layer containing curated data prepared for analytics, dashboards, and machine learning.' """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("USE SCHEMA 01_bronze;")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("CREATE VOLUME IF NOT EXISTS air_quality;")
-- MAGIC spark.sql(""" COMMENT ON VOLUME clima.01_bronze.air_quality IS 'Stores raw air quality data retrieved from the Open-Meteo API.' """)
-- MAGIC
-- MAGIC spark.sql("CREATE VOLUME IF NOT EXISTS location_data;")
-- MAGIC spark.sql(""" COMMENT ON VOLUME clima.01_bronze.location_data IS 'Stores raw location and geographic data used by the project.' """)
-- MAGIC
-- MAGIC spark.sql("CREATE VOLUME IF NOT EXISTS nasa_data;")
-- MAGIC spark.sql(""" COMMENT ON VOLUME clima.01_bronze.nasa_data IS 'Stores raw data retrieved from NASA data sources used by the project.' """)
-- MAGIC
-- MAGIC spark.sql("CREATE VOLUME IF NOT EXISTS weather_data;")
-- MAGIC spark.sql(""" COMMENT ON VOLUME clima.01_bronze.weather_data IS 'Stores raw weather data retrieved from the Open-Meteo API.' """)