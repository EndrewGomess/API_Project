# Databricks notebook source
import hashlib
import os
import sys

sys.path.append(os.path.abspath("../../.."))

from utils.bronze_ingestion import project_dataset_config, run_ingestion

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("catalog", "clima")
dbutils.widgets.text("bronze_schema", "01_bronze")
dbutils.widgets.dropdown("mode", "batch", ["batch", "auto_loader"])
dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("source_path", "")
dbutils.widgets.text("target_table", "")
dbutils.widgets.text("checkpoint_path", "")
dbutils.widgets.text("schema_location", "")

# COMMAND ----------

source_path = dbutils.widgets.get("source_path") or None
execution_id = dbutils.widgets.get("execution_id") or None

if execution_id is None and dbutils.widgets.get("mode") == "batch":
    resolved_source = source_path or "/Volumes/clima/01_bronze/location_data"
    source_files = [
        entry
        for entry in dbutils.fs.ls(resolved_source)
        if entry.name == "cities.json"
    ]
    source_fingerprint = "|".join(
        f"{entry.path}:{entry.modificationTime}:{entry.size}"
        for entry in sorted(source_files, key=lambda item: item.path)
    )
    execution_id = "geocoding_" + hashlib.sha256(
        source_fingerprint.encode("utf-8")
    ).hexdigest()

config = project_dataset_config(
    "geocoding",
    environment=dbutils.widgets.get("environment"),
    catalog=dbutils.widgets.get("catalog"),
    bronze_schema=dbutils.widgets.get("bronze_schema"),
    mode=dbutils.widgets.get("mode"),
    execution_id=execution_id,
    source_path=source_path,
    target_table=dbutils.widgets.get("target_table") or None,
    checkpoint_path=dbutils.widgets.get("checkpoint_path") or None,
    schema_location=dbutils.widgets.get("schema_location") or None,
)

result = run_ingestion(spark, dbutils, config)
print(result)