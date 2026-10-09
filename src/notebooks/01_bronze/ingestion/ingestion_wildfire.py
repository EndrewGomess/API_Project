# Databricks notebook source
import os
import sys

sys.path.append(os.path.abspath("../../.."))

from utils.bronze_ingestion import project_dataset_config, run_ingestion

# COMMAND ----------

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("catalog", "clima")
dbutils.widgets.text("bronze_schema", "01_bronze")
dbutils.widgets.dropdown("mode", "auto_loader", ["auto_loader", "batch"])
dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("source_path", "")
dbutils.widgets.text("target_table", "")
dbutils.widgets.text("checkpoint_path", "")
dbutils.widgets.text("schema_location", "")

# COMMAND ----------

config = project_dataset_config(
	"firms",
	environment=dbutils.widgets.get("environment"),
	catalog=dbutils.widgets.get("catalog"),
	bronze_schema=dbutils.widgets.get("bronze_schema"),
	mode=dbutils.widgets.get("mode"),
	execution_id=dbutils.widgets.get("execution_id") or None,
	source_path=dbutils.widgets.get("source_path") or None,
	target_table=dbutils.widgets.get("target_table") or None,
	checkpoint_path=dbutils.widgets.get("checkpoint_path") or None,
	schema_location=dbutils.widgets.get("schema_location") or None,
)

result = run_ingestion(spark, dbutils, config)
print(result)