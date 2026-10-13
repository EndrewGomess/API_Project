---
name: climate-data-ingestion
description: >
  Production-oriented ingestion of Open-Meteo,
  NASA FIRMS, and Geocoding API data from
  Unity Catalog Volumes into Databricks Bronze
  Delta tables using PySpark and Auto Loader.
---

# Climate Data Bronze Ingestion

Create or improve ingestion pipelines that load already-extracted Open-Meteo, NASA FIRMS, and geocoding files from Unity Catalog Volumes into Bronze Delta tables. Do not implement API extraction unless explicitly requested. Keep all generated code and technical documentation in English.

## Use This Skill When

- Building, reviewing, testing, or operating Bronze ingestion for the project's weather, wildfire, air-quality, or geographic source files.
- Moving existing files from Unity Catalog Volumes into managed Delta tables with batch Spark or Auto Loader.
- Adding incremental processing, schema evolution, execution metadata, recovery, or ingestion tests.

## Project Context

This repository uses Medallion Architecture. The environment bootstrap in `src/notebooks/00_environment/creating_catalog_clima.sql` currently names catalog `clima`, schemas `01_bronze`, `02_silver`, and `03_gold`, and Volumes including `weather_data`, `location_data`, and `nasa_data`. Extraction code writes Open-Meteo weather records as pandas JSON arrays, geocoding candidates as an indented JSON array, air-quality records as JSON arrays, and NASA FIRMS source data is read as CSV by the current ingestion prototype. These are repository observations, not proof that any object exists in the target workspace. Inspect the relevant bootstrap, extraction code, actual Volume files, and repository instructions before choosing source paths, formats, schemas, or target names. Do not create or assume catalogs, schemas, Volumes, tables, or checkpoint directories without checking workspace state and deployment permissions.

The current `src/notebooks/01_bronze/ingestion/ingestion_wildfire.py` is a prototype using a hardcoded CSV path and `overwrite`. Do not copy its overwrite behavior: preserve history and use append-based incremental ingestion unless the user explicitly authorizes a controlled replacement.

## Workflow

1. **Inspect the project.** Read relevant repository instructions, the environment/catalog bootstrap, extraction outputs, neighboring ingestion code, configuration, and tests. Identify existing naming and packaging patterns. Do not broadly refactor unrelated extraction code.
2. **Inventory source files.** Confirm Volume paths are accessible and list representative files without exposing secrets. Determine actual format and layout (for example, JSON object-per-line, pretty-printed JSON array, nested JSON, or headered CSV), compression, encoding, and whether files are complete. Empty directories are valid input states; missing configured paths are configuration errors.
3. **Agree on configuration and targets.** Make environment, catalog, Bronze schema, source Volume path, checkpoint, schema tracking path, and target table configurable. Show example values as examples only. Validate Unity Catalog permissions and table existence/ownership behavior before deployment.
4. **Choose the ingestion mode.** Prefer Auto Loader (`cloudFiles`) for recurring immutable file arrivals when the source layout and runtime support it. Use persistent, distinct checkpoint and schema tracking locations. For scheduled runs, consider `trigger(availableNow=True)`. Use batch reads for one-time historical loads, mutable fixed-path snapshots, or formats/layouts for which Auto Loader is not suitable. Explain the rerun and duplicate behavior of the selected mode.
5. **Define the raw contract.** Preserve source fields, nested structure, source timestamps, units, coordinates, nulls, and anomalies. Add only technical metadata required for traceability and ingestion. Never apply business filters, joins, deduplication, aggregation, coordinate rounding, or timestamp normalization in Bronze.
6. **Implement a small, testable pipeline.** Separate configuration validation, source read, technical metadata, Delta write, metrics/logging, and error handling into reusable functions following repository conventions. Use Spark-native processing, type hints, structured logs, and actionable errors. Keep writes append-only by default; isolate malformed or rescued data for investigation without silently dropping it.
7. **Handle schema changes deliberately.** Define initial schema/inference strategy, schema tracking, additive evolution behavior, rescued data handling, and incompatible-change response. Never silently discard unexpected fields. See [ingestion patterns](./references/ingestion-patterns.md).
8. **Validate results.** Verify target is Delta, the write succeeded, expected technical metadata is populated, source files reconcile with written/quarantined records where measurable, and no unintended overwrite occurred. Distinguish ingestion/technical failures from business-quality anomalies.
9. **Test locally and in Databricks.** Use pytest and local PySpark for logic and supported format cases; use a Databricks integration suite for Unity Catalog Volumes, Auto Loader, checkpoints, permissions, and managed Delta persistence. State which tests were actually executed and which require a workspace.
10. **Document operations and acceptance.** Provide configuration examples, invocation, job dependencies/retries, monitoring fields, recovery procedures, and acceptance criteria. Do not claim metrics or exactly-once guarantees that the implementation cannot demonstrate.

## Required References

- Dataset rules: [business rules](./references/business-rules.md)
- Raw schema and metadata: [data contracts](./references/data-contracts.md)
- Security, reliability, deployment, and operations: [production standards](./references/production-standards.md)
- Source-format selection and implementation patterns: [ingestion patterns](./references/ingestion-patterns.md)
- Unit and workspace test coverage: [testing strategy](./references/testing-strategy.md)

## Example Configuration

Treat these as illustrative configuration values, not pre-created workspace resources:

```yaml
environment: dev
catalog: clima
bronze_schema: 01_bronze
datasets:
  firms:
    source_path: /Volumes/clima/01_bronze/nasa_data/raw_nasa
    source_format: csv
    target_table: clima.01_bronze.wildfire
    checkpoint_path: /Volumes/clima/01_bronze/_checkpoints/firms
    schema_location: /Volumes/clima/01_bronze/_schemas/firms
```

Use environment-specific configuration and validate all paths, table identifiers, formats, and permissions at startup. Never place credentials in configuration or code.

## Example Invocation

Run the matching Databricks notebook as a Job task and pass its widgets as task parameters:

```text
src/notebooks/01_bronze/ingestion/ingestion_openmeteo.py
  dataset=weather_forecast
  environment=dev
  mode=auto_loader

src/notebooks/01_bronze/ingestion/ingestion_geocoding.py
  environment=dev
  mode=batch
  execution_id=<optional stable replay identity>

src/notebooks/01_bronze/ingestion/ingestion_wildfire.py
  environment=dev
  mode=auto_loader
```

The Open-Meteo notebook accepts `weather_historical`, `weather_forecast`, and `air_quality`. The geocoding notebook defaults to batch mode because the current extractor overwrites one file; when no execution ID is supplied, it derives one from that file's path, modification time, and size. Pass an explicit ID to replay the same unchanged snapshot after failure. Provision and verify managed Delta target tables and Unity Catalog permissions before running.

## Acceptance Criteria

- Reads the existing configured Volume files and does not call source APIs.
- Preserves every source record and source field, including nested content, nulls, anomalies, original timestamps, and geographic precision.
- Adds file and execution traceability, and writes append-only Delta data unless explicit authorization defines another safe operation.
- Supports the selected one-time or incremental mode, restart behavior, persistent state, and a documented replay strategy.
- Handles malformed data and additive/incompatible schema changes explicitly; no unexpected fields are silently lost.
- Validates accessibility, format, write outcome, metadata, and measurable reconciliation; logs only reliable metrics.
- Has focused pytest coverage plus clearly identified Databricks-only integration coverage.
- Documents environment configuration, invocation, operational dependencies, recovery, and unexecuted checks.