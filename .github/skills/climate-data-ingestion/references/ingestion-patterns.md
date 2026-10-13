# Ingestion Patterns

## Choose the Reader from Real Files

Inspect representative files and the extraction writer before setting Spark options. The current repository writes Open-Meteo historical and forecast DataFrames using pandas `to_json(orient="records")` (a JSON array/document per file unless `lines=True` is added), writes geocoding candidates as an indented JSON array, and writes air-quality records as a JSON array. The wildfire prototype reads CSV from `nasa_data/raw_nasa`. Validate deployed files; do not assume future files retain these layouts.

- **JSON Lines:** use Spark JSON defaults only when each line is an independent JSON object. Preserve nested structs/arrays and use an explicit or tracked schema policy.
- **Pretty-printed JSON document/array:** use document-aware parsing such as `multiLine=true` for batch where suitable. The current `cities.json` extraction writes an indented JSON array; default JSON Lines parsing is not interchangeable. Confirm Auto Loader support/options for the actual layout before selecting it.
- **CSV:** inspect header, delimiter, quote/escape, encoding, and line structure. Use explicit schema when stable; otherwise define controlled inference and schema tracking. Avoid `inferSchema` on every production run if it causes unstable or expensive reads.
- **Compressed or other formats:** confirm supported extensions/runtime and parser behavior. Fail configuration validation on unsupported formats instead of guessing.

Do not use one parser/schema for all Open-Meteo endpoints. Preserve complete source records and document any structural parsing needed to turn response documents into rows.

## Auto Loader for Recurring Files

Illustrative PySpark shape; implement as a testable function and use configured values:

```python
source = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", source_format)
    .option("cloudFiles.schemaLocation", schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .load(source_path)
)
```

Configure parser-specific options and schema strategy after inspecting source files. Add supported file metadata and execution metadata before writing. Keep schema location and checkpoint location persistent, unique per dataset/environment, and outside ordinary source data.

```python
query = (
    enriched.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(target_table)
)
query.awaitTermination()
```

Adapt API/runtime details to the selected Databricks Runtime. Handle schema evolution as an explicit contract; `addNewColumns` is not a substitute for review of incompatible changes. Ensure the writer's table creation and managed-table behavior match the environment, and fail with actionable diagnostics when permissions or target configuration are invalid.

## One-Time Historical Batch

Use batch Spark reads for a finite backfill or unsuitable Auto Loader layout. Validate source existence and distinguish an existing empty directory from a missing path. Write append-only to Delta. Batch append is not automatically rerun-idempotent: use a durable processed-file ledger/manifest or stable source identity with a deliberate transaction strategy. Do not claim rerun safety based only on append or a dataframe count.

The repository's geocoding extractor currently overwrites one `cities.json` file in place. Auto Loader tracks files and may not notice later changes to that same path, so the geocoding notebook defaults to batch mode. Its default transaction identity is derived from the source path, modification time, and size: an unchanged file is idempotent across reruns, and a changed file gets a new identity. This fingerprint is metadata-based, not a content checksum; supply an explicit execution ID for controlled replay or if file metadata is not reliable. Never reuse an identity for changed contents.

When the source is a whole JSON document/array, parse using an option and schema that match the file shape. Keep the original record content available, particularly where flattening a nested response would otherwise lose units, timezone, or endpoint metadata.

## Traceability and Execution Metadata

Prefer reader-provided metadata such as source file path, name, and modification time where supported. Add an execution ID and ingestion timestamp at a clearly defined scope. For Auto Loader, use supported `_metadata` fields appropriate to the runtime. Avoid collecting all file names on the driver. If an execution-level metric requires querying Auto Loader state or Delta history, use supported system evidence and define its scope accurately.

Keep metadata namespaced and collision-safe. Never substitute file modification time or ingestion time for source observation/acquisition time.

## Malformed Data and Schema Drift

Define permissive/rescue/quarantine behavior per format. Configure corrupt-record handling or Auto Loader rescued data where supported. Keep a malformed-file record with path and diagnostic, and decide whether a bad file fails the run or is isolated based on the contract. Do not silently drop malformed rows. Review rescued data and schema history before promoting new fields or changing target types.

## Write, Measure, and Log

Use append as the default Delta write. Validate format/table identity and permissions. Record write success from the completed streaming query or successful Delta operation; do not equate rows materialized before a write with rows persisted. Avoid expensive full target scans solely to generate run metrics. Include pipeline, dataset, execution, source, target, duration, status, and actionable error detail. Include counts only when their origin and meaning are reliable.

## Invocation Shape

Expose explicit configuration and mode selection rather than embedding paths in code. For example:

```text
ingest(dataset="firms", environment="dev", mode="incremental")
```

For Databricks Jobs, pass these values as task parameters and provision UC objects/permissions through the repository's deployment process. Do not create resources or delete state implicitly from the ingestion task.