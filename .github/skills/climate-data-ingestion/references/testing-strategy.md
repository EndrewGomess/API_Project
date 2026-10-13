# Testing Strategy

Use pytest for deterministic unit tests and local PySpark for transformations/read behavior supported by the installed Spark version. Maintain a separate Databricks integration suite for Unity Catalog, Auto Loader, managed tables, access control, and durable checkpoints. Clearly report which tests ran, runtime/environment, and which require a Databricks workspace; do not present workspace-only tests as executed locally.

## Required Coverage

1. Initial ingestion reads fixtures and appends source records to the intended Delta target.
2. Incremental ingestion discovers only new files with persistent checkpoint state.
3. Repeated execution with no new files writes no duplicate records solely due to the rerun.
4. Empty existing source directory completes as a valid no-op with accurate metrics.
5. Missing/unreadable source path fails clearly before an unintended table write.
6. Malformed JSON and CSV are isolated/rescued or recorded according to the contract; ingestion does not silently lose records.
7. Nested Open-Meteo JSON, optional null variables, timezone/units, and distinct endpoint schemas remain preserved.
8. NASA FIRMS schema variants and optional confidence/brightness/FRP columns do not cause valid records to be filtered.
9. Geocoding fixtures preserve multiple candidates, duplicate names, administrative metadata, and source identifiers.
10. Additive schema evolution is captured; incompatible schema changes fail visibly or follow the documented quarantine path.
11. Interrupted execution resumes from the same checkpoint without loss or duplicate processing beyond documented guarantees.
12. Successful target persistence is Delta and registered at the configured fully qualified table name.
13. Technical metadata is present, populated, collision-safe, and traceable to source file and execution.
14. Source-to-target reconciliation matches the defined semantics for read, written, malformed, and rescued records.

## Test Layers

### Unit Tests

Test configuration validation, identifier/path validation, format selection, metadata construction, structured failure context, and metric classification without requiring a live workspace. Use tiny fixtures for JSON Lines, pretty-printed JSON arrays, nested JSON, headered CSV, optional fields, and malformed inputs. Assert raw values and nested content, not just row counts.

### Local Spark Tests

Where local PySpark and Delta dependencies are available, test DataFrame parsing, schema behavior, metadata columns, and Delta append/read behavior in temporary paths. Do not claim this simulates Unity Catalog Volumes or Auto Loader checkpoint semantics.

### Databricks Integration Tests

Run in an explicitly disposable dev catalog/schema and Volume. Verify grants, source accessibility, Auto Loader discovery and restart, persistent schema/checkpoint state, `availableNow` completion, malformed-data handling, managed Delta registration, repeated no-new-file run, and actual job retry/recovery behavior. Clean up only test-owned resources, never shared production checkpoints or tables.

## Reconciliation Assertions

Define what constitutes an input record for each parser. Compare source counts with target writes plus intentionally quarantined/rescued records for the same execution, accounting for JSON arrays and malformed lines. Validate execution/file metadata and the Delta commit. Avoid comparing by business-key distinct counts because Bronze preserves duplicate source observations.

## Reporting

Report exact commands or job runs, pass/fail outcomes, and any skipped Databricks-only cases. When tests were not executed, say so and identify the required workspace/runtime rather than implying successful validation.