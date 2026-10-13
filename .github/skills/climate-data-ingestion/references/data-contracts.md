# Bronze Data Contracts

Define a source-specific contract for each dataset. The contract records the discovered file layout and parser options, not an assumed API-wide schema. Preserve source field names and nested shape unless a documented technical requirement demands a representation change.

## Source Contract Inventory

For each configured dataset, record:

- Dataset and source system; actual Unity Catalog Volume URI; owning catalog/schema/Volume.
- Observed file extensions, format, compression, encoding, and record layout (JSON Lines, JSON document/array, CSV header/delimiter, or other supported format).
- Parser options, explicit schema or inference policy, nullable/optional fields, and known schema variants.
- Target three-part table name, ingestion mode, checkpoint path, schema tracking path, and replay strategy.
- Catalog/Volume/table permissions and environment in which the contract was verified.

Do not treat the repository's present `clima` bootstrap and extraction paths as deployment guarantees. Verify actual workspace resources and representative files before finalizing this contract.

## Source Fields

Source columns and nested values remain source-faithful. Optional fields may be absent or null. Schema contracts must allow dataset-specific variations, especially among Open-Meteo endpoints and NASA FIRMS products. For geocoding, preserve the complete candidate collection or each candidate as a source record without selecting by name.

When a parser must convert a JSON document/array into rows, document that structural parsing step and preserve the complete source record for each emitted row. For CSV, preserve all available columns and configure header, delimiter, quote/escape, and malformed-record handling based on inspected files.

## Technical Metadata

Add a consistent, namespaced metadata set where supported by the chosen reader/runtime:

| Field | Contract |
|---|---|
| `_ingest_source_file` | Original Volume file path; prefer Spark/Auto Loader file metadata over reconstructing it. |
| `_ingest_file_name` | File name when available and useful for operations. |
| `_ingest_file_modification_time` | Source file modification time when available; do not substitute for source observation time. |
| `_ingest_timestamp` | UTC timestamp when the record was ingested; set once for the run or record processing semantics and document which. |
| `_ingest_execution_id` | Stable identifier for the Databricks job run or generated execution. |
| `_ingest_dataset` | Configured logical dataset name. |
| `_rescued_data` | Captured unexpected or incompatible values when Auto Loader rescued-data support is enabled. |

Avoid collision with source fields. If a source already uses a reserved metadata name, choose a documented namespace or retain the source field separately. Metadata must not replace source timestamps or content.

## Error Records and Reconciliation

Malformed files/records must be isolated or recorded with source path, error category, execution ID, and a safe diagnostic; do not log secrets or full sensitive payloads. Distinguish parser failures, rescued fields, inaccessible sources, Delta write failures, and legitimate source nulls.

Define counts precisely. Examples: files discovered/processed from reader metadata or Auto Loader state; records read; records written, measured from write/target evidence; malformed or rescued records if the implementation can count them reliably. If a count cannot be measured without an unsafe full scan or ambiguous semantics, report it as unavailable rather than estimating it.