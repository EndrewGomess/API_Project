# Production Standards

## Configuration and Environment

Configure environment, catalog, Bronze schema, source Volume URI, format, target table, checkpoint path, schema tracking path, parser/schema options, and ingestion mode outside pipeline logic. Validate configuration before starting a write. Keep dev and production state isolated. Do not assume names found in source code already exist in a Databricks workspace.

Use fully qualified three-part target names. Prefer managed Delta tables registered in Unity Catalog. A Volume directory is a file location, not a Delta table merely because it is in a Volume. Verify target table ownership, existing format, access grants, and path collisions before writing.

## Incrementality and Idempotency

- Prefer Auto Loader for supported recurring ingestion. Use a persistent checkpoint and separate schema tracking location per environment and dataset. Never store either inside an ordinary source-data directory or delete them automatically.
- `trigger(availableNow=True)` is appropriate for scheduled catch-up runs where supported. Use a Databricks Job with a single well-defined streaming query lifecycle.
- With the same valid checkpoint, Auto Loader tracks discovered files and a Delta streaming sink provides restartable processing. Do not generalize this into a guarantee against changed-in-place files, checkpoint loss, manual replay, external side effects, or duplicate records already present in source files.
- For batch historical loads, append alone is not rerun-idempotent. Use Delta idempotent transaction identifiers (`txnAppId`/`txnVersion`) or a durable processed-file manifest/ledger. Reuse the same transaction identity only for a retry of the same input snapshot; changed input requires a new identity. Preserve true source duplicates; do not hash business fields and deduplicate without a contract.
- Before reprocessing, document the affected input interval/files, target impact, backup or isolated target, and a controlled replay procedure. Never overwrite historical data without explicit authorization.

## Delta Writes and Schema Evolution

- Append is the default. Do not use overwrite as routine ingestion behavior.
- Define a compatible additive-evolution policy and capture unexpected fields (for example Auto Loader rescued data) where practical.
- Review schema changes before enabling evolution. Additive fields may be accepted through a documented setting; type conflicts, field removal, incompatible nesting changes, and renames must fail visibly or be quarantined for review, never silently discarded.
- Validate that the target is Delta and that table registration/managed-table behavior matches the requested deployment. Avoid writing files directly into a table's storage directory.

## Failure Handling and Recovery

Catch only errors that can be given meaningful context or recovery behavior. Log exception type, stage, dataset, execution ID, source, target, and remediation; re-raise failures so Databricks Jobs reflect failure. Do not use catch-all suppression.

Document procedures for:

- **Interrupted job:** rerun with the same persistent checkpoint; inspect job, stream, and Delta status before intervention.
- **Malformed file:** isolate/record the file and parser error; correct or quarantine it, then resume or replay using the documented state strategy.
- **Schema change:** inspect rescued fields and schema history; approve compatible evolution or block/quarantine incompatible changes.
- **Missing checkpoint:** stop automatic replay; estimate the input range and target impact, then use controlled replay/ledger safeguards.
- **Partial failure:** inspect the failed stage and Delta commit evidence; retry only through the defined checkpoint or transaction path.
- **Historical reprocessing:** use a planned range, isolated output or explicit approved replacement, reconciliation, and recovery plan. Never delete checkpoints automatically.

## Logging and Monitoring

Emit structured records for pipeline, dataset, execution ID, start/end or duration, source location, target table, status, and error details. Include processed file count, written record count, invalid/rescued count only when each is measured with clear semantics. Record metrics once per execution where possible. Never infer successful persistence solely from a DataFrame count before the write.

## Security and Performance

- Use Unity Catalog grants and least privilege for source Volumes, checkpoint/schema locations, and target tables.
- Do not hardcode credentials. Use Databricks Secrets only when required by the job; ingestion from existing files should not need source API credentials.
- Use Spark-native reads and transformations. Avoid `collect()`/`toPandas()`, repeated full historical scans, unnecessary repartitioning, and unbounded driver-side processing.
- Add partitioning, clustering, or optimization only after validating workload size and query patterns; do not partition reflexively.

## Databricks Jobs

Document the job's notebook/wheel/script entry point, runtime and libraries, parameters, identity/access grants, schedule, concurrency, timeout, retry policy, checkpoint persistence, and alerting. Use bounded retries for transient infrastructure failures; permanent parser/schema/configuration failures should surface promptly. Ensure concurrent runs cannot race on a shared checkpoint or mutable batch ledger.