# Bronze Business Rules

Bronze is the historical, source-faithful landing layer. Apply only technical changes needed to parse and persist records and attach ingestion metadata. Preserve source anomalies and legitimate nulls; do not turn business-quality expectations into ingestion filters.

## Open-Meteo

- Preserve all weather observations and forecast records available in the source, including records with missing optional measurements.
- Retain coordinates, observation/forecast timestamps, meteorological variables, units, timezone metadata, and forecast generation/model metadata when supplied.
- Preserve original nested JSON when flattening would discard information. Endpoints and forecast/historical products can have different schemas; do not impose one universal source schema.
- Do not convert source timestamps to UTC, truncate time precision, replace timezone metadata, or infer missing values in Bronze.

## NASA FIRMS

- Preserve all valid source observations and available latitude, longitude, acquisition date/time, satellite, instrument, confidence, brightness, and fire radiative power fields.
- Do not filter by confidence or brightness thresholds. A thermal anomaly is not automatically a confirmed wildfire.
- Keep schema variants and optional fields. Do not require a particular set of columns across every product/satellite file.

## Geocoding

- Preserve location names, country, administrative regions, coordinates, timezone, and source identifiers whenever present.
- Keep all candidate locations returned by the source. Names are not globally unique; do not select a single match or deduplicate by name during Bronze ingestion.

## Geographic and Temporal Rules

- Preserve source coordinates without rounding, geospatial bucketing, or aggregation.
- Do not associate FIRMS points with cities in Bronze. Perform spatial matching in Silver using documented criteria.
- Retain source timestamps and timezone information exactly as represented. Create standardized UTC values in Silver when required.

## Traceability and History

- Every ingested record must be traceable to its original source file and ingestion execution.
- Retain historical records even if later files overlap in time. Do not overwrite history or remove duplicates based only on business columns; repeated source records may be meaningful evidence.
- Technical replay prevention is allowed when keyed to ingestion state or stable source identity and documented. It must not silently discard distinct source records.

## Prohibited Bronze Operations

- Business filtering, thresholding, normalization, enrichment, joins, deduplication, aggregation, or spatial/temporal standardization.
- Silent dropping of malformed records, unknown fields, nulls, or schema-variant data.
- Destructive overwrite or automatic deletion of source, checkpoint, schema tracking, or historical target data.