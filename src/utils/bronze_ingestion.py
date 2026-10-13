import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any, Generator, List, Mapping, Optional


LOGGER = logging.getLogger(__name__)

SUPPORTED_FORMATS = {"json", "csv"}
SUPPORTED_LAYOUTS = {"json_lines", "json_document_array", "csv_header"}
SUPPORTED_MODES = {"auto_loader", "batch"}
INGESTION_METADATA_COLUMNS = {
    "_ingest_source_file",
    "_ingest_file_name",
    "_ingest_file_modification_time",
    "_ingest_timestamp",
    "_ingest_execution_id",
    "_ingest_dataset",
    "_rescued_data",
}


class IngestionConfigurationError(ValueError):
    """Raised when an ingestion configuration is invalid."""


@dataclass(frozen=True)
class DatasetConfig:
    """Configuration for one raw dataset ingestion."""

    dataset: str
    environment: str
    source_path: str
    source_format: str
    layout: str
    target_table: str
    checkpoint_path: str
    schema_location: str
    mode: str = "auto_loader"
    path_glob_filter: str = "*"
    reader_options: Mapping[str, str] = field(default_factory=dict)
    execution_id: Optional[str] = None

    def validate(self) -> None:
        """Validate identifiers, supported options, and state-path separation."""
        required_values = {
            "dataset": self.dataset,
            "environment": self.environment,
            "source_path": self.source_path,
            "target_table": self.target_table,
            "checkpoint_path": self.checkpoint_path,
            "schema_location": self.schema_location,
        }
        missing = [name for name, value in required_values.items() if not value]
        if missing:
            raise IngestionConfigurationError(
                f"Missing required configuration values: {', '.join(missing)}"
            )

        if self.source_format not in SUPPORTED_FORMATS:
            raise IngestionConfigurationError(
                f"Unsupported source format {self.source_format!r}; "
                f"expected one of {sorted(SUPPORTED_FORMATS)}."
            )
        if self.layout not in SUPPORTED_LAYOUTS:
            raise IngestionConfigurationError(
                f"Unsupported source layout {self.layout!r}; "
                f"expected one of {sorted(SUPPORTED_LAYOUTS)}."
            )
        if self.mode not in SUPPORTED_MODES:
            raise IngestionConfigurationError(
                f"Unsupported ingestion mode {self.mode!r}; "
                f"expected one of {sorted(SUPPORTED_MODES)}."
            )
        if self.source_format == "csv" and self.layout != "csv_header":
            raise IngestionConfigurationError("CSV sources must use the csv_header layout.")
        if self.source_format == "json" and self.layout == "csv_header":
            raise IngestionConfigurationError("JSON sources cannot use the csv_header layout.")

        table_parts = self.target_table.split(".")
        if len(table_parts) != 3 or any(
            not re.fullmatch(r"[A-Za-z0-9_]+", part) for part in table_parts
        ):
            raise IngestionConfigurationError(
                "target_table must be a three-part catalog.schema.table identifier "
                "containing only letters, digits, and underscores."
            )

        source = PurePosixPath(self.source_path.rstrip("/"))
        checkpoint = PurePosixPath(self.checkpoint_path.rstrip("/"))
        schema = PurePosixPath(self.schema_location.rstrip("/"))
        if _is_same_or_child_path(checkpoint, source) or _is_same_or_child_path(
            schema, source
        ):
            raise IngestionConfigurationError(
                "Checkpoint and schema locations must not be inside the source path."
            )
        if checkpoint == schema or _is_same_or_child_path(checkpoint, schema) or (
            _is_same_or_child_path(schema, checkpoint)
        ):
            raise IngestionConfigurationError(
                "Checkpoint and schema locations must be separate paths."
            )
        if self.mode == "batch" and not self.execution_id:
            raise IngestionConfigurationError(
                "Batch mode requires an explicit stable execution_id so a retry can be "
                "recognized. Reuse that ID only for the same batch attempt."
            )


def _is_same_or_child_path(candidate: PurePosixPath, parent: PurePosixPath) -> bool:
    return candidate == parent or parent in candidate.parents


def quote_table_identifier(table_name: str) -> str:
    """Quote a validated three-part Unity Catalog table identifier."""
    parts = table_name.split(".")
    if len(parts) != 3 or any(not re.fullmatch(r"[A-Za-z0-9_]+", part) for part in parts):
        raise IngestionConfigurationError(
            "Table name must be a three-part catalog.schema.table identifier."
        )
    return ".".join(f"`{part}`" for part in parts)


def project_dataset_config(
    dataset: str,
    *,
    environment: str,
    catalog: str = "clima",
    bronze_schema: str = "01_bronze",
    source_path: Optional[str] = None,
    target_table: Optional[str] = None,
    checkpoint_path: Optional[str] = None,
    schema_location: Optional[str] = None,
    mode: Optional[str] = None,
    execution_id: Optional[str] = None,
) -> DatasetConfig:
    """Build a configuration from the repository's current extraction layout.

    Defaults reflect checked-in source code only. The caller must validate that the
    corresponding Unity Catalog objects and paths exist in the target workspace.
    """
    definitions = {
        "weather_historical": {
            "volume": "weather_data",
            "source_subpath": "weather_historical",
            "format": "json",
            "layout": "json_document_array",
            "glob": "*.json",
        },
        "weather_forecast": {
            "volume": "weather_data",
            "source_subpath": "weather_forecast",
            "format": "json",
            "layout": "json_document_array",
            "glob": "*.json",
        },
        "air_quality": {
            "volume": "air_quality",
            "source_subpath": "data",
            "format": "json",
            "layout": "json_document_array",
            "glob": "*.json",
        },
        "geocoding": {
            "volume": "location_data",
            "source_subpath": "",
            "format": "json",
            "layout": "json_document_array",
            "glob": "cities.json",
            "default_mode": "batch",
            "state_volume": "weather_data",
        },
        "firms": {
            "volume": "nasa_data",
            "source_subpath": "raw_nasa",
            "format": "csv",
            "layout": "csv_header",
            "glob": "*.csv",
        },
    }
    if dataset not in definitions:
        raise IngestionConfigurationError(
            f"Unknown dataset {dataset!r}; choose one of {sorted(definitions)}."
        )

    definition = definitions[dataset]
    selected_mode = mode or definition.get("default_mode", "auto_loader")
    volume_root = f"/Volumes/{catalog}/{bronze_schema}/{definition['volume']}"
    source_default = volume_root
    if definition["source_subpath"]:
        source_default = f"{volume_root}/{definition['source_subpath']}"

    state_volume = definition.get("state_volume", definition["volume"])
    state_root = (
        f"/Volumes/{catalog}/{bronze_schema}/{state_volume}/"
        f"_ingestion_state/{environment}/{dataset}"
    )
    config = DatasetConfig(
        dataset=dataset,
        environment=environment,
        source_path=source_path or source_default,
        source_format=definition["format"],
        layout=definition["layout"],
        target_table=target_table or f"{catalog}.{bronze_schema}.{dataset if dataset != 'firms' else 'wildfire'}",
        checkpoint_path=checkpoint_path or f"{state_root}/checkpoint",
        schema_location=schema_location or f"{state_root}/schema",
        mode=selected_mode,
        path_glob_filter=definition["glob"],
        reader_options={"header": "true"}
        if definition["format"] == "csv"
        else {},
        execution_id=execution_id,
    )
    config.validate()
    return config


def _log_event(level: int, event: str, **details: Any) -> None:
    LOGGER.log(level, json.dumps({"event": event, **details}, sort_keys=True))


def _validate_source_directory(dbutils: Any, config: DatasetConfig) -> bool:
    try:
        entries = dbutils.fs.ls(config.source_path)
    except Exception as error:
        raise FileNotFoundError(
            f"Cannot access configured source path {config.source_path!r}: {error}"
        ) from error
    return bool(entries)


def _validate_delta_target(spark: Any, config: DatasetConfig) -> str:
    quoted_target = quote_table_identifier(config.target_table)
    if not spark.catalog.tableExists(quoted_target):
        raise IngestionConfigurationError(
            f"Target table {config.target_table!r} does not exist. Provision the "
            "managed Delta table and its Unity Catalog grants before running ingestion."
        )
    detail = spark.sql(f"DESCRIBE DETAIL {quoted_target}").first()
    if detail is None or str(detail["format"]).lower() != "delta":
        raise IngestionConfigurationError(
            f"Target table {config.target_table!r} is not a Delta table."
        )
    return quoted_target


def _add_ingestion_metadata(dataframe: Any, config: DatasetConfig, execution_id: str) -> Any:
    from pyspark.sql import functions as F

    collisions = sorted(
        (INGESTION_METADATA_COLUMNS - {"_rescued_data"}).intersection(
            dataframe.columns
        )
    )
    if collisions:
        raise IngestionConfigurationError(
            "Source fields conflict with reserved ingestion metadata columns: "
            f"{', '.join(collisions)}. Preserve/rename them explicitly before ingestion."
        )

    return (
        dataframe.withColumn("_ingest_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_file_name", F.col("_metadata.file_name"))
        .withColumn(
            "_ingest_file_modification_time",
            F.col("_metadata.file_modification_time"),
        )
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_ingest_execution_id", F.lit(execution_id))
        .withColumn("_ingest_dataset", F.lit(config.dataset))
    )


def _read_options(config: DatasetConfig) -> dict[str, str]:
    options = dict(config.reader_options)
    options["pathGlobFilter"] = config.path_glob_filter
    options["mode"] = "FAILFAST"
    if config.layout == "json_document_array":
        options["multiLine"] = "true"
    return options


def _run_auto_loader(
    spark: Any,
    config: DatasetConfig,
    quoted_target: str,
    execution_id: str,
) -> None:
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", config.source_format)
        .option("cloudFiles.schemaLocation", config.schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.inferColumnTypes", "true")
    )
    for name, value in _read_options(config).items():
        reader = reader.option(name, value)

    source = reader.load(config.source_path)
    enriched = _add_ingestion_metadata(source, config, execution_id)
    query = (
        enriched.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(quoted_target)
    )
    query.awaitTermination()


def _run_batch(
    spark: Any,
    config: DatasetConfig,
    quoted_target: str,
    execution_id: str,
) -> None:
    reader = spark.read.format(config.source_format)
    for name, value in _read_options(config).items():
        reader = reader.option(name, value)
    if config.source_format == "csv":
        reader = reader.option("inferSchema", "true")
    if config.source_format == "json":
        reader = reader.option("columnNameOfCorruptRecord", "_corrupt_record")

    source = reader.load(config.source_path)
    enriched = _add_ingestion_metadata(source, config, execution_id)
    (
        enriched.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .option("txnAppId", f"{config.environment}:{config.dataset}:{execution_id}")
        .option("txnVersion", 0)
        .saveAsTable(quoted_target)
    )


def run_ingestion(spark: Any, dbutils: Any, config: DatasetConfig) -> dict[str, Any]:
    """Ingest configured files into an existing managed Delta table.

    Auto Loader uses its persistent checkpoint for file-level incremental progress.
    Batch retries must reuse the explicit execution ID. A changed-in-place source
    file is not guaranteed to be rediscovered by Auto Loader.
    """
    config.validate()
    execution_id = config.execution_id or str(uuid.uuid4())
    started_at = time.monotonic()
    context = {
        "dataset": config.dataset,
        "environment": config.environment,
        "execution_id": execution_id,
        "source_path": config.source_path,
        "target_table": config.target_table,
        "mode": config.mode,
    }
    _log_event(logging.INFO, "ingestion_started", **context)

    try:
        quoted_target = _validate_delta_target(spark, config)
        if not _validate_source_directory(dbutils, config):
            result = {
                **context,
                "status": "empty",
                "duration_seconds": time.monotonic() - started_at,
            }
            _log_event(logging.INFO, "ingestion_empty_source", **result)
            return result

        if config.mode == "auto_loader":
            _run_auto_loader(spark, config, quoted_target, execution_id)
        else:
            _run_batch(spark, config, quoted_target, execution_id)

        duration = time.monotonic() - started_at
        result = {**context, "status": "success", "duration_seconds": duration}
        _log_event(logging.INFO, "ingestion_completed", **result)
        return result
    except Exception as error:
        duration = time.monotonic() - started_at
        _log_event(
            logging.ERROR,
            "ingestion_failed",
            **context,
            duration_seconds=duration,
            error_type=type(error).__name__,
            error_detail=str(error),
        )
        raise RuntimeError(
            f"Ingestion failed for dataset {config.dataset!r} from "
            f"{config.source_path!r} to {config.target_table!r}: {error}"
        ) from error


def load_cities(file_path: str) -> List[dict]:
    """Loads the list of cities from a JSON file."""
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def create_batches(items: List[Any], batch_size: int) -> Generator[List[Any], None, None]:
    """Splits a list of items into batches with a maximum size defined by batch_size."""
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]