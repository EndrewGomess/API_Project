import pytest

from src.utils.bronze_ingestion import (
    DatasetConfig,
    IngestionConfigurationError,
    project_dataset_config,
    quote_table_identifier,
)


@pytest.mark.parametrize(
    ("dataset", "source_format", "layout", "target_table"),
    [
        (
            "weather_historical",
            "json",
            "json_document_array",
            "clima.01_bronze.weather_historical",
        ),
        (
            "weather_forecast",
            "json",
            "json_document_array",
            "clima.01_bronze.weather_forecast",
        ),
        ("air_quality", "json", "json_document_array", "clima.01_bronze.air_quality"),
        ("geocoding", "json", "json_document_array", "clima.01_bronze.geocoding"),
        ("firms", "csv", "csv_header", "clima.01_bronze.wildfire"),
    ],
)
def test_project_dataset_config_uses_extractor_layouts(
    dataset: str,
    source_format: str,
    layout: str,
    target_table: str,
) -> None:
    config = project_dataset_config(
        dataset,
        environment="test",
        execution_id="test-geocoding" if dataset == "geocoding" else None,
    )

    assert config.source_format == source_format
    assert config.layout == layout
    assert config.target_table == target_table
    assert config.mode == ("batch" if dataset == "geocoding" else "auto_loader")
    assert config.checkpoint_path != config.schema_location
    assert "test" in config.checkpoint_path


def test_geocoding_source_targets_existing_volume_file_pattern() -> None:
    config = project_dataset_config(
        "geocoding", environment="dev", execution_id="geocoding-test-snapshot"
    )

    assert config.source_path.endswith("/location_data")
    assert config.path_glob_filter == "cities.json"
    assert config.mode == "batch"


def test_quote_table_identifier_handles_numeric_schema_name() -> None:
    assert quote_table_identifier("clima.01_bronze.wildfire") == (
        "`clima`.`01_bronze`.`wildfire`"
    )


def test_config_rejects_checkpoint_inside_source() -> None:
    config = project_dataset_config("firms", environment="test")
    invalid = DatasetConfig(
        dataset=config.dataset,
        environment=config.environment,
        source_path=config.source_path,
        source_format=config.source_format,
        layout=config.layout,
        target_table=config.target_table,
        checkpoint_path=f"{config.source_path}/checkpoint",
        schema_location=config.schema_location,
        mode=config.mode,
        path_glob_filter=config.path_glob_filter,
        reader_options=config.reader_options,
    )

    with pytest.raises(IngestionConfigurationError, match="inside the source path"):
        invalid.validate()


def test_batch_mode_requires_stable_execution_id() -> None:
    with pytest.raises(IngestionConfigurationError, match="explicit stable execution_id"):
        project_dataset_config("weather_historical", environment="test", mode="batch")