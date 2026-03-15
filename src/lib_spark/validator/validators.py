from __future__ import annotations

import logging
from typing import Optional, Set

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from lib_spark.config import (
    VALID_DISTRIBUTION_MODES,
    ExecutionMode,
    JobExecutionConfig,
    LoadStrategy,
    MaintenanceConfig,
    TableMetadata,
    WriteConfig,
    WriteMode,
    WriteOptimizationConfig,
)
from lib_spark.exceptions import (
    InvalidConfigError,
    InvalidRuntimeParamsError,
    MergeKeyError,
    PartitionValidationError,
    SchemaValidationError,
    UnsupportedExecutionModeError,
)

logger = logging.getLogger("lib_spark.validator")


def validate_config(config: WriteConfig) -> None:
    """Validate that all required fields are present and consistent."""
    if not config.target_table or not config.target_table.strip():
        raise InvalidConfigError("target_table must not be empty.")

    parts = config.target_table.strip().split(".")
    if len(parts) < 2:
        raise InvalidConfigError(
            f"target_table '{config.target_table}' must be at least database.table"
        )

    if config.write_mode == WriteMode.MERGE and not config.merge_keys:
        raise InvalidConfigError(
            "merge_keys are required when write_mode is MERGE."
        )

    if (
        config.load_strategy == LoadStrategy.INCREMENTAL
        and not config.incremental_column
    ):
        raise InvalidConfigError(
            "incremental_column is required when load_strategy is INCREMENTAL."
        )

    if (
        config.write_mode == WriteMode.OVERWRITE_PARTITIONS
        and not config.partition_columns
    ):
        raise InvalidConfigError(
            "partition_columns are required when write_mode is OVERWRITE_PARTITIONS."
        )

    logger.debug("Config validation passed for '%s'.", config.target_table)


def validate_table_exists(metadata: TableMetadata) -> None:
    if not metadata.exists:
        from lib_spark.exceptions import TableNotFoundError

        raise TableNotFoundError(
            f"Table '{metadata.full_name}' does not exist in the catalog."
        )


def validate_merge_keys(
    config: WriteConfig,
    df_schema: StructType,
    table_schema: StructType,
) -> None:
    """Ensure every merge key exists in both the DataFrame and the target table."""
    if config.write_mode != WriteMode.MERGE:
        return

    df_cols: Set[str] = {f.name.lower() for f in df_schema.fields}
    table_cols: Set[str] = {f.name.lower() for f in table_schema.fields}

    for key in config.merge_keys:
        key_lower = key.lower()
        if key_lower not in df_cols:
            raise MergeKeyError(
                f"Merge key '{key}' not found in the source DataFrame. "
                f"Available columns: {sorted(df_cols)}"
            )
        if key_lower not in table_cols:
            raise MergeKeyError(
                f"Merge key '{key}' not found in the target table schema. "
                f"Available columns: {sorted(table_cols)}"
            )

    logger.debug("Merge key validation passed: %s", config.merge_keys)


def validate_partition_columns(
    config: WriteConfig,
    table_metadata: TableMetadata,
) -> None:
    """Validate partition columns when the operation requires them."""
    if config.write_mode != WriteMode.OVERWRITE_PARTITIONS:
        return

    table_partitions = {c.lower() for c in table_metadata.partition_columns}

    for col in config.partition_columns:
        if col.lower() not in table_partitions:
            raise PartitionValidationError(
                f"Partition column '{col}' is not a partition column of table "
                f"'{table_metadata.full_name}'. "
                f"Table partitions: {sorted(table_partitions)}"
            )

    logger.debug(
        "Partition column validation passed: %s", config.partition_columns
    )


def validate_schema_compatibility(
    df_schema: StructType,
    table_schema: StructType,
) -> None:
    """Basic check that the DataFrame has no columns with types wildly incompatible."""
    df_fields = {f.name.lower(): f for f in df_schema.fields}
    table_fields = {f.name.lower(): f for f in table_schema.fields}

    errors = []
    for col_name, df_field in df_fields.items():
        if col_name in table_fields:
            table_field = table_fields[col_name]
            if df_field.dataType.simpleString() != table_field.dataType.simpleString():
                errors.append(
                    f"Column '{col_name}': source type "
                    f"'{df_field.dataType.simpleString()}' differs from target "
                    f"'{table_field.dataType.simpleString()}'"
                )

    if errors:
        raise SchemaValidationError(
            "Schema compatibility check found type mismatches:\n"
            + "\n".join(f"  - {e}" for e in errors)
        )

    logger.debug("Schema compatibility check passed.")


def validate_dataframe_not_empty(df: DataFrame) -> None:
    head_row = df.head(1)
    if not head_row:
        raise InvalidConfigError(
            "The source DataFrame is empty. Nothing to write."
        )


def validate_optimization_config(config: WriteOptimizationConfig) -> None:
    if not config.enabled:
        return

    if config.distribution_mode not in VALID_DISTRIBUTION_MODES:
        raise InvalidConfigError(
            f"distribution_mode '{config.distribution_mode}' is invalid. "
            f"Valid values: {VALID_DISTRIBUTION_MODES}"
        )

    if config.target_file_size_mb <= 0:
        raise InvalidConfigError(
            f"target_file_size_mb must be positive, got {config.target_file_size_mb}."
        )

    if config.advisory_partition_size_mb <= 0:
        raise InvalidConfigError(
            f"advisory_partition_size_mb must be positive, "
            f"got {config.advisory_partition_size_mb}."
        )

    if config.min_input_files_before_repartition < 0:
        raise InvalidConfigError(
            f"min_input_files_before_repartition must be >= 0, "
            f"got {config.min_input_files_before_repartition}."
        )

    for col in config.sort_columns:
        if not col or not col.strip():
            raise InvalidConfigError(
                "sort_columns must not contain empty or blank strings."
            )

    logger.debug("Optimization config validation passed.")


def validate_maintenance_config(config: MaintenanceConfig) -> None:
    if not config.enabled:
        return

    if config.snapshot_retention_days <= 0:
        raise InvalidConfigError(
            f"snapshot_retention_days must be > 0, "
            f"got {config.snapshot_retention_days}."
        )

    if config.retain_last_snapshots < 1:
        raise InvalidConfigError(
            f"retain_last_snapshots must be >= 1, "
            f"got {config.retain_last_snapshots}."
        )

    if config.rewrite_data_files_min_input_files < 1:
        raise InvalidConfigError(
            f"rewrite_data_files_min_input_files must be >= 1, "
            f"got {config.rewrite_data_files_min_input_files}."
        )

    logger.debug("Maintenance config validation passed.")


def validate_runtime_dates(
    date_start: Optional[str],
    date_end: Optional[str],
) -> None:
    """Validate date_start/date_end combination for runtime params.

    Raises InvalidRuntimeParamsError if date_end is set without date_start,
    or if date_start > date_end.
    """
    if date_end and not date_start:
        raise InvalidRuntimeParamsError(
            "date_end requires date_start. Provide date_start when using date_end."
        )
    if date_start and date_end and date_start > date_end:
        raise InvalidRuntimeParamsError(
            f"date_start ({date_start}) must be less than or equal to date_end ({date_end})."
        )
    logger.debug(
        "Runtime dates validated: date_start=%s, date_end=%s",
        date_start,
        date_end,
    )


def validate_job_supports_mode(
    job_config: JobExecutionConfig,
    mode: ExecutionMode,
) -> None:
    """Check that the job supports the resolved execution mode.

    Raises UnsupportedExecutionModeError if the mode is not supported.
    """
    if mode == ExecutionMode.FULL_REFRESH and not job_config.supports_full_refresh:
        raise UnsupportedExecutionModeError(
            "Job does not support full_refresh. Set supports_full_refresh=True in JobExecutionConfig to allow it."
        )
    if mode == ExecutionMode.FROM_DATE and not job_config.supports_from_date:
        raise UnsupportedExecutionModeError(
            "Job does not support from_date (date_start only). Set supports_from_date=True in JobExecutionConfig to allow it."
        )
    if mode == ExecutionMode.DATE_RANGE and not job_config.supports_date_range:
        raise UnsupportedExecutionModeError(
            "Job does not support date_range (date_start + date_end). Set supports_date_range=True in JobExecutionConfig to allow it."
        )
    logger.debug("Job supports execution mode: %s", mode.value)
