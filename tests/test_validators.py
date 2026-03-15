import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from lib_spark.config import (
    LoadStrategy,
    MaintenanceConfig,
    SchemaPolicy,
    TableMetadata,
    WriteConfig,
    WriteMode,
    WriteOptimizationConfig,
)
from lib_spark.exceptions import (
    InvalidConfigError,
    MergeKeyError,
    PartitionValidationError,
    SchemaValidationError,
)
from lib_spark.validator.validators import (
    validate_config,
    validate_maintenance_config,
    validate_merge_keys,
    validate_optimization_config,
    validate_partition_columns,
    validate_schema_compatibility,
    validate_table_exists,
)


class TestValidateConfig:
    def test_valid_append(self):
        cfg = WriteConfig(
            target_table="db.table",
            write_mode=WriteMode.APPEND,
        )
        validate_config(cfg)

    def test_empty_target_table_raises(self):
        cfg = WriteConfig(target_table="", write_mode=WriteMode.APPEND)
        with pytest.raises(InvalidConfigError, match="target_table must not be empty"):
            validate_config(cfg)

    def test_single_part_target_raises(self):
        cfg = WriteConfig(target_table="table_only", write_mode=WriteMode.APPEND)
        with pytest.raises(InvalidConfigError, match="at least database.table"):
            validate_config(cfg)

    def test_merge_without_keys_raises(self):
        cfg = WriteConfig(
            target_table="db.table",
            write_mode=WriteMode.MERGE,
            merge_keys=[],
        )
        with pytest.raises(InvalidConfigError, match="merge_keys are required"):
            validate_config(cfg)

    def test_incremental_without_column_raises(self):
        cfg = WriteConfig(
            target_table="db.table",
            write_mode=WriteMode.APPEND,
            load_strategy=LoadStrategy.INCREMENTAL,
        )
        with pytest.raises(InvalidConfigError, match="incremental_column is required"):
            validate_config(cfg)

    def test_overwrite_partitions_without_columns_raises(self):
        cfg = WriteConfig(
            target_table="db.table",
            write_mode=WriteMode.OVERWRITE_PARTITIONS,
        )
        with pytest.raises(InvalidConfigError, match="partition_columns are required"):
            validate_config(cfg)


class TestValidateMergeKeys:
    def _schema(self, *names):
        return StructType(
            [StructField(n, StringType(), True) for n in names]
        )

    def test_valid_keys(self):
        cfg = WriteConfig(
            target_table="db.t",
            write_mode=WriteMode.MERGE,
            merge_keys=["id"],
        )
        validate_merge_keys(cfg, self._schema("id", "name"), self._schema("id", "name"))

    def test_key_missing_from_source(self):
        cfg = WriteConfig(
            target_table="db.t",
            write_mode=WriteMode.MERGE,
            merge_keys=["id"],
        )
        with pytest.raises(MergeKeyError, match="not found in the source DataFrame"):
            validate_merge_keys(cfg, self._schema("name"), self._schema("id", "name"))

    def test_key_missing_from_target(self):
        cfg = WriteConfig(
            target_table="db.t",
            write_mode=WriteMode.MERGE,
            merge_keys=["id"],
        )
        with pytest.raises(MergeKeyError, match="not found in the target table"):
            validate_merge_keys(cfg, self._schema("id", "name"), self._schema("name"))

    def test_skipped_for_non_merge_mode(self):
        cfg = WriteConfig(
            target_table="db.t",
            write_mode=WriteMode.APPEND,
        )
        validate_merge_keys(cfg, self._schema("a"), self._schema("b"))


class TestValidatePartitionColumns:
    def test_valid_partitions(self):
        cfg = WriteConfig(
            target_table="db.t",
            write_mode=WriteMode.OVERWRITE_PARTITIONS,
            partition_columns=["year"],
        )
        meta = TableMetadata(
            catalog="c", database="db", table="t",
            full_name="c.db.t", partition_columns=["year"],
        )
        validate_partition_columns(cfg, meta)

    def test_invalid_partition(self):
        cfg = WriteConfig(
            target_table="db.t",
            write_mode=WriteMode.OVERWRITE_PARTITIONS,
            partition_columns=["month"],
        )
        meta = TableMetadata(
            catalog="c", database="db", table="t",
            full_name="c.db.t", partition_columns=["year"],
        )
        with pytest.raises(PartitionValidationError, match="not a partition column"):
            validate_partition_columns(cfg, meta)


class TestValidateSchemaCompatibility:
    def test_matching_schemas(self):
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        validate_schema_compatibility(schema, schema)

    def test_type_mismatch(self):
        src = StructType([StructField("id", StringType(), True)])
        tgt = StructType([StructField("id", IntegerType(), True)])
        with pytest.raises(SchemaValidationError, match="type mismatches"):
            validate_schema_compatibility(src, tgt)


class TestValidateOptimizationConfig:
    def test_valid_defaults(self):
        validate_optimization_config(WriteOptimizationConfig())

    def test_disabled_skips_validation(self):
        cfg = WriteOptimizationConfig(enabled=False, target_file_size_mb=-1)
        validate_optimization_config(cfg)

    def test_invalid_distribution_mode(self):
        cfg = WriteOptimizationConfig(distribution_mode="invalid")
        with pytest.raises(InvalidConfigError, match="distribution_mode"):
            validate_optimization_config(cfg)

    def test_negative_target_file_size(self):
        cfg = WriteOptimizationConfig(target_file_size_mb=-10)
        with pytest.raises(InvalidConfigError, match="target_file_size_mb"):
            validate_optimization_config(cfg)

    def test_zero_advisory_partition_size(self):
        cfg = WriteOptimizationConfig(advisory_partition_size_mb=0)
        with pytest.raises(InvalidConfigError, match="advisory_partition_size_mb"):
            validate_optimization_config(cfg)

    def test_negative_min_input_files(self):
        cfg = WriteOptimizationConfig(min_input_files_before_repartition=-1)
        with pytest.raises(InvalidConfigError, match="min_input_files_before_repartition"):
            validate_optimization_config(cfg)

    def test_sort_columns_with_empty_string(self):
        cfg = WriteOptimizationConfig(sort_columns=["region", ""])
        with pytest.raises(InvalidConfigError, match="sort_columns must not contain empty"):
            validate_optimization_config(cfg)

    def test_sort_columns_with_blank_string(self):
        cfg = WriteOptimizationConfig(sort_columns=["  "])
        with pytest.raises(InvalidConfigError, match="sort_columns must not contain empty"):
            validate_optimization_config(cfg)

    def test_sort_columns_valid(self):
        cfg = WriteOptimizationConfig(sort_columns=["region", "event_date"])
        validate_optimization_config(cfg)


class TestValidateMaintenanceConfig:
    def test_valid_defaults(self):
        validate_maintenance_config(MaintenanceConfig())

    def test_disabled_skips_validation(self):
        cfg = MaintenanceConfig(enabled=False, snapshot_retention_days=-5)
        validate_maintenance_config(cfg)

    def test_zero_retention_days(self):
        cfg = MaintenanceConfig(enabled=True, snapshot_retention_days=0)
        with pytest.raises(InvalidConfigError, match="snapshot_retention_days"):
            validate_maintenance_config(cfg)

    def test_zero_retain_last_snapshots(self):
        cfg = MaintenanceConfig(enabled=True, retain_last_snapshots=0)
        with pytest.raises(InvalidConfigError, match="retain_last_snapshots"):
            validate_maintenance_config(cfg)

    def test_zero_rewrite_min_files(self):
        cfg = MaintenanceConfig(enabled=True, rewrite_data_files_min_input_files=0)
        with pytest.raises(InvalidConfigError, match="rewrite_data_files_min_input_files"):
            validate_maintenance_config(cfg)
