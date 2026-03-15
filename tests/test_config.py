import pytest
from pyspark.sql.types import IntegerType, LongType, StringType, StructField

from lib_spark.config import (
    LoadStrategy,
    MaintenanceConfig,
    SchemaDiff,
    SchemaPolicy,
    TypeChange,
    WriteConfig,
    WriteMode,
    WriteOptimizationConfig,
    WriteResult,
)


class TestWriteMode:
    def test_enum_values(self):
        assert WriteMode.APPEND.value == "append"
        assert WriteMode.OVERWRITE_TABLE.value == "overwrite_table"
        assert WriteMode.OVERWRITE_PARTITIONS.value == "overwrite_partitions"
        assert WriteMode.MERGE.value == "merge"


class TestLoadStrategy:
    def test_enum_values(self):
        assert LoadStrategy.FULL.value == "full"
        assert LoadStrategy.INCREMENTAL.value == "incremental"


class TestSchemaPolicy:
    def test_enum_values(self):
        assert SchemaPolicy.STRICT.value == "strict"
        assert SchemaPolicy.SAFE_SCHEMA_EVOLUTION.value == "safe_schema_evolution"
        assert SchemaPolicy.FAIL_ON_DIFF.value == "fail_on_diff"
        assert SchemaPolicy.CUSTOM.value == "custom"


class TestSchemaDiff:
    def test_empty_diff_is_compatible(self):
        diff = SchemaDiff()
        assert diff.is_compatible is True
        assert diff.has_differences is False

    def test_added_columns_still_compatible(self):
        diff = SchemaDiff(
            added_columns=[StructField("new_col", StringType(), True)]
        )
        assert diff.is_compatible is True
        assert diff.has_differences is True

    def test_removed_columns_not_compatible(self):
        diff = SchemaDiff(
            removed_columns=[StructField("old_col", StringType(), True)]
        )
        assert diff.is_compatible is False

    def test_unsafe_type_change_not_compatible(self):
        diff = SchemaDiff(
            type_changes=[
                TypeChange("col", IntegerType(), StringType(), is_safe=False)
            ]
        )
        assert diff.is_compatible is False

    def test_safe_type_change_compatible(self):
        diff = SchemaDiff(
            type_changes=[
                TypeChange("col", IntegerType(), LongType(), is_safe=True)
            ]
        )
        assert diff.is_compatible is True


class TestWriteConfig:
    def test_defaults(self):
        cfg = WriteConfig(
            target_table="db.table",
            write_mode=WriteMode.APPEND,
        )
        assert cfg.load_strategy == LoadStrategy.FULL
        assert cfg.schema_policy == SchemaPolicy.STRICT
        assert cfg.merge_keys == []
        assert cfg.partition_columns == []
        assert cfg.incremental_column is None

    def test_merge_config(self):
        cfg = WriteConfig(
            target_table="catalog.db.table",
            write_mode=WriteMode.MERGE,
            merge_keys=["id"],
            schema_policy=SchemaPolicy.SAFE_SCHEMA_EVOLUTION,
        )
        assert cfg.merge_keys == ["id"]


class TestWriteOptimizationConfig:
    def test_defaults(self):
        cfg = WriteOptimizationConfig()
        assert cfg.enabled is True
        assert cfg.distribution_mode == "hash"
        assert cfg.sort_columns == []
        assert cfg.target_file_size_mb == 512
        assert cfg.advisory_partition_size_mb == 1024
        assert cfg.min_input_files_before_repartition == 10
        assert cfg.repartition_by_partition_columns is True

    def test_custom_values(self):
        cfg = WriteOptimizationConfig(
            target_file_size_mb=256,
            distribution_mode="range",
        )
        assert cfg.target_file_size_mb == 256
        assert cfg.distribution_mode == "range"

    def test_sort_columns_custom(self):
        cfg = WriteOptimizationConfig(sort_columns=["region", "event_date"])
        assert cfg.sort_columns == ["region", "event_date"]

    def test_disabled(self):
        cfg = WriteOptimizationConfig(enabled=False)
        assert cfg.enabled is False


class TestMaintenanceConfig:
    def test_defaults(self):
        cfg = MaintenanceConfig()
        assert cfg.enabled is False
        assert cfg.expire_snapshots is True
        assert cfg.snapshot_retention_days == 30
        assert cfg.retain_last_snapshots == 5
        assert cfg.rewrite_data_files is False
        assert cfg.rewrite_data_files_min_input_files == 20
        assert cfg.rewrite_manifests is False

    def test_enabled_with_custom_retention(self):
        cfg = MaintenanceConfig(
            enabled=True,
            snapshot_retention_days=7,
            retain_last_snapshots=3,
        )
        assert cfg.enabled is True
        assert cfg.snapshot_retention_days == 7
        assert cfg.retain_last_snapshots == 3


class TestWriteResultNewFields:
    def test_defaults(self):
        r = WriteResult(
            success=True,
            target_table="db.t",
            write_mode=WriteMode.APPEND,
            load_strategy=LoadStrategy.FULL,
        )
        assert r.optimization_applied is False
        assert r.maintenance_actions == []

    def test_with_maintenance(self):
        r = WriteResult(
            success=True,
            target_table="db.t",
            write_mode=WriteMode.APPEND,
            load_strategy=LoadStrategy.FULL,
            optimization_applied=True,
            maintenance_actions=["expire_snapshots"],
        )
        assert r.optimization_applied is True
        assert r.maintenance_actions == ["expire_snapshots"]
