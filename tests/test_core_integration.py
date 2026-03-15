import pytest

from lib_spark import (
    GlueTableManager,
    InvalidConfigError,
    LoadStrategy,
    MaintenanceConfig,
    SchemaPolicy,
    TableNotFoundError,
    WriteConfig,
    WriteMode,
    WriteOptimizationConfig,
)
from lib_spark.exceptions import (
    MergeKeyError,
    SchemaEvolutionBlockedError,
)


class TestGlueTableManagerWrite:
    def test_append_full(self, spark, sample_table):
        manager = GlueTableManager(spark)
        df = spark.createDataFrame(
            [(1, "a", 1.0), (2, "b", 2.0)], ["id", "name", "value"]
        )
        config = WriteConfig(
            target_table=sample_table,
            write_mode=WriteMode.APPEND,
        )
        result = manager.write(df, config)

        assert result.success is True
        assert result.records_written == 2
        assert result.write_mode == WriteMode.APPEND
        assert result.duration_seconds >= 0
        assert spark.table(sample_table).count() == 2

    def test_overwrite_table(self, spark, sample_table):
        manager = GlueTableManager(spark)
        df1 = spark.createDataFrame(
            [(1, "a", 1.0), (2, "b", 2.0)], ["id", "name", "value"]
        )
        manager.write(
            df1,
            WriteConfig(target_table=sample_table, write_mode=WriteMode.APPEND),
        )

        df2 = spark.createDataFrame([(10, "x", 10.0)], ["id", "name", "value"])
        result = manager.write(
            df2,
            WriteConfig(
                target_table=sample_table,
                write_mode=WriteMode.OVERWRITE_TABLE,
            ),
        )
        assert result.success is True
        assert spark.table(sample_table).count() == 1

    def test_overwrite_partitions(self, spark, partitioned_table):
        manager = GlueTableManager(spark)
        df = spark.createDataFrame(
            [(1, "a", 2023), (2, "b", 2024)], ["id", "name", "year"]
        )
        manager.write(
            df,
            WriteConfig(
                target_table=partitioned_table,
                write_mode=WriteMode.APPEND,
            ),
        )
        assert spark.table(partitioned_table).count() == 2

        df_replace = spark.createDataFrame(
            [(10, "z", 2023)], ["id", "name", "year"]
        )
        result = manager.write(
            df_replace,
            WriteConfig(
                target_table=partitioned_table,
                write_mode=WriteMode.OVERWRITE_PARTITIONS,
                partition_columns=["year"],
            ),
        )
        assert result.success is True
        assert spark.table(partitioned_table).count() == 2

    def test_merge(self, spark, sample_table):
        manager = GlueTableManager(spark)
        df_init = spark.createDataFrame(
            [(1, "a", 1.0), (2, "b", 2.0)], ["id", "name", "value"]
        )
        manager.write(
            df_init,
            WriteConfig(target_table=sample_table, write_mode=WriteMode.APPEND),
        )

        df_merge = spark.createDataFrame(
            [(2, "b_new", 20.0), (3, "c", 3.0)], ["id", "name", "value"]
        )
        result = manager.write(
            df_merge,
            WriteConfig(
                target_table=sample_table,
                write_mode=WriteMode.MERGE,
                merge_keys=["id"],
            ),
        )
        assert result.success is True
        assert spark.table(sample_table).count() == 3

    def test_table_not_found_raises(self, spark, test_db):
        manager = GlueTableManager(spark)
        df = spark.createDataFrame([(1,)], ["id"])
        config = WriteConfig(
            target_table=f"{test_db}.nonexistent",
            write_mode=WriteMode.APPEND,
        )
        with pytest.raises(TableNotFoundError):
            manager.write(df, config)

    def test_invalid_config_raises(self, spark):
        manager = GlueTableManager(spark)
        df = spark.createDataFrame([(1,)], ["id"])
        config = WriteConfig(
            target_table="",
            write_mode=WriteMode.APPEND,
        )
        with pytest.raises(InvalidConfigError):
            manager.write(df, config)

    def test_write_with_optimization_none_uses_defaults(self, spark, sample_table):
        manager = GlueTableManager(spark)
        df = spark.createDataFrame(
            [(1, "a", 1.0)], ["id", "name", "value"]
        )
        result = manager.write(
            df,
            WriteConfig(target_table=sample_table, write_mode=WriteMode.APPEND),
            optimization=None,
            maintenance=None,
        )
        assert result.success is True
        assert result.optimization_applied is True
        assert result.maintenance_actions == []

    def test_write_with_optimization_disabled(self, spark, sample_table):
        manager = GlueTableManager(spark)
        df = spark.createDataFrame(
            [(1, "a", 1.0)], ["id", "name", "value"]
        )
        result = manager.write(
            df,
            WriteConfig(target_table=sample_table, write_mode=WriteMode.APPEND),
            optimization=WriteOptimizationConfig(enabled=False),
        )
        assert result.success is True
        assert result.optimization_applied is False

    def test_write_with_custom_optimization(self, spark, sample_table):
        manager = GlueTableManager(spark)
        df = spark.createDataFrame(
            [(1, "a", 1.0)], ["id", "name", "value"]
        )
        result = manager.write(
            df,
            WriteConfig(target_table=sample_table, write_mode=WriteMode.APPEND),
            optimization=WriteOptimizationConfig(target_file_size_mb=256),
        )
        assert result.success is True
        assert result.optimization_applied is True


class TestGlueTableManagerRead:
    def test_read_full(self, spark, sample_table):
        spark.createDataFrame(
            [(1, "a", 1.0), (2, "b", 2.0)], ["id", "name", "value"]
        ).writeTo(sample_table).append()

        manager = GlueTableManager(spark)
        df = manager.read(sample_table)
        assert df.count() == 2

    def test_read_incremental(self, spark, sample_table):
        spark.createDataFrame(
            [(1, "a", 1.0), (2, "b", 5.0), (3, "c", 10.0)],
            ["id", "name", "value"],
        ).writeTo(sample_table).append()

        manager = GlueTableManager(spark)
        df = manager.read(
            sample_table,
            incremental_column="value",
            incremental_value=3.0,
        )
        assert df.count() == 2


class TestSchemaEvolution:
    def test_strict_policy_blocks_new_column(self, spark, sample_table):
        manager = GlueTableManager(spark)
        df = spark.createDataFrame(
            [(1, "a", 1.0, "extra")], ["id", "name", "value", "new_col"]
        )
        config = WriteConfig(
            target_table=sample_table,
            write_mode=WriteMode.APPEND,
            schema_policy=SchemaPolicy.STRICT,
        )
        with pytest.raises(SchemaEvolutionBlockedError):
            manager.write(df, config)

    def test_safe_schema_evolution_adds_column(self, spark, sample_table):
        manager = GlueTableManager(spark)
        df = spark.createDataFrame(
            [(1, "a", 1.0, "extra")], ["id", "name", "value", "new_col"]
        )
        config = WriteConfig(
            target_table=sample_table,
            write_mode=WriteMode.APPEND,
            schema_policy=SchemaPolicy.SAFE_SCHEMA_EVOLUTION,
        )
        result = manager.write(df, config)
        assert result.success is True
        assert result.schema_changes is not None
        assert len(result.schema_changes.added_columns) == 1

        cols = [f.name for f in spark.table(sample_table).schema.fields]
        assert "new_col" in cols
