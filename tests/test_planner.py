import pytest

from lib_spark.config import (
    LoadStrategy,
    SchemaPolicy,
    TableMetadata,
    WriteConfig,
    WriteMode,
)
from lib_spark.planner.execution_planner import ExecutionPlanner
from lib_spark.writer.append import AppendWriter
from lib_spark.writer.merge import MergeWriter
from lib_spark.writer.overwrite import (
    OverwritePartitionsWriter,
    OverwriteTableWriter,
)


class TestExecutionPlanner:
    def setup_method(self):
        self.planner = ExecutionPlanner()

    def test_resolve_append_writer(self):
        writer = self.planner.resolve_writer(WriteMode.APPEND)
        assert isinstance(writer, AppendWriter)

    def test_resolve_overwrite_table_writer(self):
        writer = self.planner.resolve_writer(WriteMode.OVERWRITE_TABLE)
        assert isinstance(writer, OverwriteTableWriter)

    def test_resolve_overwrite_partitions_writer(self):
        writer = self.planner.resolve_writer(WriteMode.OVERWRITE_PARTITIONS)
        assert isinstance(writer, OverwritePartitionsWriter)

    def test_resolve_merge_writer(self):
        writer = self.planner.resolve_writer(WriteMode.MERGE)
        assert isinstance(writer, MergeWriter)

    def test_prepare_dataframe_full_returns_same(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
        cfg = WriteConfig(
            target_table="db.t",
            write_mode=WriteMode.APPEND,
            load_strategy=LoadStrategy.FULL,
        )
        result = self.planner.prepare_dataframe(df, cfg)
        assert result.count() == 2

    def test_prepare_dataframe_incremental_filters(self, spark):
        df = spark.createDataFrame(
            [(1, 100), (2, 200), (3, 300)], ["id", "ts"]
        )
        cfg = WriteConfig(
            target_table="db.t",
            write_mode=WriteMode.APPEND,
            load_strategy=LoadStrategy.INCREMENTAL,
            incremental_column="ts",
            incremental_value=150,
        )
        result = self.planner.prepare_dataframe(df, cfg)
        assert result.count() == 2

    def test_build_plan(self):
        cfg = WriteConfig(
            target_table="c.db.t",
            write_mode=WriteMode.MERGE,
            merge_keys=["id"],
        )
        meta = TableMetadata(
            catalog="c", database="db", table="t", full_name="c.db.t"
        )
        plan = self.planner.build_plan(cfg, meta, schema_actions=["ADD COLUMN x string"])
        assert plan.writer_key == WriteMode.MERGE
        assert len(plan.schema_actions) == 1
