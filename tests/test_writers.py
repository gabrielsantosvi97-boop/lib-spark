import pytest

from lib_spark.config import WriteConfig, WriteMode
from lib_spark.writer.append import AppendWriter
from lib_spark.writer.merge import MergeWriter
from lib_spark.writer.overwrite import (
    OverwritePartitionsWriter,
    OverwriteTableWriter,
)


class TestAppendWriter:
    def test_append_inserts_rows(self, spark, sample_table):
        df = spark.createDataFrame(
            [(1, "a", 1.0), (2, "b", 2.0)],
            ["id", "name", "value"],
        )
        writer = AppendWriter()
        count = writer.execute(spark, df, sample_table)
        assert count == 2

        result = spark.table(sample_table).count()
        assert result == 2

    def test_append_accumulates(self, spark, sample_table):
        df1 = spark.createDataFrame([(1, "a", 1.0)], ["id", "name", "value"])
        df2 = spark.createDataFrame([(2, "b", 2.0)], ["id", "name", "value"])
        writer = AppendWriter()
        writer.execute(spark, df1, sample_table)
        writer.execute(spark, df2, sample_table)

        assert spark.table(sample_table).count() == 2


class TestOverwriteTableWriter:
    def test_overwrite_replaces_all(self, spark, sample_table):
        df_initial = spark.createDataFrame(
            [(1, "a", 1.0), (2, "b", 2.0)],
            ["id", "name", "value"],
        )
        AppendWriter().execute(spark, df_initial, sample_table)

        df_new = spark.createDataFrame(
            [(10, "x", 10.0)], ["id", "name", "value"]
        )
        writer = OverwriteTableWriter()
        count = writer.execute(spark, df_new, sample_table)
        assert count == 1

        assert spark.table(sample_table).count() == 1
        row = spark.table(sample_table).collect()[0]
        assert row["id"] == 10


class TestOverwritePartitionsWriter:
    def test_overwrites_only_affected_partition(self, spark, partitioned_table):
        df_2023 = spark.createDataFrame(
            [(1, "a", 2023), (2, "b", 2023)],
            ["id", "name", "year"],
        )
        df_2024 = spark.createDataFrame(
            [(3, "c", 2024)], ["id", "name", "year"]
        )
        AppendWriter().execute(spark, df_2023, partitioned_table)
        AppendWriter().execute(spark, df_2024, partitioned_table)

        assert spark.table(partitioned_table).count() == 3

        df_replace_2023 = spark.createDataFrame(
            [(10, "z", 2023)], ["id", "name", "year"]
        )
        writer = OverwritePartitionsWriter()
        writer.execute(spark, df_replace_2023, partitioned_table)

        total = spark.table(partitioned_table).count()
        assert total == 2

        rows_2024 = (
            spark.table(partitioned_table).filter("year = 2024").count()
        )
        assert rows_2024 == 1


class TestMergeWriter:
    def test_merge_inserts_and_updates(self, spark, sample_table):
        df_initial = spark.createDataFrame(
            [(1, "a", 1.0), (2, "b", 2.0)],
            ["id", "name", "value"],
        )
        AppendWriter().execute(spark, df_initial, sample_table)

        df_merge = spark.createDataFrame(
            [(2, "b_updated", 20.0), (3, "c", 3.0)],
            ["id", "name", "value"],
        )
        writer = MergeWriter()
        writer.execute(spark, df_merge, sample_table, merge_keys=["id"])

        result = spark.table(sample_table)
        assert result.count() == 3

        row2 = result.filter("id = 2").collect()[0]
        assert row2["name"] == "b_updated"
        assert row2["value"] == 20.0

    def test_merge_requires_keys(self, spark, sample_table):
        df = spark.createDataFrame([(1, "a", 1.0)], ["id", "name", "value"])
        writer = MergeWriter()
        with pytest.raises(ValueError, match="merge_keys are required"):
            writer.execute(spark, df, sample_table, merge_keys=[])
