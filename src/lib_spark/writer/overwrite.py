from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from lib_spark.writer.base import BaseWriter

logger = logging.getLogger("lib_spark.writer")


class OverwriteTableWriter(BaseWriter):
    """Overwrites the entire Iceberg table with the DataFrame contents."""

    def execute(
        self,
        spark: SparkSession,
        df: DataFrame,
        table_name: str,
        **kwargs,
    ) -> int:
        count = df.count()
        logger.info(
            "OVERWRITE_TABLE: writing %d records to '%s'",
            count,
            table_name,
        )
        view_name = f"__lib_spark_overwrite_{table_name.replace('.', '_')}"
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT OVERWRITE TABLE {table_name} SELECT * FROM {view_name}")
        spark.catalog.dropTempView(view_name)
        logger.info("OVERWRITE_TABLE: completed for '%s'.", table_name)
        return count


class OverwritePartitionsWriter(BaseWriter):
    """Overwrites only the partitions present in the DataFrame (Iceberg dynamic overwrite)."""

    def execute(
        self,
        spark: SparkSession,
        df: DataFrame,
        table_name: str,
        **kwargs,
    ) -> int:
        count = df.count()
        logger.info(
            "OVERWRITE_PARTITIONS: writing %d records to '%s'",
            count,
            table_name,
        )
        df.writeTo(table_name).overwritePartitions()
        logger.info(
            "OVERWRITE_PARTITIONS: completed for '%s'.", table_name
        )
        return count
