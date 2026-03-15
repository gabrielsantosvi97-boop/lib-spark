from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from lib_spark.writer.base import BaseWriter

logger = logging.getLogger("lib_spark.writer")


class AppendWriter(BaseWriter):
    """Appends new records to the target Iceberg table."""

    def execute(
        self,
        spark: SparkSession,
        df: DataFrame,
        table_name: str,
        **kwargs,
    ) -> int:
        count = df.count()
        logger.info(
            "APPEND: writing %d records to '%s'", count, table_name
        )
        df.writeTo(table_name).append()
        logger.info("APPEND: completed for '%s'.", table_name)
        return count
