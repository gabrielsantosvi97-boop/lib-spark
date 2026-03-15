from __future__ import annotations

import logging
from typing import List

from pyspark.sql import DataFrame, SparkSession

from lib_spark.writer.base import BaseWriter

logger = logging.getLogger("lib_spark.writer")


class MergeWriter(BaseWriter):
    """Performs MERGE INTO (upsert) on an Iceberg table using Spark SQL."""

    def execute(
        self,
        spark: SparkSession,
        df: DataFrame,
        table_name: str,
        **kwargs,
    ) -> int:
        merge_keys: List[str] = kwargs.get("merge_keys", [])
        if not merge_keys:
            raise ValueError("merge_keys are required for MergeWriter.")

        count = df.count()
        source_view = f"__lib_spark_merge_src_{table_name.replace('.', '_')}"
        df.createOrReplaceTempView(source_view)

        join_condition = " AND ".join(
            f"target.{key} = source.{key}" for key in merge_keys
        )

        merge_sql = (
            f"MERGE INTO {table_name} AS target "
            f"USING {source_view} AS source "
            f"ON {join_condition} "
            f"WHEN MATCHED THEN UPDATE SET * "
            f"WHEN NOT MATCHED THEN INSERT *"
        )

        logger.info(
            "MERGE: executing merge with %d source records into '%s' "
            "on keys %s",
            count,
            table_name,
            merge_keys,
        )
        spark.sql(merge_sql)
        spark.catalog.dropTempView(source_view)
        logger.info("MERGE: completed for '%s'.", table_name)
        return count
