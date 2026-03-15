from __future__ import annotations

import abc
import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger("lib_spark.writer")


class BaseWriter(abc.ABC):
    """Abstract base class for all write operations."""

    @abc.abstractmethod
    def execute(
        self,
        spark: SparkSession,
        df: DataFrame,
        table_name: str,
        **kwargs,
    ) -> int:
        """Write the DataFrame to the target table and return rows written."""
