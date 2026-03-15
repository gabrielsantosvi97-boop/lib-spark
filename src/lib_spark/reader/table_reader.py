from __future__ import annotations

import logging
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from lib_spark.exceptions import TableNotFoundError

logger = logging.getLogger("lib_spark.reader")


class TableReader:
    def __init__(self, spark: SparkSession):
        self._spark = spark

    def read_full(self, table_name: str) -> DataFrame:
        """Read the entire table from the catalog."""
        logger.info("Reading full table: %s", table_name)
        try:
            df = self._spark.table(table_name)
        except Exception as exc:
            raise TableNotFoundError(
                f"Failed to read table '{table_name}': {exc}"
            ) from exc
        logger.info(
            "Full read of '%s': schema has %d columns.",
            table_name,
            len(df.schema.fields),
        )
        return df

    def read_incremental(
        self,
        table_name: str,
        incremental_column: str,
        incremental_value: Any,
    ) -> DataFrame:
        """Read rows where incremental_column > incremental_value."""
        logger.info(
            "Reading incremental from '%s' where %s > %s",
            table_name,
            incremental_column,
            incremental_value,
        )
        df = self.read_full(table_name)
        filtered = df.filter(col(incremental_column) > incremental_value)
        return filtered

    def read(
        self,
        table_name: str,
        incremental_column: Optional[str] = None,
        incremental_value: Optional[Any] = None,
    ) -> DataFrame:
        """Unified read: full or incremental depending on arguments."""
        if incremental_column is not None and incremental_value is not None:
            return self.read_incremental(
                table_name, incremental_column, incremental_value
            )
        return self.read_full(table_name)
