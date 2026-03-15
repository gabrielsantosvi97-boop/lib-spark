from __future__ import annotations

import logging
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from lib_spark.config import (
    ExecutionPlan,
    LoadStrategy,
    SchemaDiff,
    SchemaPolicy,
    TableMetadata,
    WriteConfig,
    WriteMode,
)
from lib_spark.writer.append import AppendWriter
from lib_spark.writer.base import BaseWriter
from lib_spark.writer.merge import MergeWriter
from lib_spark.writer.overwrite import (
    OverwritePartitionsWriter,
    OverwriteTableWriter,
)

logger = logging.getLogger("lib_spark.planner")

_WRITER_REGISTRY = {
    WriteMode.APPEND: AppendWriter,
    WriteMode.OVERWRITE_TABLE: OverwriteTableWriter,
    WriteMode.OVERWRITE_PARTITIONS: OverwritePartitionsWriter,
    WriteMode.MERGE: MergeWriter,
}


class ExecutionPlanner:
    def resolve_writer(self, write_mode: WriteMode) -> BaseWriter:
        writer_cls = _WRITER_REGISTRY.get(write_mode)
        if writer_cls is None:
            raise ValueError(f"No writer registered for mode '{write_mode}'.")
        return writer_cls()

    def prepare_dataframe(
        self,
        df: DataFrame,
        config: WriteConfig,
    ) -> DataFrame:
        """Apply load strategy filtering to the DataFrame."""
        if config.load_strategy == LoadStrategy.INCREMENTAL:
            if config.incremental_column and config.incremental_value is not None:
                logger.info(
                    "Applying incremental filter: %s > %s",
                    config.incremental_column,
                    config.incremental_value,
                )
                return df.filter(
                    col(config.incremental_column) > config.incremental_value
                )
        return df

    def build_plan(
        self,
        config: WriteConfig,
        table_metadata: TableMetadata,
        schema_actions: List[str] | None = None,
    ) -> ExecutionPlan:
        return ExecutionPlan(
            writer_key=config.write_mode,
            table_metadata=table_metadata,
            config=config,
            schema_actions=schema_actions or [],
        )
