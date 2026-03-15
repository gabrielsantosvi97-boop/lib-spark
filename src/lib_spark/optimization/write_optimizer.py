from __future__ import annotations

import logging
from typing import List

from pyspark.sql import DataFrame, SparkSession

from lib_spark.config import WriteOptimizationConfig

logger = logging.getLogger("lib_spark.optimization")

_TARGET_FILE_SIZE_PROPERTY = "write.target-file-size-bytes"
_DISTRIBUTION_MODE_PROPERTY = "write.distribution-mode"
_ADVISORY_PARTITION_SIZE = "spark.sql.iceberg.advisory-partition-size"

_MB = 1024 * 1024


class WriteOptimizer:
    """Applies write-time optimizations to reduce small files."""

    def apply(
        self,
        spark: SparkSession,
        df: DataFrame,
        table_name: str,
        config: WriteOptimizationConfig,
        partition_columns: List[str] | None = None,
    ) -> DataFrame:
        if not config.enabled:
            logger.debug("Write optimization disabled; skipping.")
            return df

        self._set_table_write_properties(spark, table_name, config)
        self._set_sort_order(spark, table_name, config)
        self._set_advisory_partition_size(spark, config)
        df = self._maybe_repartition(df, config, partition_columns)

        sort_info = f", sort_columns={config.sort_columns}" if config.sort_columns else ""
        logger.info(
            "Write optimization applied: distribution_mode=%s, "
            "target_file_size=%dMB, advisory_partition_size=%dMB%s",
            config.distribution_mode,
            config.target_file_size_mb,
            config.advisory_partition_size_mb,
            sort_info,
        )
        return df

    def _set_table_write_properties(
        self,
        spark: SparkSession,
        table_name: str,
        config: WriteOptimizationConfig,
    ) -> None:
        target_bytes = config.target_file_size_mb * _MB
        try:
            spark.sql(
                f"ALTER TABLE {table_name} SET TBLPROPERTIES ("
                f"'{_TARGET_FILE_SIZE_PROPERTY}' = '{target_bytes}', "
                f"'{_DISTRIBUTION_MODE_PROPERTY}' = '{config.distribution_mode}')"
            )
        except Exception as exc:
            logger.warning(
                "Could not set table write properties on '%s': %s",
                table_name,
                exc,
            )

    def _set_sort_order(
        self,
        spark: SparkSession,
        table_name: str,
        config: WriteOptimizationConfig,
    ) -> None:
        if not config.sort_columns:
            return

        cols = ", ".join(config.sort_columns)
        sql = f"ALTER TABLE {table_name} WRITE ORDERED BY ({cols})"
        logger.info("Setting sort order: %s", sql)
        try:
            spark.sql(sql)
        except Exception as exc:
            logger.warning(
                "Could not set sort order on '%s': %s", table_name, exc
            )

    def _set_advisory_partition_size(
        self,
        spark: SparkSession,
        config: WriteOptimizationConfig,
    ) -> None:
        advisory_bytes = str(config.advisory_partition_size_mb * _MB)
        spark.conf.set(_ADVISORY_PARTITION_SIZE, advisory_bytes)

    def _maybe_repartition(
        self,
        df: DataFrame,
        config: WriteOptimizationConfig,
        partition_columns: List[str] | None,
    ) -> DataFrame:
        if not config.repartition_by_partition_columns:
            return df

        if not partition_columns:
            return df

        num_files = df.rdd.getNumPartitions()
        if num_files < config.min_input_files_before_repartition:
            logger.debug(
                "DataFrame has %d partitions (< threshold %d); "
                "skipping repartition.",
                num_files,
                config.min_input_files_before_repartition,
            )
            return df

        logger.info(
            "Repartitioning DataFrame by %s (%d -> coalesced)",
            partition_columns,
            num_files,
        )
        return df.repartition(*[df[c] for c in partition_columns])
