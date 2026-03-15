from __future__ import annotations

import logging
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from lib_spark.audit.logger import AuditLogger
from lib_spark.catalog.resolver import CatalogResolver
from lib_spark.config import (
    MaintenanceConfig,
    SchemaDiff,
    WriteConfig,
    WriteMode,
    WriteOptimizationConfig,
    WriteResult,
)
from lib_spark.exceptions import LibSparkError
from lib_spark.maintenance.table_maintenance import TableMaintenance
from lib_spark.optimization.write_optimizer import WriteOptimizer
from lib_spark.planner.execution_planner import ExecutionPlanner
from lib_spark.reader.table_reader import TableReader
from lib_spark.schema_manager.comparator import compare_schemas
from lib_spark.schema_manager.evolver import apply_evolution
from lib_spark.validator.validators import (
    validate_config,
    validate_dataframe_not_empty,
    validate_maintenance_config,
    validate_merge_keys,
    validate_optimization_config,
    validate_partition_columns,
    validate_schema_compatibility,
    validate_table_exists,
)

logger = logging.getLogger("lib_spark")

_DEFAULT_OPTIMIZATION = WriteOptimizationConfig()
_DEFAULT_MAINTENANCE = MaintenanceConfig()


class GlueTableManager:
    """Facade that orchestrates reading, validation, schema evolution, and writing."""

    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._catalog = CatalogResolver(spark)
        self._planner = ExecutionPlanner()
        self._reader = TableReader(spark)
        self._audit = AuditLogger()
        self._optimizer = WriteOptimizer()
        self._maintenance = TableMaintenance()

    def write(
        self,
        df: DataFrame,
        config: WriteConfig,
        optimization: WriteOptimizationConfig | None = None,
        maintenance: MaintenanceConfig | None = None,
    ) -> WriteResult:
        """Execute a validated, audited write to an Iceberg table.

        Parameters
        ----------
        df : DataFrame
            Source data.
        config : WriteConfig
            Core write configuration.
        optimization : WriteOptimizationConfig, optional
            Write-time optimization settings.  When *None*, sensible defaults
            are applied automatically (``enabled=True``).
        maintenance : MaintenanceConfig, optional
            Post-write maintenance settings.  When *None*, no maintenance is
            executed (``enabled=False``).
        """
        opt = optimization if optimization is not None else _DEFAULT_OPTIMIZATION
        mnt = maintenance if maintenance is not None else _DEFAULT_MAINTENANCE

        result = self._audit.start(config)

        try:
            validate_config(config)
            validate_optimization_config(opt)
            validate_maintenance_config(mnt)

            table_meta = self._catalog.resolve(config.target_table)
            validate_table_exists(table_meta)

            if config.write_mode == WriteMode.MERGE:
                validate_merge_keys(config, df.schema, table_meta.schema)

            if config.write_mode == WriteMode.OVERWRITE_PARTITIONS:
                validate_partition_columns(config, table_meta)

            diff = compare_schemas(df.schema, table_meta.schema)
            schema_actions = []
            if diff.has_differences:
                schema_actions = apply_evolution(
                    self._spark,
                    config.target_table,
                    diff,
                    config.schema_policy,
                )

            if not diff.has_differences:
                validate_schema_compatibility(df.schema, table_meta.schema)

            prepared_df = self._planner.prepare_dataframe(df, config)

            validate_dataframe_not_empty(prepared_df)

            prepared_df = self._optimizer.apply(
                self._spark,
                prepared_df,
                config.target_table,
                opt,
                partition_columns=config.partition_columns or table_meta.partition_columns,
            )

            writer = self._planner.resolve_writer(config.write_mode)

            writer_kwargs = {}
            if config.write_mode == WriteMode.MERGE:
                writer_kwargs["merge_keys"] = config.merge_keys

            records = writer.execute(
                self._spark,
                prepared_df,
                config.target_table,
                **writer_kwargs,
            )

            mnt_actions = self._maintenance.run(
                self._spark, config.target_table, mnt
            )

            return self._audit.finish_success(
                result,
                records_written=records,
                schema_changes=diff if diff.has_differences else None,
                optimization_applied=opt.enabled,
                maintenance_actions=mnt_actions,
            )

        except Exception as exc:
            self._audit.finish_error(result, exc)
            if isinstance(exc, LibSparkError):
                raise
            raise LibSparkError(
                f"Unexpected error writing to '{config.target_table}': {exc}"
            ) from exc

    def read(
        self,
        table_name: str,
        incremental_column: Optional[str] = None,
        incremental_value: Optional[Any] = None,
    ) -> DataFrame:
        """Read a table fully or incrementally."""
        return self._reader.read(
            table_name,
            incremental_column=incremental_column,
            incremental_value=incremental_value,
        )
