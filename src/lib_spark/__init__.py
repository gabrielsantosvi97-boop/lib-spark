"""lib_spark -- PySpark library for standardized Iceberg table operations on AWS Glue Catalog."""

from lib_spark.config import (
    ExecutionContext,
    ExecutionMode,
    ExecutionPlan,
    JobExecutionConfig,
    LoadStrategy,
    MaintenanceConfig,
    RuntimeParams,
    SchemaDiff,
    SchemaPolicy,
    TableMetadata,
    TypeChange,
    WriteConfig,
    WriteMode,
    WriteOptimizationConfig,
    WriteResult,
)
from lib_spark.core import GlueTableManager
from lib_spark.exceptions import (
    InvalidConfigError,
    InvalidRuntimeParamsError,
    LibSparkError,
    MergeKeyError,
    PartitionValidationError,
    RuntimeParamsError,
    SchemaEvolutionBlockedError,
    SchemaValidationError,
    TableNotFoundError,
    UnsupportedExecutionModeError,
)
from lib_spark.execution_resolver import (
    ExecutionResolver,
    parse_runtime_params_from_argv,
    apply_runtime_filter,
    effective_write_config,
)

__version__ = "0.1.0"

__all__ = [
    "GlueTableManager",
    "WriteConfig",
    "WriteOptimizationConfig",
    "MaintenanceConfig",
    "WriteMode",
    "LoadStrategy",
    "SchemaPolicy",
    "WriteResult",
    "SchemaDiff",
    "TypeChange",
    "TableMetadata",
    "ExecutionPlan",
    "ExecutionMode",
    "RuntimeParams",
    "JobExecutionConfig",
    "ExecutionContext",
    "ExecutionResolver",
    "parse_runtime_params_from_argv",
    "apply_runtime_filter",
    "effective_write_config",
    "LibSparkError",
    "TableNotFoundError",
    "SchemaValidationError",
    "MergeKeyError",
    "PartitionValidationError",
    "SchemaEvolutionBlockedError",
    "InvalidConfigError",
    "RuntimeParamsError",
    "InvalidRuntimeParamsError",
    "UnsupportedExecutionModeError",
]
