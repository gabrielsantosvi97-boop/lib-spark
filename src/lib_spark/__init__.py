"""lib_spark -- PySpark library for standardized Iceberg table operations on AWS Glue Catalog."""

from lib_spark.config import (
    ExecutionPlan,
    LoadStrategy,
    MaintenanceConfig,
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
    LibSparkError,
    MergeKeyError,
    PartitionValidationError,
    SchemaEvolutionBlockedError,
    SchemaValidationError,
    TableNotFoundError,
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
    "LibSparkError",
    "TableNotFoundError",
    "SchemaValidationError",
    "MergeKeyError",
    "PartitionValidationError",
    "SchemaEvolutionBlockedError",
    "InvalidConfigError",
]
