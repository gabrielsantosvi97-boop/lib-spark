from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql.types import DataType, StructField


class WriteMode(Enum):
    APPEND = "append"
    OVERWRITE_TABLE = "overwrite_table"
    OVERWRITE_PARTITIONS = "overwrite_partitions"
    MERGE = "merge"


class LoadStrategy(Enum):
    FULL = "full"
    INCREMENTAL = "incremental"


class SchemaPolicy(Enum):
    STRICT = "strict"
    SAFE_SCHEMA_EVOLUTION = "safe_schema_evolution"
    FAIL_ON_DIFF = "fail_on_diff"
    CUSTOM = "custom"


@dataclass
class TypeChange:
    column_name: str
    source_type: DataType
    target_type: DataType
    is_safe: bool


@dataclass
class SchemaDiff:
    added_columns: List[StructField] = field(default_factory=list)
    removed_columns: List[StructField] = field(default_factory=list)
    type_changes: List[TypeChange] = field(default_factory=list)

    @property
    def is_compatible(self) -> bool:
        if self.removed_columns:
            return False
        if any(not tc.is_safe for tc in self.type_changes):
            return False
        return True

    @property
    def has_differences(self) -> bool:
        return bool(self.added_columns or self.removed_columns or self.type_changes)


VALID_DISTRIBUTION_MODES = ("none", "hash", "range")


@dataclass
class WriteOptimizationConfig:
    """Controls write-time optimizations to reduce small files.

    When ``optimization`` is not passed to ``GlueTableManager.write()``, a
    default instance with ``enabled=True`` is used automatically.
    """

    enabled: bool = True
    distribution_mode: str = "hash"
    sort_columns: List[str] = field(default_factory=list)
    target_file_size_mb: int = 512
    advisory_partition_size_mb: int = 1024
    min_input_files_before_repartition: int = 10
    repartition_by_partition_columns: bool = True


@dataclass
class MaintenanceConfig:
    """Controls optional post-write table maintenance routines.

    Maintenance is **disabled by default**.  When ``maintenance`` is not passed
    to ``GlueTableManager.write()``, no maintenance is executed.
    """

    enabled: bool = False
    expire_snapshots: bool = True
    snapshot_retention_days: int = 30
    retain_last_snapshots: int = 5
    rewrite_data_files: bool = False
    rewrite_data_files_min_input_files: int = 20
    rewrite_manifests: bool = False


@dataclass
class WriteConfig:
    target_table: str
    write_mode: WriteMode
    load_strategy: LoadStrategy = LoadStrategy.FULL
    merge_keys: List[str] = field(default_factory=list)
    partition_columns: List[str] = field(default_factory=list)
    incremental_column: Optional[str] = None
    incremental_value: Optional[Any] = None
    schema_policy: SchemaPolicy = SchemaPolicy.STRICT
    custom_schema_rules: Optional[Dict[str, Any]] = None
    extra_options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TableMetadata:
    catalog: str
    database: str
    table: str
    full_name: str
    schema: Any = None  # StructType at runtime
    partition_columns: List[str] = field(default_factory=list)
    location: Optional[str] = None
    exists: bool = False


@dataclass
class ExecutionPlan:
    writer_key: WriteMode
    table_metadata: TableMetadata
    config: WriteConfig
    schema_actions: List[str] = field(default_factory=list)


@dataclass
class WriteResult:
    success: bool
    target_table: str
    write_mode: WriteMode
    load_strategy: LoadStrategy
    records_written: int = 0
    schema_changes: Optional[SchemaDiff] = None
    optimization_applied: bool = False
    maintenance_actions: List[str] = field(default_factory=list)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_seconds: float = 0.0
    error: Optional[str] = None
