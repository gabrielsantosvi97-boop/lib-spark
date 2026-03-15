class LibSparkError(Exception):
    """Base exception for all lib_spark errors."""


class TableNotFoundError(LibSparkError):
    """Raised when the target table does not exist in the Glue Catalog."""


class SchemaValidationError(LibSparkError):
    """Raised when the DataFrame schema is incompatible with the target table."""


class MergeKeyError(LibSparkError):
    """Raised when merge keys are missing or invalid."""


class PartitionValidationError(LibSparkError):
    """Raised when partition columns are invalid or inconsistent."""


class SchemaEvolutionBlockedError(LibSparkError):
    """Raised when a schema change is blocked by the active policy."""


class InvalidConfigError(LibSparkError):
    """Raised when WriteConfig contains invalid or inconsistent parameters."""


class RuntimeParamsError(LibSparkError):
    """Base exception for runtime parameters (DAG: full_refresh, date_start, date_end)."""


class InvalidRuntimeParamsError(RuntimeParamsError):
    """Raised when runtime parameters are invalid or combined incorrectly (e.g. date_end without date_start, date_start > date_end)."""


class UnsupportedExecutionModeError(RuntimeParamsError):
    """Raised when the resolved execution mode is not supported by the job (e.g. full_refresh=true but job does not support it)."""
