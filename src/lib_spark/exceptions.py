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
