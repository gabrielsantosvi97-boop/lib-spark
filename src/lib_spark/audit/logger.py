from __future__ import annotations

import logging
from datetime import datetime, timezone

from lib_spark.config import (
    LoadStrategy,
    SchemaDiff,
    WriteConfig,
    WriteMode,
    WriteResult,
)

logger = logging.getLogger("lib_spark.audit")


class AuditLogger:
    """Records execution metadata and produces WriteResult objects."""

    def start(self, config: WriteConfig) -> WriteResult:
        """Create an initial WriteResult marking the start of execution."""
        result = WriteResult(
            success=False,
            target_table=config.target_table,
            write_mode=config.write_mode,
            load_strategy=config.load_strategy,
            started_at=datetime.now(timezone.utc),
        )
        logger.info(
            "AUDIT START | table=%s | mode=%s | strategy=%s",
            config.target_table,
            config.write_mode.value,
            config.load_strategy.value,
        )
        return result

    def finish_success(
        self,
        result: WriteResult,
        records_written: int,
        schema_changes: SchemaDiff | None = None,
        optimization_applied: bool = False,
        maintenance_actions: list[str] | None = None,
    ) -> WriteResult:
        now = datetime.now(timezone.utc)
        result.success = True
        result.records_written = records_written
        result.schema_changes = schema_changes
        result.optimization_applied = optimization_applied
        result.maintenance_actions = maintenance_actions or []
        result.finished_at = now
        if result.started_at:
            result.duration_seconds = (
                now - result.started_at
            ).total_seconds()

        mnt_summary = (
            f" | maintenance={result.maintenance_actions}"
            if result.maintenance_actions
            else ""
        )
        logger.info(
            "AUDIT SUCCESS | table=%s | mode=%s | records=%d | "
            "optimized=%s | duration=%.2fs%s",
            result.target_table,
            result.write_mode.value,
            records_written,
            optimization_applied,
            result.duration_seconds,
            mnt_summary,
        )
        return result

    def finish_error(
        self,
        result: WriteResult,
        error: Exception,
    ) -> WriteResult:
        now = datetime.now(timezone.utc)
        result.success = False
        result.error = str(error)
        result.finished_at = now
        if result.started_at:
            result.duration_seconds = (
                now - result.started_at
            ).total_seconds()

        logger.error(
            "AUDIT FAILURE | table=%s | mode=%s | error=%s | duration=%.2fs",
            result.target_table,
            result.write_mode.value,
            error,
            result.duration_seconds,
        )
        return result
