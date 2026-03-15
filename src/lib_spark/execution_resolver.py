"""Resolve runtime params (full_refresh, date_start, date_end) into ExecutionContext and helpers."""

from __future__ import annotations

import argparse
import logging
from dataclasses import replace
from typing import Any, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from lib_spark.config import (
    ExecutionContext,
    ExecutionMode,
    JobExecutionConfig,
    LoadStrategy,
    RuntimeParams,
    WriteConfig,
)
from lib_spark.validator.validators import (
    validate_job_supports_mode,
    validate_runtime_dates,
)

logger = logging.getLogger("lib_spark.runtime")


def parse_runtime_params_from_argv(
    argv: Optional[List[str]] = None,
) -> RuntimeParams:
    """Parse runtime params from CLI (e.g. spark-submit ... job.py --data_inicio=2025-05-01).

    Accepts:
        --data_inicio=YYYY-MM-DD  (alias: --date_start)
        --data_fim=YYYY-MM-DD     (alias: --date_end)
        --full_refresh=true|false  (alias: --full-refresh)

    Intended for DAGs that pass params into the job; e.g.:
        spark-submit ... meu_job.py --data_inicio=2025-05-01 --data_fim=2025-05-31
    """
    parser = argparse.ArgumentParser(description="Runtime params for lib-spark (DAG)")
    parser.add_argument(
        "--data_inicio",
        "--date_start",
        dest="date_start",
        default="",
        metavar="YYYY-MM-DD",
        help="Start date (FROM_DATE or DATE_RANGE)",
    )
    parser.add_argument(
        "--data_fim",
        "--date_end",
        dest="date_end",
        default="",
        metavar="YYYY-MM-DD",
        help="End date (DATE_RANGE only; requires data_inicio)",
    )
    parser.add_argument(
        "--full_refresh",
        "--full-refresh",
        dest="full_refresh",
        default="false",
        metavar="true|false",
        help="Force full refresh (overrides incremental)",
    )
    args, _ = parser.parse_known_args(argv)
    return RuntimeParams(
        full_refresh=args.full_refresh or None,
        date_start=args.date_start.strip() or None,
        date_end=args.date_end.strip() or None,
    )


def _normalize_bool(value: Any) -> bool:
    """Normalize full_refresh: accept bool or str ('true', '1', 'yes' -> True)."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if not s:
        return False
    return s in ("true", "1", "yes")


def _normalize_date(value: Any) -> Optional[str]:
    """Normalize date string: empty/whitespace -> None, trim, keep YYYY-MM-DD as-is."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    return s


def _resolve_mode(
    full_refresh: bool,
    date_start: Optional[str],
    date_end: Optional[str],
    job_config: JobExecutionConfig,
) -> ExecutionContext:
    """Apply precedence rules and build ExecutionContext. Assumes dates already validated."""
    if full_refresh:
        validate_job_supports_mode(job_config, ExecutionMode.FULL_REFRESH)
        logger.info(
            "Runtime override: full_refresh=true -> execution_mode=FULL_REFRESH (date_start/date_end ignored)."
        )
        return ExecutionContext(
            execution_mode=ExecutionMode.FULL_REFRESH,
            effective_date_start=None,
            effective_date_end=None,
            is_full_refresh=True,
            should_apply_date_filter=False,
        )

    if date_start and date_end:
        validate_job_supports_mode(job_config, ExecutionMode.DATE_RANGE)
        logger.info(
            "Runtime: date_start and date_end -> execution_mode=DATE_RANGE, window [%s, %s].",
            date_start,
            date_end,
        )
        return ExecutionContext(
            execution_mode=ExecutionMode.DATE_RANGE,
            effective_date_start=date_start,
            effective_date_end=date_end,
            is_full_refresh=False,
            should_apply_date_filter=True,
        )

    if date_start:
        validate_job_supports_mode(job_config, ExecutionMode.FROM_DATE)
        logger.info(
            "Runtime: date_start only -> execution_mode=FROM_DATE, from %s.",
            date_start,
        )
        return ExecutionContext(
            execution_mode=ExecutionMode.FROM_DATE,
            effective_date_start=date_start,
            effective_date_end=None,
            is_full_refresh=False,
            should_apply_date_filter=True,
        )

    mode = job_config.default_mode
    logger.info(
        "Runtime: no date params -> execution_mode=%s (job default).",
        mode.value,
    )
    return ExecutionContext(
        execution_mode=mode,
        effective_date_start=None,
        effective_date_end=None,
        is_full_refresh=False,
        should_apply_date_filter=False,
    )


class ExecutionResolver:
    """Resolves raw runtime params + job config into a normalized ExecutionContext."""

    def resolve(
        self,
        params: RuntimeParams,
        job_config: JobExecutionConfig,
    ) -> ExecutionContext:
        """Normalize params, validate, apply precedence rules, return ExecutionContext."""
        full_refresh = _normalize_bool(params.full_refresh)
        date_start = _normalize_date(params.date_start)
        date_end = _normalize_date(params.date_end)

        logger.debug(
            "Runtime params normalized: full_refresh=%s, date_start=%s, date_end=%s",
            full_refresh,
            date_start,
            date_end,
        )

        validate_runtime_dates(date_start, date_end)

        return _resolve_mode(full_refresh, date_start, date_end, job_config)


def apply_runtime_filter(
    df: DataFrame,
    context: ExecutionContext,
    date_column: str,
) -> DataFrame:
    """Apply date filter to DataFrame based on execution context.

    - FULL_REFRESH / INCREMENTAL_DEFAULT: no filter, return df as-is.
    - FROM_DATE: filter where date_column >= effective_date_start.
    - DATE_RANGE: filter where date_column in [effective_date_start, effective_date_end].
    """
    if not context.should_apply_date_filter:
        return df
    if context.execution_mode == ExecutionMode.FROM_DATE and context.effective_date_start:
        return df.filter(col(date_column) >= context.effective_date_start)
    if (
        context.execution_mode == ExecutionMode.DATE_RANGE
        and context.effective_date_start
        and context.effective_date_end
    ):
        return df.filter(
            (col(date_column) >= context.effective_date_start)
            & (col(date_column) <= context.effective_date_end)
        )
    return df


def effective_write_config(
    config: WriteConfig,
    context: ExecutionContext,
) -> WriteConfig:
    """Return a WriteConfig with load_strategy overridden when full_refresh.

    When context.is_full_refresh is True, returns a copy of config with
    load_strategy=LoadStrategy.FULL and incremental_column/incremental_value cleared.
    Otherwise returns config unchanged.
    """
    if not context.is_full_refresh:
        return config
    return replace(
        config,
        load_strategy=LoadStrategy.FULL,
        incremental_column=None,
        incremental_value=None,
    )
