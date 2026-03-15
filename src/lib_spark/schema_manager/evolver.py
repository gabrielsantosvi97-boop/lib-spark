from __future__ import annotations

import logging
from typing import List

from pyspark.sql import SparkSession

from lib_spark.config import SchemaDiff, SchemaPolicy
from lib_spark.exceptions import SchemaEvolutionBlockedError

logger = logging.getLogger("lib_spark.schema_manager")


def apply_evolution(
    spark: SparkSession,
    table_name: str,
    diff: SchemaDiff,
    policy: SchemaPolicy,
) -> List[str]:
    """Apply schema evolution based on policy. Returns list of actions taken."""
    if not diff.has_differences:
        return []

    if policy == SchemaPolicy.STRICT:
        _fail_strict(diff)
        return []

    if policy == SchemaPolicy.FAIL_ON_DIFF:
        _fail_on_diff(diff)
        return []

    if policy == SchemaPolicy.SAFE_SCHEMA_EVOLUTION:
        return _safe_schema_evolution(spark, table_name, diff)

    if policy == SchemaPolicy.CUSTOM:
        return _safe_schema_evolution(spark, table_name, diff)

    return []


def _fail_strict(diff: SchemaDiff) -> None:
    parts = []
    if diff.added_columns:
        cols = [f.name for f in diff.added_columns]
        parts.append(f"New columns in source not in target: {cols}")
    if diff.removed_columns:
        cols = [f.name for f in diff.removed_columns]
        parts.append(f"Columns in target missing from source: {cols}")
    if diff.type_changes:
        changes = [
            f"{tc.column_name}: {tc.source_type} -> {tc.target_type}"
            for tc in diff.type_changes
        ]
        parts.append(f"Type mismatches: {changes}")

    raise SchemaEvolutionBlockedError(
        "STRICT policy: schema differences detected.\n"
        + "\n".join(f"  - {p}" for p in parts)
    )


def _fail_on_diff(diff: SchemaDiff) -> None:
    summary_parts = []
    if diff.added_columns:
        summary_parts.append(f"+{len(diff.added_columns)} columns")
    if diff.removed_columns:
        summary_parts.append(f"-{len(diff.removed_columns)} columns")
    if diff.type_changes:
        summary_parts.append(f"~{len(diff.type_changes)} type changes")

    raise SchemaEvolutionBlockedError(
        f"FAIL_ON_DIFF policy: schema diff detected ({', '.join(summary_parts)}). "
        "No changes applied."
    )


def _safe_schema_evolution(
    spark: SparkSession,
    table_name: str,
    diff: SchemaDiff,
) -> List[str]:
    actions: List[str] = []

    if diff.removed_columns:
        removed_names = [f.name for f in diff.removed_columns]
        logger.warning(
            "Columns present in target but missing from source will NOT be "
            "removed (blocked by default): %s",
            removed_names,
        )

    unsafe_changes = [tc for tc in diff.type_changes if not tc.is_safe]
    if unsafe_changes:
        details = [
            f"{tc.column_name}: {tc.source_type} -> {tc.target_type}"
            for tc in unsafe_changes
        ]
        raise SchemaEvolutionBlockedError(
            "SAFE_SCHEMA_EVOLUTION policy: unsafe type changes detected and blocked:\n"
            + "\n".join(f"  - {d}" for d in details)
        )

    if diff.added_columns:
        for field in diff.added_columns:
            type_str = field.dataType.simpleString()
            nullable_str = "" if field.nullable else " NOT NULL"
            sql = (
                f"ALTER TABLE {table_name} "
                f"ADD COLUMNS ({field.name} {type_str}{nullable_str})"
            )
            logger.info("Applying schema evolution: %s", sql)
            spark.sql(sql)
            actions.append(f"ADD COLUMN {field.name} {type_str}")

    safe_type_changes = [tc for tc in diff.type_changes if tc.is_safe]
    for tc in safe_type_changes:
        new_type = tc.source_type.simpleString()
        sql = (
            f"ALTER TABLE {table_name} "
            f"ALTER COLUMN {tc.column_name} TYPE {new_type}"
        )
        logger.info("Applying safe type evolution: %s", sql)
        spark.sql(sql)
        actions.append(
            f"ALTER COLUMN {tc.column_name} TYPE "
            f"{tc.target_type.simpleString()} -> {new_type}"
        )

    return actions
