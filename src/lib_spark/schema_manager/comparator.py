from __future__ import annotations

import logging

from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StructType,
)

from lib_spark.config import SchemaDiff, TypeChange

logger = logging.getLogger("lib_spark.schema_manager")

SAFE_PROMOTIONS = {
    ("byte", "short"),
    ("byte", "int"),
    ("byte", "long"),
    ("short", "int"),
    ("short", "long"),
    ("int", "long"),
    ("float", "double"),
}


def _is_safe_decimal_widening(source, target) -> bool:
    if isinstance(source, DecimalType) and isinstance(target, DecimalType):
        return (
            target.precision >= source.precision
            and target.scale >= source.scale
        )
    return False


def _is_safe_promotion(source_type, target_type) -> bool:
    src = source_type.simpleString()
    tgt = target_type.simpleString()

    if src == tgt:
        return True

    if (src, tgt) in SAFE_PROMOTIONS:
        return True

    if _is_safe_decimal_widening(source_type, target_type):
        return True

    return False


def compare_schemas(
    source_schema: StructType,
    target_schema: StructType,
) -> SchemaDiff:
    """Compare source (DataFrame) schema against target (table) schema.

    Returns a SchemaDiff describing added columns, removed columns,
    and type changes with safety classification.
    """
    source_fields = {f.name.lower(): f for f in source_schema.fields}
    target_fields = {f.name.lower(): f for f in target_schema.fields}

    added = [
        source_fields[name]
        for name in source_fields
        if name not in target_fields
    ]

    removed = [
        target_fields[name]
        for name in target_fields
        if name not in source_fields
    ]

    type_changes = []
    for name in source_fields:
        if name in target_fields:
            src_field = source_fields[name]
            tgt_field = target_fields[name]
            if src_field.dataType != tgt_field.dataType:
                safe = _is_safe_promotion(src_field.dataType, tgt_field.dataType)
                type_changes.append(
                    TypeChange(
                        column_name=name,
                        source_type=src_field.dataType,
                        target_type=tgt_field.dataType,
                        is_safe=safe,
                    )
                )

    diff = SchemaDiff(
        added_columns=added,
        removed_columns=removed,
        type_changes=type_changes,
    )

    if diff.has_differences:
        logger.info(
            "Schema diff: +%d added, -%d removed, ~%d type changes (compatible=%s)",
            len(added),
            len(removed),
            len(type_changes),
            diff.is_compatible,
        )

    return diff
