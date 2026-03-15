import pytest
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from lib_spark.schema_manager.comparator import compare_schemas


class TestCompareSchemas:
    def test_identical_schemas_no_diff(self):
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        diff = compare_schemas(schema, schema)
        assert not diff.has_differences
        assert diff.is_compatible

    def test_added_column_detected(self):
        source = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
        ])
        target = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        diff = compare_schemas(source, target)
        assert len(diff.added_columns) == 1
        assert diff.added_columns[0].name == "email"
        assert diff.is_compatible

    def test_removed_column_detected(self):
        source = StructType([
            StructField("id", IntegerType(), True),
        ])
        target = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        diff = compare_schemas(source, target)
        assert len(diff.removed_columns) == 1
        assert diff.removed_columns[0].name == "name"
        assert not diff.is_compatible

    def test_safe_int_to_long_promotion(self):
        source = StructType([StructField("id", LongType(), True)])
        target = StructType([StructField("id", IntegerType(), True)])
        diff = compare_schemas(source, target)
        assert len(diff.type_changes) == 1
        assert diff.type_changes[0].is_safe is False  # long->int is NOT safe

    def test_safe_promotion_int_to_long(self):
        source = StructType([StructField("id", IntegerType(), True)])
        target = StructType([StructField("id", LongType(), True)])
        diff = compare_schemas(source, target)
        assert len(diff.type_changes) == 1
        assert diff.type_changes[0].is_safe is True

    def test_safe_float_to_double(self):
        source = StructType([StructField("val", FloatType(), True)])
        target = StructType([StructField("val", DoubleType(), True)])
        diff = compare_schemas(source, target)
        assert len(diff.type_changes) == 1
        assert diff.type_changes[0].is_safe is True

    def test_unsafe_string_to_int(self):
        source = StructType([StructField("id", StringType(), True)])
        target = StructType([StructField("id", IntegerType(), True)])
        diff = compare_schemas(source, target)
        assert len(diff.type_changes) == 1
        assert diff.type_changes[0].is_safe is False
        assert not diff.is_compatible

    def test_safe_decimal_widening(self):
        source = StructType([StructField("val", DecimalType(12, 4), True)])
        target = StructType([StructField("val", DecimalType(10, 2), True)])
        diff = compare_schemas(source, target)
        assert len(diff.type_changes) == 1
        assert diff.type_changes[0].is_safe is True

    def test_case_insensitive_matching(self):
        source = StructType([StructField("ID", IntegerType(), True)])
        target = StructType([StructField("id", IntegerType(), True)])
        diff = compare_schemas(source, target)
        assert not diff.has_differences
