"""Tests for runtime params: ExecutionResolver, apply_runtime_filter, effective_write_config."""

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from lib_spark.config import (
    ExecutionContext,
    ExecutionMode,
    JobExecutionConfig,
    LoadStrategy,
    RuntimeParams,
    WriteConfig,
    WriteMode,
)
from lib_spark.exceptions import (
    InvalidRuntimeParamsError,
    UnsupportedExecutionModeError,
)
from lib_spark.execution_resolver import (
    ExecutionResolver,
    parse_runtime_params_from_argv,
    apply_runtime_filter,
    effective_write_config,
)
from lib_spark.validator.validators import validate_runtime_dates


class TestParseRuntimeParamsFromArgv:
    """parse_runtime_params_from_argv() parses CLI args (e.g. from spark-submit)."""

    def test_data_inicio_and_data_fim(self):
        params = parse_runtime_params_from_argv(
            ["--data_inicio=2025-05-01", "--data_fim=2025-05-31"]
        )
        assert params.date_start == "2025-05-01"
        assert params.date_end == "2025-05-31"
        assert params.full_refresh in (None, "false")

    def test_date_start_date_end_aliases(self):
        params = parse_runtime_params_from_argv(
            ["--date_start=2024-01-01", "--date_end=2024-01-15"]
        )
        assert params.date_start == "2024-01-01"
        assert params.date_end == "2024-01-15"

    def test_full_refresh_true(self):
        params = parse_runtime_params_from_argv(["--full_refresh=true"])
        assert params.full_refresh == "true"
        assert params.date_start is None
        assert params.date_end is None

    def test_full_refresh_alias(self):
        params = parse_runtime_params_from_argv(["--full-refresh=yes"])
        assert params.full_refresh == "yes"

    def test_empty_argv_defaults(self):
        params = parse_runtime_params_from_argv([])
        assert params.date_start is None
        assert params.date_end is None
        assert params.full_refresh in ("false", None)


class TestExecutionResolverNoParams:
    """No params -> INCREMENTAL_DEFAULT (or job default)."""

    def test_no_params_incremental_default(self):
        resolver = ExecutionResolver()
        job_config = JobExecutionConfig(default_mode=ExecutionMode.INCREMENTAL_DEFAULT)
        params = RuntimeParams()
        ctx = resolver.resolve(params, job_config)
        assert ctx.execution_mode == ExecutionMode.INCREMENTAL_DEFAULT
        assert ctx.is_full_refresh is False
        assert ctx.should_apply_date_filter is False
        assert ctx.effective_date_start is None
        assert ctx.effective_date_end is None

    def test_none_params_same_as_empty(self):
        resolver = ExecutionResolver()
        job_config = JobExecutionConfig()
        ctx = resolver.resolve(
            RuntimeParams(full_refresh=None, date_start=None, date_end=None),
            job_config,
        )
        assert ctx.execution_mode == ExecutionMode.INCREMENTAL_DEFAULT


class TestExecutionResolverFullRefresh:
    """full_refresh=true -> FULL_REFRESH."""

    def test_full_refresh_bool_true(self):
        resolver = ExecutionResolver()
        job_config = JobExecutionConfig()
        ctx = resolver.resolve(
            RuntimeParams(full_refresh=True, date_start="2024-01-01", date_end="2024-01-31"),
            job_config,
        )
        assert ctx.execution_mode == ExecutionMode.FULL_REFRESH
        assert ctx.is_full_refresh is True
        assert ctx.should_apply_date_filter is False
        assert ctx.effective_date_start is None
        assert ctx.effective_date_end is None

    def test_full_refresh_string_true(self):
        resolver = ExecutionResolver()
        job_config = JobExecutionConfig()
        for val in ("true", "TRUE", "1", "yes"):
            ctx = resolver.resolve(
                RuntimeParams(full_refresh=val, date_start="2024-01-01", date_end=None),
                job_config,
            )
            assert ctx.execution_mode == ExecutionMode.FULL_REFRESH
            assert ctx.is_full_refresh is True

    def test_full_refresh_string_false(self):
        resolver = ExecutionResolver()
        job_config = JobExecutionConfig()
        ctx = resolver.resolve(
            RuntimeParams(full_refresh="false", date_start="2024-01-01", date_end=None),
            job_config,
        )
        assert ctx.execution_mode == ExecutionMode.FROM_DATE
        assert ctx.is_full_refresh is False


class TestExecutionResolverFromDate:
    """Only date_start -> FROM_DATE."""

    def test_date_start_only(self):
        resolver = ExecutionResolver()
        job_config = JobExecutionConfig()
        ctx = resolver.resolve(
            RuntimeParams(full_refresh=False, date_start="2024-06-01", date_end=None),
            job_config,
        )
        assert ctx.execution_mode == ExecutionMode.FROM_DATE
        assert ctx.effective_date_start == "2024-06-01"
        assert ctx.effective_date_end is None
        assert ctx.should_apply_date_filter is True


class TestExecutionResolverDateRange:
    """date_start + date_end -> DATE_RANGE."""

    def test_date_start_and_end(self):
        resolver = ExecutionResolver()
        job_config = JobExecutionConfig()
        ctx = resolver.resolve(
            RuntimeParams(
                full_refresh=False,
                date_start="2024-01-01",
                date_end="2024-01-31",
            ),
            job_config,
        )
        assert ctx.execution_mode == ExecutionMode.DATE_RANGE
        assert ctx.effective_date_start == "2024-01-01"
        assert ctx.effective_date_end == "2024-01-31"
        assert ctx.should_apply_date_filter is True


class TestExecutionResolverValidationFailures:
    """date_end without date_start, date_start > date_end -> fail."""

    def test_date_end_without_date_start_fails(self):
        resolver = ExecutionResolver()
        job_config = JobExecutionConfig()
        with pytest.raises(InvalidRuntimeParamsError) as exc_info:
            resolver.resolve(
                RuntimeParams(full_refresh=False, date_start=None, date_end="2024-01-31"),
                job_config,
            )
        assert "date_end" in str(exc_info.value)
        assert "date_start" in str(exc_info.value)

    def test_date_start_gt_date_end_fails(self):
        resolver = ExecutionResolver()
        job_config = JobExecutionConfig()
        with pytest.raises(InvalidRuntimeParamsError) as exc_info:
            resolver.resolve(
                RuntimeParams(
                    full_refresh=False,
                    date_start="2024-02-01",
                    date_end="2024-01-01",
                ),
                job_config,
            )
        assert "date_start" in str(exc_info.value) or "date_end" in str(exc_info.value)


class TestExecutionResolverUnsupportedMode:
    """Job does not support mode -> UnsupportedExecutionModeError."""

    def test_full_refresh_not_supported_fails(self):
        resolver = ExecutionResolver()
        job_config = JobExecutionConfig(supports_full_refresh=False)
        with pytest.raises(UnsupportedExecutionModeError) as exc_info:
            resolver.resolve(
                RuntimeParams(full_refresh=True, date_start=None, date_end=None),
                job_config,
            )
        assert "full_refresh" in str(exc_info.value).lower()

    def test_from_date_not_supported_fails(self):
        resolver = ExecutionResolver()
        job_config = JobExecutionConfig(supports_from_date=False)
        with pytest.raises(UnsupportedExecutionModeError):
            resolver.resolve(
                RuntimeParams(full_refresh=False, date_start="2024-01-01", date_end=None),
                job_config,
            )

    def test_date_range_not_supported_fails(self):
        resolver = ExecutionResolver()
        job_config = JobExecutionConfig(supports_date_range=False)
        with pytest.raises(UnsupportedExecutionModeError):
            resolver.resolve(
                RuntimeParams(
                    full_refresh=False,
                    date_start="2024-01-01",
                    date_end="2024-01-31",
                ),
                job_config,
            )


class TestValidateRuntimeDates:
    """Direct validator tests."""

    def test_date_end_without_date_start_raises(self):
        with pytest.raises(InvalidRuntimeParamsError):
            validate_runtime_dates(None, "2024-01-31")

    def test_date_start_gt_date_end_raises(self):
        with pytest.raises(InvalidRuntimeParamsError):
            validate_runtime_dates("2024-02-01", "2024-01-01")

    def test_valid_range_passes(self):
        validate_runtime_dates("2024-01-01", "2024-01-31")

    def test_only_start_passes(self):
        validate_runtime_dates("2024-01-01", None)


@pytest.fixture(scope="module")
def spark_for_runtime():
    """Minimal SparkSession for runtime filter tests (no Iceberg/Hadoop)."""
    try:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("test_runtime")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        yield session
        session.stop()
    except Exception:
        pytest.skip("Spark not available (e.g. HADOOP_HOME on Windows)")


class TestApplyRuntimeFilter:
    """apply_runtime_filter behavior per mode."""

    def test_full_refresh_no_filter(self, spark_for_runtime):
        spark = spark_for_runtime
        ctx = ExecutionContext(
            execution_mode=ExecutionMode.FULL_REFRESH,
            is_full_refresh=True,
            should_apply_date_filter=False,
        )
        schema = StructType([StructField("dt", StringType(), True), StructField("x", StringType(), True)])
        df = spark.createDataFrame([("2024-01-01", "a"), ("2024-02-01", "b")], schema)
        out = apply_runtime_filter(df, ctx, "dt")
        try:
            assert out.count() == 2
        except Py4JJavaError:
            pytest.skip("Spark workers unavailable (e.g. Windows driver-only)")

    def test_incremental_default_no_filter(self, spark_for_runtime):
        spark = spark_for_runtime
        ctx = ExecutionContext(
            execution_mode=ExecutionMode.INCREMENTAL_DEFAULT,
            should_apply_date_filter=False,
        )
        schema = StructType([StructField("dt", StringType(), True)])
        df = spark.createDataFrame([("2024-01-01",), ("2024-02-01",)], schema)
        out = apply_runtime_filter(df, ctx, "dt")
        try:
            assert out.count() == 2
        except Py4JJavaError:
            pytest.skip("Spark workers unavailable (e.g. Windows driver-only)")

    def test_from_date_filter(self, spark_for_runtime):
        spark = spark_for_runtime
        ctx = ExecutionContext(
            execution_mode=ExecutionMode.FROM_DATE,
            effective_date_start="2024-02-01",
            should_apply_date_filter=True,
        )
        schema = StructType([StructField("dt", StringType(), True)])
        df = spark.createDataFrame(
            [("2024-01-15",), ("2024-02-01",), ("2024-02-15",), ("2024-03-01",)],
            schema,
        )
        out = apply_runtime_filter(df, ctx, "dt")
        try:
            assert out.count() == 3
            rows = [r["dt"] for r in out.collect()]
            assert "2024-01-15" not in rows
            assert "2024-02-01" in rows
            assert "2024-02-15" in rows
            assert "2024-03-01" in rows
        except Py4JJavaError:
            pytest.skip("Spark workers unavailable (e.g. Windows driver-only)")

    def test_date_range_filter(self, spark_for_runtime):
        spark = spark_for_runtime
        ctx = ExecutionContext(
            execution_mode=ExecutionMode.DATE_RANGE,
            effective_date_start="2024-02-01",
            effective_date_end="2024-02-29",
            should_apply_date_filter=True,
        )
        schema = StructType([StructField("dt", StringType(), True)])
        df = spark.createDataFrame(
            [("2024-01-15",), ("2024-02-01",), ("2024-02-15",), ("2024-03-01",)],
            schema,
        )
        out = apply_runtime_filter(df, ctx, "dt")
        try:
            assert out.count() == 2
            rows = [r["dt"] for r in out.collect()]
            assert "2024-02-01" in rows
            assert "2024-02-15" in rows
        except Py4JJavaError:
            pytest.skip("Spark workers unavailable (e.g. Windows driver-only)")


class TestEffectiveWriteConfig:
    """effective_write_config overrides load_strategy when full_refresh."""

    def test_full_refresh_overrides_to_full(self):
        config = WriteConfig(
            target_table="db.table",
            write_mode=WriteMode.APPEND,
            load_strategy=LoadStrategy.INCREMENTAL,
            incremental_column="dt",
            incremental_value="2024-01-01",
        )
        ctx = ExecutionContext(
            execution_mode=ExecutionMode.FULL_REFRESH,
            is_full_refresh=True,
        )
        out = effective_write_config(config, ctx)
        assert out.load_strategy == LoadStrategy.FULL
        assert out.incremental_column is None
        assert out.incremental_value is None
        assert out.target_table == config.target_table

    def test_not_full_refresh_unchanged(self):
        config = WriteConfig(
            target_table="db.table",
            write_mode=WriteMode.APPEND,
            load_strategy=LoadStrategy.INCREMENTAL,
            incremental_column="dt",
            incremental_value="2024-01-01",
        )
        ctx = ExecutionContext(
            execution_mode=ExecutionMode.INCREMENTAL_DEFAULT,
            is_full_refresh=False,
        )
        out = effective_write_config(config, ctx)
        assert out is config
        assert out.load_strategy == LoadStrategy.INCREMENTAL
        assert out.incremental_column == "dt"
