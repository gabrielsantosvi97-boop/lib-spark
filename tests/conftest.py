import os
import shutil
import tempfile

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    warehouse_dir = str(tmp_path_factory.mktemp("iceberg_warehouse"))
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("lib_spark_tests")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_dir)
        .config("spark.sql.defaultCatalog", "local")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        )
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture()
def test_db(spark):
    """Create a fresh test database and drop it after the test."""
    db_name = "local.test_db"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    yield db_name
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")


@pytest.fixture()
def sample_table(spark, test_db):
    """Create a simple Iceberg table with id (int), name (string), value (double)."""
    table_name = f"{test_db}.sample_table"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT,
            name STRING,
            value DOUBLE
        ) USING iceberg
    """)
    yield table_name
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.fixture()
def partitioned_table(spark, test_db):
    """Create an Iceberg table partitioned by 'year'."""
    table_name = f"{test_db}.partitioned_table"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT,
            name STRING,
            year INT
        ) USING iceberg
        PARTITIONED BY (year)
    """)
    yield table_name
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
