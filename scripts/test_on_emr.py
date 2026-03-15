"""
Script de teste da lib-spark no EMR.
Cria tabelas Iceberg no Glue Catalog, executa operacoes de escrita
e valida os resultados.

Bucket S3 (obrigatorio), por um dos metodos:
    - Variavel de ambiente: LIB_SPARK_TEST_BUCKET=meu-bucket
    - Argumento: python test_on_emr.py meu-bucket

Uso:
    export LIB_SPARK_TEST_BUCKET=meu-bucket
    spark-submit --py-files s3://BUCKET/lib-spark/artifacts/lib_spark-0.1.0-py3-none-any.whl \
        s3://BUCKET/lib-spark/scripts/test_on_emr.py
"""
import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s - %(message)s",
)
logger = logging.getLogger("lib_spark_emr_test")

CATALOG = "glue_catalog"
DATABASE = "lib_spark_test"

def _get_bucket() -> str:
    bucket = (
        os.environ.get("LIB_SPARK_TEST_BUCKET", "").strip()
        or (sys.argv[1].strip() if len(sys.argv) > 1 else "")
    )
    if not bucket:
        raise ValueError(
            "Bucket S3 obrigatorio. Use a variavel LIB_SPARK_TEST_BUCKET ou passe como argumento: "
            "python test_on_emr.py <bucket-name>"
        )
    return bucket

BUCKET = _get_bucket()
WAREHOUSE = f"s3://{BUCKET}/lib-spark/warehouse"


def get_spark():
    return (
        SparkSession.builder
        .appName("lib-spark-emr-test")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def drop_tables(spark):
    """Drop tables from previous runs to ensure clean state."""
    for tbl in ["sample_table", "partitioned_table", "merge_table", "evolution_table"]:
        fq = f"{CATALOG}.{DATABASE}.{tbl}"
        spark.sql(f"DROP TABLE IF EXISTS {fq} PURGE")
    logger.info("Tabelas anteriores removidas")


def create_tables(spark):
    logger.info("=== Criando tabelas Iceberg ===")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE}")

    drop_tables(spark)

    spark.sql(f"""
        CREATE TABLE {CATALOG}.{DATABASE}.sample_table (
            id BIGINT,
            name STRING,
            value DOUBLE
        ) USING iceberg
        LOCATION '{WAREHOUSE}/{DATABASE}/sample_table'
    """)
    logger.info("Tabela sample_table criada")

    spark.sql(f"""
        CREATE TABLE {CATALOG}.{DATABASE}.partitioned_table (
            id BIGINT,
            name STRING,
            year BIGINT
        ) USING iceberg
        PARTITIONED BY (year)
        LOCATION '{WAREHOUSE}/{DATABASE}/partitioned_table'
    """)
    logger.info("Tabela partitioned_table criada")

    spark.sql(f"""
        CREATE TABLE {CATALOG}.{DATABASE}.merge_table (
            id BIGINT,
            name STRING,
            value DOUBLE,
            updated_at STRING
        ) USING iceberg
        LOCATION '{WAREHOUSE}/{DATABASE}/merge_table'
    """)
    logger.info("Tabela merge_table criada")


def test_append(spark, manager):
    from lib_spark import WriteConfig, WriteMode

    logger.info("=== Teste: APPEND ===")
    table = f"{CATALOG}.{DATABASE}.sample_table"

    df = spark.createDataFrame(
        [(1, "Alice", 100.0), (2, "Bob", 200.0), (3, "Carol", 300.0)],
        ["id", "name", "value"],
    )
    result = manager.write(df, WriteConfig(target_table=table, write_mode=WriteMode.APPEND))

    assert result.success, f"APPEND falhou: {result.error}"
    assert result.records_written == 3, f"Esperava 3 registros, recebeu {result.records_written}"

    count = spark.table(table).count()
    assert count == 3, f"Tabela deveria ter 3 registros, tem {count}"

    logger.info("APPEND OK: %d registros escritos", result.records_written)
    return True


def test_overwrite_table(spark, manager):
    from lib_spark import WriteConfig, WriteMode

    logger.info("=== Teste: OVERWRITE_TABLE ===")
    table = f"{CATALOG}.{DATABASE}.sample_table"

    df = spark.createDataFrame(
        [(10, "Dave", 1000.0), (20, "Eve", 2000.0)],
        ["id", "name", "value"],
    )
    result = manager.write(df, WriteConfig(target_table=table, write_mode=WriteMode.OVERWRITE_TABLE))

    assert result.success, f"OVERWRITE_TABLE falhou: {result.error}"
    assert result.records_written == 2

    count = spark.table(table).count()
    assert count == 2, f"Tabela deveria ter 2 registros (overwrite), tem {count}"

    logger.info("OVERWRITE_TABLE OK: %d registros escritos", result.records_written)
    return True


def test_overwrite_partitions(spark, manager):
    from lib_spark import WriteConfig, WriteMode

    logger.info("=== Teste: OVERWRITE_PARTITIONS ===")
    table = f"{CATALOG}.{DATABASE}.partitioned_table"

    df_initial = spark.createDataFrame(
        [(1, "A", 2023), (2, "B", 2023), (3, "C", 2024)],
        ["id", "name", "year"],
    )
    manager.write(df_initial, WriteConfig(target_table=table, write_mode=WriteMode.APPEND))

    df_overwrite = spark.createDataFrame(
        [(10, "X", 2023), (20, "Y", 2023)],
        ["id", "name", "year"],
    )
    result = manager.write(
        df_overwrite,
        WriteConfig(
            target_table=table,
            write_mode=WriteMode.OVERWRITE_PARTITIONS,
            partition_columns=["year"],
        ),
    )

    assert result.success, f"OVERWRITE_PARTITIONS falhou: {result.error}"

    total = spark.table(table).count()
    year_2024 = spark.table(table).filter("year = 2024").count()
    year_2023 = spark.table(table).filter("year = 2023").count()
    assert year_2024 == 1, f"Particao 2024 deveria ter 1 registro, tem {year_2024}"
    assert year_2023 == 2, f"Particao 2023 deveria ter 2 registros, tem {year_2023}"

    logger.info("OVERWRITE_PARTITIONS OK: particao 2023 reescrita, 2024 preservada")
    return True


def test_merge(spark, manager):
    from lib_spark import WriteConfig, WriteMode

    logger.info("=== Teste: MERGE ===")
    table = f"{CATALOG}.{DATABASE}.merge_table"

    df_initial = spark.createDataFrame(
        [(1, "Alice", 100.0, "2024-01-01"), (2, "Bob", 200.0, "2024-01-01")],
        ["id", "name", "value", "updated_at"],
    )
    manager.write(df_initial, WriteConfig(target_table=table, write_mode=WriteMode.APPEND))

    df_upsert = spark.createDataFrame(
        [(2, "Bob Updated", 250.0, "2024-06-01"), (3, "Carol", 300.0, "2024-06-01")],
        ["id", "name", "value", "updated_at"],
    )
    result = manager.write(
        df_upsert,
        WriteConfig(
            target_table=table,
            write_mode=WriteMode.MERGE,
            merge_keys=["id"],
        ),
    )

    assert result.success, f"MERGE falhou: {result.error}"

    total = spark.table(table).count()
    assert total == 3, f"Tabela deveria ter 3 registros apos merge, tem {total}"

    bob = spark.table(table).filter("id = 2").collect()[0]
    assert bob["name"] == "Bob Updated", f"Bob deveria estar atualizado, esta: {bob['name']}"

    logger.info("MERGE OK: upsert de %d registros, total agora %d", result.records_written, total)
    return True


def test_optimization(spark, manager):
    from lib_spark import WriteConfig, WriteMode, WriteOptimizationConfig

    logger.info("=== Teste: OPTIMIZATION com sort_columns ===")
    table = f"{CATALOG}.{DATABASE}.sample_table"

    spark.sql(f"DELETE FROM {table}")

    df = spark.createDataFrame(
        [(i, f"name_{i}", float(i * 10)) for i in range(100)],
        ["id", "name", "value"],
    )

    opt = WriteOptimizationConfig(
        target_file_size_mb=128,
        distribution_mode="hash",
        sort_columns=["id"],
    )
    result = manager.write(df, WriteConfig(target_table=table, write_mode=WriteMode.APPEND), optimization=opt)

    assert result.success, f"OPTIMIZATION falhou: {result.error}"
    assert result.optimization_applied is True

    count = spark.table(table).count()
    assert count == 100

    logger.info("OPTIMIZATION OK: %d registros, optimization_applied=%s", count, result.optimization_applied)
    return True


def test_schema_evolution(spark, manager):
    from lib_spark import SchemaPolicy, WriteConfig, WriteMode

    logger.info("=== Teste: SAFE_SCHEMA_EVOLUTION ===")

    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{DATABASE}.evolution_table PURGE")
    spark.sql(f"""
        CREATE TABLE {CATALOG}.{DATABASE}.evolution_table (
            id BIGINT,
            name STRING
        ) USING iceberg
        LOCATION '{WAREHOUSE}/{DATABASE}/evolution_table'
    """)
    table = f"{CATALOG}.{DATABASE}.evolution_table"

    schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
    ])
    df = spark.createDataFrame([(1, "Alice", "alice@test.com")], schema)

    result = manager.write(
        df,
        WriteConfig(
            target_table=table,
            write_mode=WriteMode.APPEND,
            schema_policy=SchemaPolicy.SAFE_SCHEMA_EVOLUTION,
        ),
    )

    assert result.success, f"SCHEMA_EVOLUTION falhou: {result.error}"
    assert result.schema_changes is not None
    assert len(result.schema_changes.added_columns) > 0

    cols = [f.name for f in spark.table(table).schema.fields]
    assert "email" in cols, f"Coluna 'email' deveria existir, colunas: {cols}"

    logger.info("SCHEMA_EVOLUTION OK: coluna 'email' adicionada automaticamente")
    return True


def main():
    spark = get_spark()
    logger.info("SparkSession iniciada: %s", spark.version)

    from lib_spark import GlueTableManager
    manager = GlueTableManager(spark)

    create_tables(spark)

    tests = [
        ("APPEND", test_append),
        ("OVERWRITE_TABLE", test_overwrite_table),
        ("OVERWRITE_PARTITIONS", test_overwrite_partitions),
        ("MERGE", test_merge),
        ("OPTIMIZATION", test_optimization),
        ("SCHEMA_EVOLUTION", test_schema_evolution),
    ]

    results = []
    for name, test_fn in tests:
        try:
            test_fn(spark, manager)
            results.append((name, "PASSED"))
        except Exception as e:
            logger.error("Teste %s FALHOU: %s", name, e, exc_info=True)
            results.append((name, f"FAILED: {e}"))

    logger.info("")
    logger.info("=" * 60)
    logger.info("RESUMO DOS TESTES")
    logger.info("=" * 60)
    passed = 0
    for name, status in results:
        icon = "PASS" if status == "PASSED" else "FAIL"
        logger.info("  [%s] %s -- %s", icon, name, status)
        if status == "PASSED":
            passed += 1
    logger.info("=" * 60)
    logger.info("Total: %d/%d testes passaram", passed, len(results))
    logger.info("=" * 60)

    spark.stop()

    if passed < len(results):
        sys.exit(1)


if __name__ == "__main__":
    main()
