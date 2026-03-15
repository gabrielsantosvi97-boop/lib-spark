import logging
from pyspark.sql import SparkSession

logger = logging.getLogger("spark_logger")


def get_spark_session(
    session_name,
    account_id,
    aws_region,
    resource_config=None,
    environment=None,
    app_name=None,
):
    """
    Cria e configura a sessão Spark.
    """
    logger.info(f"Starting Spark session - {session_name}")

    spark_conf = {
        "spark.sql.catalogImplementation": "hive",
        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        "spark.hadoop.hive.metastore.glue.catalogid": account_id,
        "spark.sql.extensions": "com.amazonaws.spark.sql.extensions.GlueCatalogExtensions",
        "spark.sql.catalog.glue": "com.amazonaws.spark.sql.catalog.GlueCatalog",
        "spark.sql.defaultCatalog": "glue_catalog",
        "spark.sql.catalog.glue.aws.region": aws_region,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.4.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "spark.sql.legacy.timeParserPolicy": "LEGACY",
        "spark.sql.parquet.int96RebaseModeInRead": "LEGACY",
        "spark.sql.parquet.datetimeRebaseModeInRead": "LEGACY",
        "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
        "spark.sql.parquet.datetimeRebaseModeInWrite": "CORRECTED",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.initialExecutors": "2",
        "spark.dynamicAllocation.maxExecutors": "6",
        "spark.dynamicAllocation.executorIdleTimeout": "60s",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.warehouse": f"s3://localiza-temp-sa-east-1-{account_id}-dev/",
        "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
    }

    if resource_config:
        for key, value in resource_config.items():
            spark_conf[key] = value

    builder = SparkSession.builder.appName(app_name)
    for k, v in spark_conf.items():
        builder = builder.config(k, v)
    
    spark = builder.enableHiveSupport().getOrCreate()
    logger.info(
        f"Spark session started {spark.version} - {spark.sparkContext.applicationId} - {environment}"
    )
    return spark
