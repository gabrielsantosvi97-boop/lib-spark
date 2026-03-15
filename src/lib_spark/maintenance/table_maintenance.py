from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import List

from pyspark.sql import SparkSession

from lib_spark.config import MaintenanceConfig

logger = logging.getLogger("lib_spark.maintenance")


class TableMaintenance:
    """Runs optional post-write maintenance routines on Iceberg tables."""

    def run(
        self,
        spark: SparkSession,
        table_name: str,
        config: MaintenanceConfig,
    ) -> List[str]:
        if not config.enabled:
            return []

        actions: List[str] = []
        catalog = self._extract_catalog(table_name)
        bare_table = self._bare_table_name(table_name)

        if config.expire_snapshots:
            self._expire_snapshots(spark, catalog, bare_table, config)
            actions.append(
                f"expire_snapshots(retention={config.snapshot_retention_days}d, "
                f"retain_last={config.retain_last_snapshots})"
            )

        if config.rewrite_data_files:
            self._rewrite_data_files(spark, catalog, bare_table, config)
            actions.append(
                f"rewrite_data_files(min_input_files="
                f"{config.rewrite_data_files_min_input_files})"
            )

        if config.rewrite_manifests:
            self._rewrite_manifests(spark, catalog, bare_table)
            actions.append("rewrite_manifests")

        if actions:
            logger.info(
                "Maintenance completed for '%s': %s", table_name, actions
            )

        return actions

    def _expire_snapshots(
        self,
        spark: SparkSession,
        catalog: str,
        bare_table: str,
        config: MaintenanceConfig,
    ) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(
            days=config.snapshot_retention_days
        )
        ts_str = cutoff.strftime("%Y-%m-%d %H:%M:%S.000")

        sql = (
            f"CALL {catalog}.system.expire_snapshots("
            f"table => '{bare_table}', "
            f"older_than => TIMESTAMP '{ts_str}', "
            f"retain_last => {config.retain_last_snapshots})"
        )
        logger.info("Expiring snapshots: %s", sql)
        try:
            spark.sql(sql)
        except Exception as exc:
            logger.warning("expire_snapshots failed for '%s': %s", bare_table, exc)

    def _rewrite_data_files(
        self,
        spark: SparkSession,
        catalog: str,
        bare_table: str,
        config: MaintenanceConfig,
    ) -> None:
        sql = (
            f"CALL {catalog}.system.rewrite_data_files("
            f"table => '{bare_table}', "
            f"options => map('min-input-files', "
            f"'{config.rewrite_data_files_min_input_files}'))"
        )
        logger.info("Rewriting data files: %s", sql)
        try:
            spark.sql(sql)
        except Exception as exc:
            logger.warning(
                "rewrite_data_files failed for '%s': %s", bare_table, exc
            )

    def _rewrite_manifests(
        self,
        spark: SparkSession,
        catalog: str,
        bare_table: str,
    ) -> None:
        sql = (
            f"CALL {catalog}.system.rewrite_manifests("
            f"table => '{bare_table}')"
        )
        logger.info("Rewriting manifests: %s", sql)
        try:
            spark.sql(sql)
        except Exception as exc:
            logger.warning(
                "rewrite_manifests failed for '%s': %s", bare_table, exc
            )

    @staticmethod
    def _extract_catalog(table_name: str) -> str:
        parts = table_name.strip().split(".")
        if len(parts) == 3:
            return parts[0]
        return "spark_catalog"

    @staticmethod
    def _bare_table_name(table_name: str) -> str:
        """Return database.table without the catalog prefix."""
        parts = table_name.strip().split(".")
        if len(parts) == 3:
            return f"{parts[1]}.{parts[2]}"
        return table_name
