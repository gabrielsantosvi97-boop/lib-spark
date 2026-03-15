from __future__ import annotations

import logging
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from lib_spark.config import TableMetadata
from lib_spark.exceptions import TableNotFoundError

logger = logging.getLogger("lib_spark.catalog")


class CatalogResolver:
    def __init__(self, spark: SparkSession):
        self._spark = spark

    def parse_table_name(self, full_name: str) -> TableMetadata:
        """Parse a qualified table name into catalog, database, and table parts."""
        parts = full_name.strip().split(".")
        if len(parts) == 3:
            catalog, database, table = parts
        elif len(parts) == 2:
            catalog = None
            database, table = parts
        else:
            raise ValueError(
                f"Invalid table name '{full_name}'. "
                "Expected format: [catalog.]database.table"
            )

        resolved_full_name = full_name
        return TableMetadata(
            catalog=catalog or "",
            database=database,
            table=table,
            full_name=resolved_full_name,
        )

    def table_exists(self, table_name: str) -> bool:
        try:
            parts = table_name.strip().split(".")
            if len(parts) == 3:
                db_part = f"{parts[0]}.{parts[1]}"
                tbl_part = parts[2]
            elif len(parts) == 2:
                db_part = parts[0]
                tbl_part = parts[1]
            else:
                return False
            return self._spark.catalog.tableExists(tbl_part, db_part)
        except Exception:
            try:
                self._spark.table(table_name)
                return True
            except Exception:
                return False

    def resolve(self, table_name: str) -> TableMetadata:
        """Fully resolve table metadata: existence, schema, partition columns."""
        meta = self.parse_table_name(table_name)
        meta.exists = self.table_exists(table_name)

        if not meta.exists:
            raise TableNotFoundError(
                f"Table '{table_name}' not found in the catalog."
            )

        meta.schema = self._get_schema(table_name)
        meta.partition_columns = self._get_partition_columns(table_name)
        meta.location = self._get_location(table_name)

        logger.info(
            "Resolved table '%s': %d columns, partitions=%s",
            table_name,
            len(meta.schema.fields) if meta.schema else 0,
            meta.partition_columns,
        )
        return meta

    def _get_schema(self, table_name: str) -> StructType:
        return self._spark.table(table_name).schema

    def _get_partition_columns(self, table_name: str) -> List[str]:
        """Retrieve partition columns via DESCRIBE TABLE output."""
        try:
            desc_df = self._spark.sql(f"DESCRIBE TABLE {table_name}")
            rows = desc_df.collect()

            partition_section = False
            partition_cols: List[str] = []
            for row in rows:
                col_name = str(row[0]).strip() if row[0] else ""
                if col_name.startswith("# Partition"):
                    partition_section = True
                    continue
                if partition_section:
                    if col_name.startswith("#") or col_name == "":
                        break
                    partition_cols.append(col_name)
            return partition_cols
        except Exception as exc:
            logger.warning(
                "Could not retrieve partition columns for '%s': %s",
                table_name,
                exc,
            )
            return []

    def _get_location(self, table_name: str) -> Optional[str]:
        try:
            detail = self._spark.sql(
                f"DESCRIBE TABLE EXTENDED {table_name}"
            ).collect()
            for row in detail:
                key = str(row[0]).strip().lower() if row[0] else ""
                if key == "location":
                    return str(row[1]).strip()
        except Exception:
            pass
        return None
