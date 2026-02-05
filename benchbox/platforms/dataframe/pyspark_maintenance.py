"""PySpark Maintenance Operations Implementation.

This module implements DataFrame maintenance operations for PySpark,
enabling TPC-H RF1/RF2 and TPC-DS maintenance testing.

PySpark supports different capabilities based on the table format:
- With Delta Lake: Full ACID support via delta-spark
  - INSERT: df.write.mode().format("delta").save()
  - DELETE: DeltaTable.forPath().delete()
  - UPDATE: DeltaTable.forPath().update()
  - MERGE: DeltaTable.forPath().merge()

- Plain Parquet: File-level operations only
  - INSERT: df.write.mode().parquet()
  - BULK_LOAD: spark.read().write()
  - No row-level DELETE/UPDATE/MERGE

SCOPE: This implementation supports Delta Lake for row-level operations.
PySpark+Iceberg would require iceberg-spark-runtime and different API patterns,
which should be implemented as a separate module if needed.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

# Check for PySpark availability
try:
    from pyspark.sql import SparkSession

    PYSPARK_AVAILABLE = True
except ImportError:
    SparkSession = None  # type: ignore[assignment, misc]
    PYSPARK_AVAILABLE = False

# Check for Delta Lake (delta-spark) availability
try:
    from delta.tables import DeltaTable

    DELTA_SPARK_AVAILABLE = True
except ImportError:
    DeltaTable = None  # type: ignore[assignment, misc]
    DELTA_SPARK_AVAILABLE = False

from benchbox.core.dataframe.maintenance_interface import (
    BaseDataFrameMaintenanceOperations,
    DataFrameMaintenanceCapabilities,
    TransactionIsolation,
)

logger = logging.getLogger(__name__)


# Capability profiles for PySpark based on table format
PYSPARK_DELTA_CAPABILITIES = DataFrameMaintenanceCapabilities(
    platform_name="pyspark-delta",
    supports_insert=True,
    supports_delete=True,
    supports_update=True,
    supports_merge=True,
    supports_transactions=True,
    transaction_isolation=TransactionIsolation.SNAPSHOT,
    supports_partitioned_delete=True,
    supports_row_level_delete=True,
    supports_time_travel=True,
    max_batch_size=10000000,
    notes="Full ACID compliance via Delta Lake (delta-spark)",
)

PYSPARK_PARQUET_CAPABILITIES = DataFrameMaintenanceCapabilities(
    platform_name="pyspark-parquet",
    supports_insert=True,  # df.write.mode("append")
    supports_delete=False,  # No row-level delete for Parquet
    supports_update=False,  # No row-level update for Parquet
    supports_merge=False,  # No MERGE for Parquet
    supports_transactions=False,
    transaction_isolation=TransactionIsolation.NONE,
    supports_partitioned_delete=True,  # Can overwrite partitions
    supports_row_level_delete=False,
    supports_time_travel=False,
    max_batch_size=10000000,
    notes="File-level operations only. Use Delta Lake for row-level operations.",
)


class PySparkMaintenanceOperations(BaseDataFrameMaintenanceOperations):
    """PySpark maintenance operations implementation.

    Implements maintenance operations for PySpark DataFrames. Row-level operations
    (UPDATE, DELETE, MERGE) require Delta Lake table format.

    Table Format Detection:
        The implementation auto-detects Delta Lake tables by checking for
        _delta_log directory. Tables without Delta Lake support only INSERT
        and BULK_LOAD operations.

    Example:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("benchbox").getOrCreate()
        ops = PySparkMaintenanceOperations(spark_session=spark)

        # Insert new rows (works with any format)
        result = ops.insert_rows(
            table_path="/data/orders",
            dataframe=new_orders_df,
            mode="append"
        )

        # Row-level operations require Delta Lake
        if ops.is_delta_table("/data/orders"):
            result = ops.delete_rows(
                table_path="/data/orders",
                condition="order_date < '2020-01-01'"
            )

    Note:
        Requires pyspark. For row-level operations, also requires delta-spark.
    """

    def __init__(
        self,
        spark_session: Any,
        working_dir: str | Path | None = None,
        prefer_delta: bool = True,
    ) -> None:
        """Initialize PySpark maintenance operations.

        Args:
            spark_session: Active SparkSession instance
            working_dir: Optional working directory for temporary files
            prefer_delta: If True, attempt Delta operations when available

        Raises:
            ImportError: If PySpark is not installed
            ValueError: If spark_session is None
        """
        super().__init__()

        if not PYSPARK_AVAILABLE:
            raise ImportError(
                "PySpark is not installed. Install with: pip install pyspark\n"
                "For Delta Lake support, also install: pip install delta-spark"
            )

        if spark_session is None:
            raise ValueError(
                "spark_session is required. Create one with:\n"
                "  spark = SparkSession.builder.appName('benchbox').getOrCreate()"
            )

        self.spark = spark_session
        self.working_dir = Path(working_dir) if working_dir else None
        self.prefer_delta = prefer_delta and DELTA_SPARK_AVAILABLE
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        if prefer_delta and not DELTA_SPARK_AVAILABLE:
            self.logger.warning(
                "Delta Lake (delta-spark) not available. "
                "Row-level operations (UPDATE/DELETE/MERGE) will not be supported. "
                "Install with: pip install delta-spark"
            )

    def _get_capabilities(self) -> DataFrameMaintenanceCapabilities:
        """Return PySpark maintenance capabilities.

        Returns capabilities based on whether Delta Lake is available.

        Returns:
            PYSPARK_DELTA_CAPABILITIES if delta-spark available,
            PYSPARK_PARQUET_CAPABILITIES otherwise
        """
        if self.prefer_delta and DELTA_SPARK_AVAILABLE:
            return PYSPARK_DELTA_CAPABILITIES
        return PYSPARK_PARQUET_CAPABILITIES

    def is_delta_table(self, table_path: str | Path) -> bool:
        """Check if a path contains a Delta Lake table.

        Args:
            table_path: Path to check

        Returns:
            True if path contains a Delta Lake table (_delta_log exists)
        """
        if not DELTA_SPARK_AVAILABLE:
            return False

        path = Path(table_path)
        delta_log = path / "_delta_log"
        return delta_log.exists() and delta_log.is_dir()

    def _convert_to_spark_df(self, dataframe: Any) -> Any:
        """Convert various DataFrame types to Spark DataFrame.

        Args:
            dataframe: Input DataFrame (PySpark, Pandas, or dict for createDataFrame)

        Returns:
            Spark DataFrame

        Raises:
            TypeError: If dataframe type is not supported
        """
        from pyspark.sql import DataFrame as SparkDataFrame

        # Already a Spark DataFrame
        if isinstance(dataframe, SparkDataFrame):
            return dataframe

        # Pandas DataFrame - convert using spark.createDataFrame
        if hasattr(dataframe, "to_dict") and hasattr(dataframe, "columns"):
            return self.spark.createDataFrame(dataframe)

        # Polars DataFrame - convert via Pandas
        if hasattr(dataframe, "to_pandas"):
            return self.spark.createDataFrame(dataframe.to_pandas())

        # List of dicts or tuples
        if isinstance(dataframe, list):
            return self.spark.createDataFrame(dataframe)

        raise TypeError(
            f"Unsupported DataFrame type: {type(dataframe)}. "
            f"Expected Spark DataFrame, Pandas DataFrame, Polars DataFrame, or list."
        )

    def _to_spark_column(self, value: Any) -> Any:
        """Convert a value to a Spark Column expression.

        Handles conversion of string expressions, column references, and literals
        to proper Spark Column objects for use in update/merge operations.

        Value interpretation:
            - Strings starting with "col:" → Column reference (e.g., "col:source.name")
            - Strings starting with "expr:" → SQL expression (e.g., "expr:amount * 1.1")
            - Strings starting with "lit:" → Literal value (e.g., "lit:completed")
            - Strings starting with "source." → Column reference (shorthand for merge)
            - Other strings → Treated as SQL expressions (for backwards compatibility)
            - Non-strings → Literal values

        Args:
            value: The value to convert. Can be:
                - A string with optional prefix (col:, expr:, lit:)
                - A string SQL expression
                - Any other value (treated as literal)

        Returns:
            A Spark Column object

        Examples:
            >>> ops._to_spark_column("col:source.name")  # Column reference
            >>> ops._to_spark_column("expr:amount * 1.1")  # SQL expression
            >>> ops._to_spark_column("lit:completed")  # Literal string
            >>> ops._to_spark_column("source.id")  # Column reference (merge shorthand)
            >>> ops._to_spark_column(42)  # Literal int
        """
        from pyspark.sql import functions as spark_functions

        if not isinstance(value, str):
            return spark_functions.lit(value)

        # Explicit prefixes for unambiguous interpretation
        if value.startswith("col:"):
            return spark_functions.col(value[4:])
        if value.startswith("expr:"):
            return spark_functions.expr(value[5:])
        if value.startswith("lit:"):
            return spark_functions.lit(value[4:])

        # Shorthand for merge operations: source.column references
        if value.startswith("source.") or value.startswith("target."):
            return spark_functions.col(value)

        # Default: treat as SQL expression for backwards compatibility
        # This handles cases like "amount * 1.1", "UPPER(name)", etc.
        return spark_functions.expr(value)

    def _do_insert(
        self,
        table_path: Path | str,
        dataframe: Any,
        partition_columns: list[str] | None,
        mode: str,
    ) -> int:
        """Insert rows using df.write.mode().format().save().

        Uses the actual Spark DataFrame write API, not SQL execution.

        Args:
            table_path: Path to the table directory
            dataframe: DataFrame containing rows to insert
            partition_columns: Columns to partition by
            mode: Write mode ("append" or "overwrite")

        Returns:
            Number of rows inserted
        """
        table_path = str(table_path)

        # Convert to Spark DataFrame
        spark_df = self._convert_to_spark_df(dataframe)
        row_count = spark_df.count()

        if row_count == 0:
            self.logger.info("No rows to insert")
            return 0

        # Map mode to Spark mode
        spark_mode = "append" if mode == "append" else "overwrite"

        # Determine format
        is_delta = self.is_delta_table(table_path)
        write_format = "delta" if is_delta else "parquet"

        # Build writer
        writer = spark_df.write.mode(spark_mode)

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        # Write using DataFrame API
        if write_format == "delta":
            writer.format("delta").save(table_path)
        else:
            writer.parquet(table_path)

        self.logger.info(f"Inserted {row_count} rows to {table_path} (format: {write_format})")
        return row_count

    def _do_delete(
        self,
        table_path: Path | str,
        condition: str | Any,
    ) -> int:
        """Delete rows using DeltaTable.forPath().delete().

        Only supported for Delta Lake tables. Plain Parquet tables do not
        support row-level deletes.

        Args:
            table_path: Path to the Delta table
            condition: SQL-like delete predicate (e.g., "id > 100")

        Returns:
            Number of rows deleted

        Raises:
            NotImplementedError: If table is not Delta Lake format
        """
        table_path = str(table_path)

        if not DELTA_SPARK_AVAILABLE:
            raise NotImplementedError("DELETE requires Delta Lake (delta-spark). Install with: pip install delta-spark")

        if not self.is_delta_table(table_path):
            raise NotImplementedError(
                f"DELETE requires Delta Lake table format. "
                f"Table at {table_path} is not a Delta table. "
                f"Convert with: df.write.format('delta').save(path)"
            )

        # Get DeltaTable reference
        dt = DeltaTable.forPath(self.spark, table_path)

        # Get row count before delete
        rows_before = dt.toDF().count()

        # Execute delete using DeltaTable API
        dt.delete(condition=str(condition))

        # Get row count after delete
        rows_after = dt.toDF().count()
        rows_deleted = rows_before - rows_after

        self.logger.info(f"Deleted {rows_deleted} rows from {table_path}")
        return rows_deleted

    def _do_update(
        self,
        table_path: Path | str,
        condition: str | Any,
        updates: dict[str, Any],
    ) -> int:
        """Update rows using DeltaTable.forPath().update().

        Only supported for Delta Lake tables. Plain Parquet tables do not
        support row-level updates.

        Args:
            table_path: Path to the Delta table
            condition: SQL-like update predicate
            updates: Column name to new value expression mapping

        Returns:
            Estimated number of rows updated

        Raises:
            NotImplementedError: If table is not Delta Lake format
        """
        table_path = str(table_path)

        if not DELTA_SPARK_AVAILABLE:
            raise NotImplementedError("UPDATE requires Delta Lake (delta-spark). Install with: pip install delta-spark")

        if not self.is_delta_table(table_path):
            raise NotImplementedError(
                f"UPDATE requires Delta Lake table format. "
                f"Table at {table_path} is not a Delta table. "
                f"Convert with: df.write.format('delta').save(path)"
            )

        # Get DeltaTable reference
        dt = DeltaTable.forPath(self.spark, table_path)

        # Count matching rows before update (for return value)
        # Note: DeltaTable.update() doesn't return affected row count
        matching_count = dt.toDF().filter(condition).count()

        if matching_count == 0:
            self.logger.info("No rows match update condition")
            return 0

        # Convert updates dict to Spark Column expressions
        # Uses _to_spark_column() for consistent expression/literal handling
        update_set = {col: self._to_spark_column(value) for col, value in updates.items()}

        # Execute update using DeltaTable API
        dt.update(condition=condition, set=update_set)

        self.logger.info(f"Updated {matching_count} rows in {table_path}")
        return matching_count

    def _do_merge(
        self,
        table_path: Path | str,
        source_dataframe: Any,
        merge_condition: str | Any,
        when_matched: dict[str, Any] | None,
        when_not_matched: dict[str, Any] | None,
    ) -> int:
        """Merge rows using DeltaTable.alias().merge().execute().

        Only supported for Delta Lake tables. Plain Parquet tables do not
        support MERGE operations.

        Args:
            table_path: Path to the Delta table
            source_dataframe: Source DataFrame
            merge_condition: Join condition for matching rows
            when_matched: Updates to apply when matched
            when_not_matched: Inserts when not matched

        Returns:
            Number of rows affected

        Raises:
            NotImplementedError: If table is not Delta Lake format
        """
        table_path = str(table_path)

        if not DELTA_SPARK_AVAILABLE:
            raise NotImplementedError("MERGE requires Delta Lake (delta-spark). Install with: pip install delta-spark")

        if not self.is_delta_table(table_path):
            raise NotImplementedError(
                f"MERGE requires Delta Lake table format. "
                f"Table at {table_path} is not a Delta table. "
                f"Convert with: df.write.format('delta').save(path)"
            )

        # Convert source to Spark DataFrame
        source_df = self._convert_to_spark_df(source_dataframe)
        source_count = source_df.count()

        # Get DeltaTable reference
        dt = DeltaTable.forPath(self.spark, table_path)

        # Build merge operation
        merge_builder = dt.alias("target").merge(source_df.alias("source"), merge_condition)

        # Add when_matched clause
        # Uses _to_spark_column() for consistent expression/literal handling
        if when_matched:
            update_exprs = {col: self._to_spark_column(expr) for col, expr in when_matched.items()}
            merge_builder = merge_builder.whenMatchedUpdate(set=update_exprs)

        # Add when_not_matched clause
        if when_not_matched:
            insert_exprs = {col: self._to_spark_column(expr) for col, expr in when_not_matched.items()}
            merge_builder = merge_builder.whenNotMatchedInsert(values=insert_exprs)

        # Execute merge
        merge_builder.execute()

        # Delta doesn't return exact metrics through Python API by default
        # Estimate based on source size
        self.logger.info(f"Merged from {source_count} source rows into {table_path}")
        return source_count

    def execute_bulk_load(
        self,
        source_path: str | Path,
        target_path: str | Path,
        source_format: str = "parquet",
        target_format: str = "parquet",
        compression: str | None = "zstd",
        partition_columns: list[str] | None = None,
        sort_columns: list[str] | None = None,
    ) -> int:
        """Bulk load data using spark.read().write() pattern.

        This uses pure DataFrame API operations without SQL:
        1. spark.read.format(source_format).load(source_path)
        2. Optional: df.orderBy(sort_columns)
        3. df.write.format(target_format).save(target_path)

        Args:
            source_path: Path to source data
            target_path: Path to write target data
            source_format: Source format ("parquet", "csv", "json")
            target_format: Target format ("parquet", "delta")
            compression: Compression codec ("zstd", "snappy", "gzip", "lz4")
            partition_columns: Columns to partition by
            sort_columns: Columns to sort by before writing

        Returns:
            Number of rows loaded
        """
        source_path = str(source_path)
        target_path = str(target_path)

        # Read source data using DataFrame API
        reader = self.spark.read.format(source_format)

        if source_format == "csv":
            reader = reader.option("header", "true").option("inferSchema", "true")

        df = reader.load(source_path)
        row_count = df.count()

        if row_count == 0:
            self.logger.info("No rows to load")
            return 0

        # Apply sorting
        if sort_columns:
            df = df.orderBy(*sort_columns)

        # Build writer
        writer = df.write.mode("overwrite")

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        if compression:
            writer = writer.option("compression", compression)

        # Write using DataFrame API
        if target_format == "delta":
            writer.format("delta").save(target_path)
        else:
            writer.parquet(target_path)

        self.logger.info(f"Bulk loaded {row_count} rows to {target_path}")
        return row_count


def get_pyspark_maintenance_operations(
    spark_session: Any = None,
    working_dir: str | Path | None = None,
    prefer_delta: bool = True,
) -> PySparkMaintenanceOperations | None:
    """Get PySpark maintenance operations if PySpark is available.

    Args:
        spark_session: Active SparkSession (required)
        working_dir: Optional working directory
        prefer_delta: Whether to prefer Delta Lake operations

    Returns:
        PySparkMaintenanceOperations if PySpark is available and spark_session
        is provided, None otherwise
    """
    if not PYSPARK_AVAILABLE:
        logger.debug("PySpark not available, cannot create maintenance operations")
        return None

    if spark_session is None:
        logger.debug("No SparkSession provided, cannot create maintenance operations")
        return None

    return PySparkMaintenanceOperations(
        spark_session=spark_session,
        working_dir=working_dir,
        prefer_delta=prefer_delta,
    )


__all__ = [
    "PySparkMaintenanceOperations",
    "get_pyspark_maintenance_operations",
    "PYSPARK_DELTA_CAPABILITIES",
    "PYSPARK_PARQUET_CAPABILITIES",
    "PYSPARK_AVAILABLE",
    "DELTA_SPARK_AVAILABLE",
]
