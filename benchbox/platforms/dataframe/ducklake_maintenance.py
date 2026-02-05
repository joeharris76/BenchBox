"""DuckLake Maintenance Operations Implementation.

This module implements DataFrame maintenance operations for DuckLake,
providing full ACID compliance for TPC-H RF1/RF2 and TPC-DS maintenance testing.

DuckLake supports:
- INSERT: Append new data with transaction guarantees
- DELETE: Row-level deletes with predicate pushdown
- UPDATE: Row-level updates
- MERGE: Upsert operations (insert or update)

DuckLake provides:
- ACID transactions with snapshot isolation
- Time travel (query historical versions)
- Schema enforcement and evolution
- Native DuckDB integration for optimal performance

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

try:
    import duckdb

    DUCKDB_AVAILABLE = True
except ImportError:
    duckdb = None
    DUCKDB_AVAILABLE = False

try:
    import pyarrow as pa

    PYARROW_AVAILABLE = True
except ImportError:
    pa = None
    PYARROW_AVAILABLE = False

from benchbox.core.dataframe.maintenance_interface import (
    BaseDataFrameMaintenanceOperations,
    DataFrameMaintenanceCapabilities,
    TransactionIsolation,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# DuckLake capabilities - full ACID support like Delta Lake
DUCKLAKE_CAPABILITIES = DataFrameMaintenanceCapabilities(
    platform_name="ducklake",
    supports_insert=True,
    supports_delete=True,
    supports_update=True,
    supports_merge=True,
    supports_transactions=True,
    transaction_isolation=TransactionIsolation.SNAPSHOT,
    supports_partitioned_delete=True,
    supports_row_level_delete=True,
    supports_time_travel=True,
    max_batch_size=1000000,
    notes="Full ACID compliance via DuckLake (DuckDB's native table format)",
)


class DuckLakeMaintenanceOperations(BaseDataFrameMaintenanceOperations):
    """DuckLake maintenance operations implementation.

    Implements full ACID maintenance operations using DuckDB's DuckLake extension:
    - INSERT: Append with transaction guarantees
    - DELETE: Row-level deletes using predicates
    - UPDATE: Row-level updates
    - MERGE: Upsert operations

    DuckLake provides snapshot isolation and ACID guarantees,
    making it suitable for TPC-H and TPC-DS maintenance tests.

    Example:
        ops = DuckLakeMaintenanceOperations()

        # Insert new rows (transactional)
        result = ops.insert_rows(
            table_path="/data/orders",
            dataframe=new_orders_df,
            mode="append"
        )

        # Delete rows (row-level)
        result = ops.delete_rows(
            table_path="/data/orders",
            condition="order_date < '2020-01-01'"
        )

        # Update rows
        result = ops.update_rows(
            table_path="/data/orders",
            condition="status = 'pending'",
            updates={"status": "'cancelled'"}
        )

    Note:
        Requires DuckDB >= 1.2.0 with ducklake extension
    """

    def __init__(self, working_dir: str | Path | None = None) -> None:
        """Initialize DuckLake maintenance operations.

        Args:
            working_dir: Optional working directory for temporary files

        Raises:
            ImportError: If DuckDB is not installed
        """
        super().__init__()

        if not DUCKDB_AVAILABLE:
            raise ImportError(
                "DuckDB is not installed. Install with: pip install duckdb\n"
                "For TPC-H/TPC-DS maintenance tests with DuckLake, install:\n"
                "pip install 'benchbox[duckdb]'"
            )

        if not PYARROW_AVAILABLE:
            raise ImportError(
                "PyArrow is not installed. Install with: pip install pyarrow\n"
                "PyArrow is required for DuckLake operations."
            )

        self.working_dir = Path(working_dir) if working_dir else None
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._ducklake_loaded = False

    def _get_capabilities(self) -> DataFrameMaintenanceCapabilities:
        """Return DuckLake maintenance capabilities.

        Returns:
            DUCKLAKE_CAPABILITIES (full ACID support)
        """
        return DUCKLAKE_CAPABILITIES

    # Note: _convert_to_arrow() is inherited from BaseDataFrameMaintenanceOperations

    def _get_connection(self, table_path: Path | str) -> Any:
        """Get a DuckDB connection with DuckLake extension loaded.

        Args:
            table_path: Path to the DuckLake table directory

        Returns:
            DuckDB connection with DuckLake catalog attached

        Raises:
            RuntimeError: If DuckLake extension cannot be loaded
        """
        table_path = Path(table_path)

        # Create connection
        conn = duckdb.connect(":memory:")

        # Load ducklake extension
        try:
            conn.execute("INSTALL ducklake")
            conn.execute("LOAD ducklake")
        except Exception as e:
            conn.close()
            raise RuntimeError(f"Failed to load DuckLake extension: {e}") from e

        # Attach DuckLake database
        metadata_path = table_path / "metadata.ducklake"
        data_path = table_path / "data"

        if not metadata_path.exists():
            conn.close()
            raise RuntimeError(f"DuckLake metadata not found at {metadata_path}")

        try:
            # Attach DuckLake database with DATA_PATH option
            conn.execute(f"ATTACH 'ducklake:{metadata_path}' AS ducklake_db (DATA_PATH '{data_path}')")
        except Exception as e:
            conn.close()
            raise RuntimeError(f"Failed to attach DuckLake catalog: {e}") from e

        return conn

    def _get_table_name_from_path(self, table_path: Path | str) -> str:
        """Extract table name from the table path.

        Args:
            table_path: Path to the DuckLake table directory

        Returns:
            Table name (directory name)
        """
        return Path(table_path).name

    def _do_insert(
        self,
        table_path: Path | str,
        dataframe: Any,
        partition_columns: list[str] | None,
        mode: str,
    ) -> int:
        """Insert rows using DuckLake.

        Args:
            table_path: Path to the DuckLake table
            dataframe: DataFrame containing rows to insert
            partition_columns: Columns to partition by (not fully supported yet)
            mode: Write mode ("append" or "overwrite")

        Returns:
            Number of rows inserted
        """
        table_path = Path(table_path)
        table_name = self._get_table_name_from_path(table_path)

        # Convert to PyArrow
        arrow_table = self._convert_to_arrow(dataframe)
        row_count = arrow_table.num_rows

        if row_count == 0:
            self.logger.info("No rows to insert")
            return 0

        conn = self._get_connection(table_path)
        try:
            # Register the Arrow table
            conn.register("source_data", arrow_table)

            if mode == "overwrite":
                # Delete all existing data first
                conn.execute(f"DELETE FROM ducklake_db.main.{table_name}")

            # Insert into DuckLake table
            conn.execute(f"INSERT INTO ducklake_db.main.{table_name} SELECT * FROM source_data")

            self.logger.info(f"Inserted {row_count} rows to DuckLake table at {table_path}")
            return row_count

        finally:
            conn.close()

    def _do_delete(
        self,
        table_path: Path | str,
        condition: str | Any,
    ) -> int:
        """Delete rows using DuckLake predicate.

        Args:
            table_path: Path to the DuckLake table
            condition: SQL-like delete predicate (e.g., "id > 100")

        Returns:
            Number of rows deleted
        """
        table_path = Path(table_path)
        table_name = self._get_table_name_from_path(table_path)

        conn = self._get_connection(table_path)
        try:
            # Get row count before delete
            result = conn.execute(f"SELECT COUNT(*) FROM ducklake_db.main.{table_name}").fetchone()
            rows_before = result[0] if result else 0

            # Execute delete with predicate
            conn.execute(f"DELETE FROM ducklake_db.main.{table_name} WHERE {condition}")

            # Get row count after delete
            result = conn.execute(f"SELECT COUNT(*) FROM ducklake_db.main.{table_name}").fetchone()
            rows_after = result[0] if result else 0

            rows_deleted = rows_before - rows_after
            self.logger.info(f"Deleted {rows_deleted} rows from DuckLake table at {table_path}")
            return rows_deleted

        finally:
            conn.close()

    def _do_update(
        self,
        table_path: Path | str,
        condition: str | Any,
        updates: dict[str, Any],
    ) -> int:
        """Update rows using DuckLake.

        Args:
            table_path: Path to the DuckLake table
            condition: SQL-like update predicate
            updates: Column name to new value expression mapping

        Returns:
            Number of rows updated
        """
        table_path = Path(table_path)
        table_name = self._get_table_name_from_path(table_path)

        conn = self._get_connection(table_path)
        try:
            # Build SET clause from updates dictionary
            set_clauses = ", ".join([f"{col} = {val}" for col, val in updates.items()])

            # Count matching rows before update (for return value)
            result = conn.execute(f"SELECT COUNT(*) FROM ducklake_db.main.{table_name} WHERE {condition}").fetchone()
            matching_rows = result[0] if result else 0

            # Execute update
            conn.execute(f"UPDATE ducklake_db.main.{table_name} SET {set_clauses} WHERE {condition}")

            self.logger.info(f"Updated {matching_rows} rows in DuckLake table at {table_path}")
            return matching_rows

        finally:
            conn.close()

    def _do_merge(
        self,
        table_path: Path | str,
        source_dataframe: Any,
        merge_condition: str | Any,
        when_matched: dict[str, Any] | None,
        when_not_matched: dict[str, Any] | None,
    ) -> int:
        """Merge (upsert) rows using DuckLake.

        DuckDB supports MERGE statements which DuckLake can use for upsert operations.

        Args:
            table_path: Path to the DuckLake table
            source_dataframe: Source DataFrame
            merge_condition: Join condition for matching
            when_matched: Updates to apply when matched
            when_not_matched: Inserts when not matched

        Returns:
            Number of rows affected
        """
        table_path = Path(table_path)
        table_name = self._get_table_name_from_path(table_path)

        # Convert source to PyArrow
        source_arrow = self._convert_to_arrow(source_dataframe)
        source_rows = source_arrow.num_rows

        conn = self._get_connection(table_path)
        try:
            # Register source data
            conn.register("source_data", source_arrow)

            # Build MERGE statement
            merge_sql = f"MERGE INTO ducklake_db.main.{table_name} AS target\n"
            merge_sql += "USING source_data AS source\n"
            merge_sql += f"ON {merge_condition}\n"

            if when_matched:
                set_clauses = ", ".join([f"{col} = {val}" for col, val in when_matched.items()])
                merge_sql += f"WHEN MATCHED THEN UPDATE SET {set_clauses}\n"

            if when_not_matched:
                columns = ", ".join(when_not_matched.keys())
                values = ", ".join([str(v) for v in when_not_matched.values()])
                merge_sql += f"WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})"

            # Execute merge
            conn.execute(merge_sql)

            self.logger.info(f"Merged {source_rows} source rows into DuckLake table at {table_path}")
            return source_rows

        finally:
            conn.close()


def get_ducklake_maintenance_operations(
    working_dir: str | Path | None = None,
) -> DuckLakeMaintenanceOperations | None:
    """Get DuckLake maintenance operations if DuckDB is available.

    Args:
        working_dir: Optional working directory

    Returns:
        DuckLakeMaintenanceOperations if DuckDB is available, None otherwise
    """
    if not DUCKDB_AVAILABLE:
        logger.debug("DuckLake maintenance not available (duckdb not installed)")
        return None

    if not PYARROW_AVAILABLE:
        logger.debug("DuckLake maintenance not available (pyarrow not installed)")
        return None

    return DuckLakeMaintenanceOperations(working_dir=working_dir)
