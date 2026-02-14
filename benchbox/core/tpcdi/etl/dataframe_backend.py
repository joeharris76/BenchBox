"""DataFrame-maintenance-backed TPC-DI ETL backend implementation."""

from __future__ import annotations

from typing import Any

import pandas as pd

from benchbox.core.dataframe.maintenance_interface import DataFrameMaintenanceOperations


class DataFrameETLBackend:
    """TPC-DI ETL backend delegating writes to maintenance operations."""

    def __init__(self, *, maintenance_ops: DataFrameMaintenanceOperations, platform_name: str) -> None:
        self.maintenance_ops = maintenance_ops
        self.platform_name = platform_name

    def create_schema(self) -> None:
        """No-op for DataFrame platforms where table paths are materialized lazily."""
        return

    def load_dataframes(self, staged_data: dict[str, pd.DataFrame], batch_type: str) -> dict[str, Any]:
        """Load DataFrames using maintenance INSERT operations.

        ``batch_type`` is currently reserved for backend-specific partitioning
        or lineage metadata and is intentionally unused in this implementation.
        """
        _ = batch_type
        load_results: dict[str, Any] = {"records_loaded": 0, "tables_updated": []}
        for table_name, dataframe in staged_data.items():
            if dataframe is None or dataframe.empty:
                continue
            result = self.maintenance_ops.insert_rows(table_name, dataframe, mode="append")
            if not result.success:
                raise RuntimeError(result.error_message or f"Failed to insert rows for table {table_name}")
            load_results["records_loaded"] += int(result.rows_affected)
            if int(result.rows_affected) > 0 and table_name not in load_results["tables_updated"]:
                load_results["tables_updated"].append(table_name)
        return load_results

    def validate_results(self) -> dict[str, Any]:
        """Return DataFrame-mode validation metadata.

        DataFrame ETL currently reports validation as not executed rather than
        inferring a synthetic quality score from SQL-only checks.
        """
        return {
            "validation_queries": {},
            "data_quality_issues": [],
            "data_quality_score": 0.0,
            "completeness_checks": {},
            "consistency_checks": {},
            "accuracy_checks": {},
            "validation_level": "not_executed",
            "notes": f"Validation executed in DataFrame mode for platform '{self.platform_name}'",
        }

    def execute_scd2_expire(self, table_name: str, condition: str | Any, updates: dict[str, Any]) -> dict[str, Any]:
        """Expire current rows using UPDATE maintenance operation."""
        result = self.maintenance_ops.update_rows(table_name, condition, updates)
        if not result.success:
            raise RuntimeError(result.error_message or f"Failed to expire rows for table {table_name}")
        return {"success": True, "rows_affected": int(result.rows_affected)}

    def execute_scd2_insert(self, table_name: str, dataframe: pd.DataFrame) -> dict[str, Any]:
        """Insert new SCD2 rows using INSERT maintenance operation."""
        result = self.maintenance_ops.insert_rows(table_name, dataframe, mode="append")
        if not result.success:
            raise RuntimeError(result.error_message or f"Failed to insert SCD2 rows for table {table_name}")
        return {"success": True, "rows_affected": int(result.rows_affected)}

    def read_current_dimension(self, table_name: str) -> pd.DataFrame | None:
        """Return current dimension rows when backend has native read support."""
        _ = table_name
        return None
