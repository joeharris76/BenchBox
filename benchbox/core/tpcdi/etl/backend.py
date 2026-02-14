"""Backend protocol for TPC-DI ETL load/validate operations."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class TPCDIETLBackend(Protocol):
    """Protocol for mode-specific ETL backend operations."""

    def create_schema(self) -> None:
        """Create target schema or perform equivalent backend setup."""
        ...

    def load_dataframes(self, staged_data: dict[str, pd.DataFrame], batch_type: str) -> dict[str, Any]:
        """Load transformed DataFrames into backend storage."""
        ...

    def validate_results(self) -> dict[str, Any]:
        """Validate ETL results after loading."""
        ...

    def execute_scd2_expire(self, table_name: str, condition: str | Any, updates: dict[str, Any]) -> dict[str, Any]:
        """Expire current dimension rows during SCD2 processing."""
        ...

    def execute_scd2_insert(self, table_name: str, dataframe: pd.DataFrame) -> dict[str, Any]:
        """Insert new dimension rows during SCD2 processing."""
        ...

    def read_current_dimension(self, table_name: str) -> pd.DataFrame | None:
        """Read current dimension rows when backend supports native reads."""
        ...
