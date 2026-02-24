"""Configuration types for data organization.

Defines the configuration model for specifying how generated benchmark data
should be organized (sorted, clustered) when written to Parquet format.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class SortOrder(Enum):
    """Sort direction for a column."""

    ASC = "asc"
    DESC = "desc"

    @classmethod
    def from_string(cls, value: str) -> SortOrder:
        """Parse from string, case-insensitive."""
        try:
            return cls(value.lower())
        except ValueError:
            raise ValueError(f"Invalid sort order: '{value}'. Must be 'asc' or 'desc'.") from None


@dataclass(frozen=True)
class SortColumn:
    """A column to sort by, with direction.

    Attributes:
        name: Column name matching the TPC schema (e.g., 'l_shipdate').
        order: Sort direction. Default: ascending.
    """

    name: str
    order: SortOrder = SortOrder.ASC

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Sort column name cannot be empty")

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "order": self.order.value}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SortColumn:
        if "name" not in data:
            raise ValueError("Sort column requires 'name' field")
        order = SortOrder.from_string(data.get("order", "asc"))
        return cls(name=data["name"], order=order)


@dataclass
class DataOrganizationConfig:
    """Configuration for how to organize generated data.

    Controls sorting, row group sizing, and compression when writing
    Parquet output from TBL source files.

    Attributes:
        sort_columns: Columns to sort by, in priority order. Empty means no sorting.
        row_group_size: Target row group size in bytes. Controls zone map granularity.
            Smaller row groups = more granular zone maps = better pruning for selective queries.
            Default: 128MB (PyArrow default).
        compression: Parquet compression codec. Default: zstd.
        table_configs: Per-table overrides keyed by table name (e.g., 'lineitem').
            If a table has an entry here, it overrides the top-level sort_columns
            for that table only.
    """

    sort_columns: list[SortColumn] = field(default_factory=list)
    row_group_size: int = 128 * 1024 * 1024  # 128MB
    compression: str = "zstd"
    table_configs: dict[str, list[SortColumn]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.row_group_size <= 0:
            raise ValueError(f"row_group_size must be positive, got {self.row_group_size}")

        valid_compressions = {"snappy", "gzip", "zstd", "none"}
        if self.compression not in valid_compressions:
            raise ValueError(f"Invalid compression: '{self.compression}'. Must be one of {sorted(valid_compressions)}")

    @property
    def has_sorting(self) -> bool:
        """Whether any sorting is configured (global or per-table)."""
        return bool(self.sort_columns) or bool(self.table_configs)

    def get_sort_columns_for_table(self, table_name: str) -> list[SortColumn]:
        """Get sort columns for a specific table, with per-table override.

        Args:
            table_name: Table name (e.g., 'lineitem', 'orders').

        Returns:
            Sort columns for this table. Per-table config takes priority
            over global sort_columns. Returns empty list if no sorting
            is configured for this table.
        """
        return self.table_configs.get(table_name, self.sort_columns)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "compression": self.compression,
            "row_group_size": self.row_group_size,
        }
        if self.sort_columns:
            result["sort_columns"] = [sc.to_dict() for sc in self.sort_columns]
        if self.table_configs:
            result["table_configs"] = {
                table: [sc.to_dict() for sc in cols] for table, cols in self.table_configs.items()
            }
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DataOrganizationConfig:
        sort_columns = [SortColumn.from_dict(sc) for sc in data.get("sort_columns", [])]
        table_configs = {}
        for table, cols in data.get("table_configs", {}).items():
            table_configs[table] = [SortColumn.from_dict(sc) for sc in cols]
        return cls(
            sort_columns=sort_columns,
            row_group_size=data.get("row_group_size", 128 * 1024 * 1024),
            compression=data.get("compression", "zstd"),
            table_configs=table_configs,
        )
