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
        partition_by: Partition columns to order first before sort columns.
        cluster_by: Clustering columns to apply after sort/partition ordering.
        clustering_method: Clustering algorithm ("z_order" or "hilbert").
        output_format: Organized output format ("parquet", "delta", or "iceberg").
        row_group_size: Target row group size in bytes. Controls zone map granularity.
            Smaller row groups = more granular zone maps = better pruning for selective queries.
            Default: 128MB (PyArrow default).
        compression: Parquet compression codec. Default: zstd.
        table_configs: Per-table overrides keyed by table name (e.g., 'lineitem').
            If a table has an entry here, it overrides the top-level sort_columns
            for that table only.
        table_partition_configs: Per-table partition columns keyed by table name.
            If a table has an entry here, it overrides top-level partition_by.
        table_cluster_configs: Per-table clustering columns keyed by table name.
            If a table has an entry here, it overrides top-level cluster_by.
        table_clustering_methods: Per-table clustering methods keyed by table name.
            If a table has an entry here, it overrides top-level clustering_method.
    """

    sort_columns: list[SortColumn] = field(default_factory=list)
    partition_by: list[str] = field(default_factory=list)
    cluster_by: list[str] = field(default_factory=list)
    clustering_method: str = "z_order"
    output_format: str = "parquet"
    row_group_size: int = 128 * 1024 * 1024  # 128MB
    compression: str = "zstd"
    table_configs: dict[str, list[SortColumn]] = field(default_factory=dict)
    table_partition_configs: dict[str, list[str]] = field(default_factory=dict)
    table_cluster_configs: dict[str, list[str]] = field(default_factory=dict)
    table_clustering_methods: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.row_group_size <= 0:
            raise ValueError(f"row_group_size must be positive, got {self.row_group_size}")

        valid_compressions = {"snappy", "gzip", "zstd", "none"}
        if self.compression not in valid_compressions:
            raise ValueError(f"Invalid compression: '{self.compression}'. Must be one of {sorted(valid_compressions)}")

        valid_methods = {"z_order", "hilbert"}
        if self.clustering_method not in valid_methods:
            raise ValueError(
                f"Invalid clustering_method: '{self.clustering_method}'. Must be one of {sorted(valid_methods)}"
            )
        valid_output_formats = {"parquet", "delta", "iceberg"}
        if self.output_format not in valid_output_formats:
            raise ValueError(
                f"Invalid output_format: '{self.output_format}'. Must be one of {sorted(valid_output_formats)}"
            )

    @property
    def has_sorting(self) -> bool:
        """Whether any sorting/partitioning is configured (global or per-table)."""
        return (
            bool(self.sort_columns)
            or bool(self.table_configs)
            or bool(self.partition_by)
            or bool(self.table_partition_configs)
            or bool(self.cluster_by)
            or bool(self.table_cluster_configs)
        )

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

    def get_partition_columns_for_table(self, table_name: str) -> list[str]:
        """Get partition columns for a specific table, with per-table override."""
        return self.table_partition_configs.get(table_name, self.partition_by)

    def get_cluster_columns_for_table(self, table_name: str) -> list[str]:
        """Get clustering columns for a specific table, with per-table override."""
        return self.table_cluster_configs.get(table_name, self.cluster_by)

    def get_clustering_method_for_table(self, table_name: str) -> str:
        """Get clustering method for a specific table, with per-table override."""
        return self.table_clustering_methods.get(table_name, self.clustering_method)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "compression": self.compression,
            "row_group_size": self.row_group_size,
        }
        if self.sort_columns:
            result["sort_columns"] = [sc.to_dict() for sc in self.sort_columns]
        if self.partition_by:
            result["partition_by"] = list(self.partition_by)
        if self.cluster_by:
            result["cluster_by"] = list(self.cluster_by)
        if self.clustering_method != "z_order":
            result["clustering_method"] = self.clustering_method
        if self.output_format != "parquet":
            result["output_format"] = self.output_format
        if self.table_configs:
            result["table_configs"] = {
                table: [sc.to_dict() for sc in cols] for table, cols in self.table_configs.items()
            }
        if self.table_partition_configs:
            result["table_partition_configs"] = {
                table: list(cols) for table, cols in self.table_partition_configs.items()
            }
        if self.table_cluster_configs:
            result["table_cluster_configs"] = {table: list(cols) for table, cols in self.table_cluster_configs.items()}
        if self.table_clustering_methods:
            result["table_clustering_methods"] = dict(self.table_clustering_methods)
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DataOrganizationConfig:
        sort_columns = [SortColumn.from_dict(sc) for sc in data.get("sort_columns", [])]
        table_configs = {}
        for table, cols in data.get("table_configs", {}).items():
            table_configs[table] = [SortColumn.from_dict(sc) for sc in cols]
        return cls(
            sort_columns=sort_columns,
            partition_by=list(data.get("partition_by", [])),
            cluster_by=list(data.get("cluster_by", [])),
            clustering_method=data.get("clustering_method", "z_order"),
            output_format=data.get("output_format", "parquet"),
            row_group_size=data.get("row_group_size", 128 * 1024 * 1024),
            compression=data.get("compression", "zstd"),
            table_configs=table_configs,
            table_partition_configs={
                table: list(cols) for table, cols in data.get("table_partition_configs", {}).items()
            },
            table_cluster_configs={table: list(cols) for table, cols in data.get("table_cluster_configs", {}).items()},
            table_clustering_methods=dict(data.get("table_clustering_methods", {})),
        )
