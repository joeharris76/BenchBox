"""Spark Family DDL Generators.

Standalone DDL generators for Spark-based platforms supporting multiple
table formats: Delta Lake, Apache Iceberg, Apache Parquet, and Hive.

These generators can be used independently of the SparkDDLGeneratorMixin
for dry-run output or external tooling.

Example:
    >>> from benchbox.core.tuning.generators.spark_family import DeltaDDLGenerator
    >>> generator = DeltaDDLGenerator()
    >>> clauses = generator.generate_tuning_clauses(table_tuning)
    >>> print(generator.generate_create_table_ddl("lineitem", columns, clauses))
    CREATE TABLE lineitem (...)
    USING DELTA
    PARTITIONED BY (l_shipdate)
    CLUSTER BY (l_orderkey);

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
from enum import Enum
from typing import TYPE_CHECKING

from benchbox.core.tuning.ddl_generator import (
    BaseDDLGenerator,
    ColumnDefinition,
    TuningClauses,
)

if TYPE_CHECKING:
    from benchbox.core.tuning.interface import (
        PlatformOptimizationConfiguration,
        TableTuning,
    )

logger = logging.getLogger(__name__)


class SparkTableFormat(str, Enum):
    """Supported Spark table formats."""

    DELTA = "delta"
    ICEBERG = "iceberg"
    PARQUET = "parquet"
    HIVE = "hive"


class SparkBaseDDLGenerator(BaseDDLGenerator):
    """Base class for Spark DDL generators with shared functionality.

    Provides common Spark SQL syntax handling used across all table formats.
    """

    IDENTIFIER_QUOTE = "`"
    SUPPORTS_IF_NOT_EXISTS = True
    STATEMENT_TERMINATOR = ";"

    SUPPORTED_TUNING_TYPES = frozenset({"partitioning", "clustering", "sorting", "distribution"})

    # Subclasses set this
    TABLE_FORMAT: str = "parquet"

    # Default bucket count
    DEFAULT_BUCKET_COUNT = 32

    def __init__(self, default_bucket_count: int = 32):
        """Initialize Spark DDL generator.

        Args:
            default_bucket_count: Default number of buckets for distribution.
        """
        self._default_bucket_count = default_bucket_count

    @property
    def platform_name(self) -> str:
        return f"spark-{self.TABLE_FORMAT}"

    def generate_create_table_ddl(
        self,
        table_name: str,
        columns: list[ColumnDefinition],
        tuning: TuningClauses | None = None,
        if_not_exists: bool = False,
        schema: str | None = None,
    ) -> str:
        """Generate Spark CREATE TABLE statement.

        Spark DDL structure:
        CREATE TABLE [IF NOT EXISTS] t (...)
        USING <format>
        PARTITIONED BY (...)
        CLUSTER BY (...) | CLUSTERED BY (...) INTO N BUCKETS
        [SORTED BY (...)]
        [TBLPROPERTIES (...)]
        """
        parts = ["CREATE TABLE"]

        if if_not_exists:
            parts.append("IF NOT EXISTS")

        parts.append(self.format_qualified_name(table_name, schema))

        statement = " ".join(parts)

        # Column definitions
        col_list = self.generate_column_list(columns, include_constraints=False)
        statement = f"{statement} (\n    {col_list}\n)"

        # Add tuning clauses if provided
        if tuning and not tuning.is_empty():
            # USING clause (from additional_clauses)
            for clause in tuning.additional_clauses:
                statement = f"{statement}\n{clause}"

            # Partitioning
            if tuning.partition_by:
                statement = f"{statement}\n{tuning.partition_by}"

            # Clustering (Delta) or CLUSTERED BY (Parquet/Hive)
            if tuning.cluster_by:
                statement = f"{statement}\n{tuning.cluster_by}"
            if tuning.distribute_by:
                statement = f"{statement}\n{tuning.distribute_by}"

            # Sorting
            if tuning.sort_by:
                statement = f"{statement}\n{tuning.sort_by}"

            # Table properties
            if tuning.table_properties:
                props = ", ".join(f"'{k}' = '{v}'" for k, v in sorted(tuning.table_properties.items()))
                statement = f"{statement}\nTBLPROPERTIES ({props})"

        statement = f"{statement}{self.STATEMENT_TERMINATOR}"

        return statement


class DeltaDDLGenerator(SparkBaseDDLGenerator):
    """DDL generator for Delta Lake tables.

    Supports:
    - PARTITIONED BY (col1, col2)
    - CLUSTER BY (col1, col2) - Delta 2.0+ liquid clustering
    - TBLPROPERTIES for auto-optimize
    - Z-ORDER via post-load OPTIMIZE command

    Tuning Configuration Mapping:
    - partitioning → PARTITIONED BY
    - clustering → CLUSTER BY (liquid) or Z-ORDER (legacy)
    - sorting → CLUSTER BY (liquid) or Z-ORDER (legacy)
    - distribution → Z-ORDER
    """

    TABLE_FORMAT = "delta"

    def __init__(
        self,
        use_liquid_clustering: bool = True,
        enable_auto_optimize: bool = True,
        default_bucket_count: int = 32,
    ):
        """Initialize Delta DDL generator.

        Args:
            use_liquid_clustering: Use CLUSTER BY (Delta 2.0+) vs Z-ORDER.
            enable_auto_optimize: Enable autoOptimize table properties.
            default_bucket_count: Default bucket count (for reference).
        """
        super().__init__(default_bucket_count)
        self._use_liquid_clustering = use_liquid_clustering
        self._enable_auto_optimize = enable_auto_optimize

    @property
    def platform_name(self) -> str:
        return "delta"

    def generate_tuning_clauses(
        self,
        table_tuning: TableTuning | None,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> TuningClauses:
        """Generate Delta Lake tuning clauses."""
        clauses = TuningClauses()

        if not table_tuning:
            clauses.additional_clauses.append("USING DELTA")
            return clauses

        from benchbox.core.tuning.interface import TuningType

        # Set table format
        clauses.additional_clauses.append("USING DELTA")

        # Handle partitioning
        partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
        if partition_columns:
            sorted_cols = sorted(partition_columns, key=lambda c: c.order)
            col_names = [c.name for c in sorted_cols]
            clauses.partition_by = f"PARTITIONED BY ({', '.join(col_names)})"

        # Handle clustering (Delta 2.0+ liquid clustering)
        cluster_columns = table_tuning.get_columns_by_type(TuningType.CLUSTERING)
        sort_columns = table_tuning.get_columns_by_type(TuningType.SORTING)

        all_cluster_cols = list(cluster_columns) + [c for c in sort_columns if c not in cluster_columns]

        # Check for liquid clustering setting
        use_liquid = self._use_liquid_clustering
        if platform_opts:
            use_liquid = getattr(platform_opts, "use_liquid_clustering", use_liquid)

        if all_cluster_cols and use_liquid:
            sorted_cols = sorted(all_cluster_cols, key=lambda c: c.order)
            col_names = [c.name for c in sorted_cols]
            clauses.cluster_by = f"CLUSTER BY ({', '.join(col_names)})"
        elif all_cluster_cols:
            # Fall back to Z-ORDER
            sorted_cols = sorted(all_cluster_cols, key=lambda c: c.order)
            col_names = [c.name for c in sorted_cols]
            clauses.post_create_statements.append(f"OPTIMIZE {{table_name}} ZORDER BY ({', '.join(col_names)})")

        # Handle distribution via Z-ORDER
        distribution_columns = table_tuning.get_columns_by_type(TuningType.DISTRIBUTION)
        if distribution_columns and not all_cluster_cols:
            sorted_cols = sorted(distribution_columns, key=lambda c: c.order)
            col_names = [c.name for c in sorted_cols]
            clauses.post_create_statements.append(f"OPTIMIZE {{table_name}} ZORDER BY ({', '.join(col_names)})")

        # Add auto-optimize properties
        enable_auto = self._enable_auto_optimize
        if platform_opts:
            enable_auto = getattr(platform_opts, "enable_auto_optimize", enable_auto)

        if enable_auto:
            clauses.table_properties["delta.autoOptimize.optimizeWrite"] = "true"
            clauses.table_properties["delta.autoOptimize.autoCompact"] = "true"

        return clauses


class IcebergDDLGenerator(SparkBaseDDLGenerator):
    """DDL generator for Apache Iceberg tables.

    Supports partition transforms:
    - years(col), months(col), days(col), hours(col)
    - bucket(N, col)
    - truncate(N, col)
    - identity(col)

    Tuning Configuration Mapping:
    - partitioning → PARTITIONED BY with transforms
    - distribution → bucket() transform
    - sorting → write.sort-order table property
    - clustering → treated same as sorting
    """

    TABLE_FORMAT = "iceberg"

    @property
    def platform_name(self) -> str:
        return "iceberg"

    def generate_tuning_clauses(
        self,
        table_tuning: TableTuning | None,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> TuningClauses:
        """Generate Apache Iceberg tuning clauses."""
        clauses = TuningClauses()

        if not table_tuning:
            clauses.additional_clauses.append("USING ICEBERG")
            return clauses

        from benchbox.core.tuning.interface import TuningType

        # Set table format
        clauses.additional_clauses.append("USING ICEBERG")

        # Handle partitioning with transforms
        partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
        if partition_columns:
            transforms = []
            sorted_cols = sorted(partition_columns, key=lambda c: c.order)

            for col in sorted_cols:
                transform = self._get_iceberg_transform(col, platform_opts)
                transforms.append(transform)

            clauses.partition_by = f"PARTITIONED BY ({', '.join(transforms)})"

        # Handle distribution via bucket transform
        distribution_columns = table_tuning.get_columns_by_type(TuningType.DISTRIBUTION)
        if distribution_columns and not partition_columns:
            sorted_cols = sorted(distribution_columns, key=lambda c: c.order)
            transforms = []

            bucket_count = self._default_bucket_count
            if platform_opts:
                bucket_count = getattr(platform_opts, "bucket_count", bucket_count)

            for col in sorted_cols:
                transforms.append(f"bucket({bucket_count}, {col.name})")

            clauses.partition_by = f"PARTITIONED BY ({', '.join(transforms)})"

        # Handle sorting
        sort_columns = table_tuning.get_columns_by_type(TuningType.SORTING)
        cluster_columns = table_tuning.get_columns_by_type(TuningType.CLUSTERING)

        all_sort_cols = list(sort_columns) + [c for c in cluster_columns if c not in sort_columns]
        if all_sort_cols:
            sorted_cols = sorted(all_sort_cols, key=lambda c: c.order)
            sort_parts = []
            for col in sorted_cols:
                direction = getattr(col, "sort_order", "ASC")
                sort_parts.append(f"{col.name} {direction}")
            clauses.table_properties["write.sort-order"] = ", ".join(sort_parts)

        return clauses

    def _get_iceberg_transform(
        self,
        col,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> str:
        """Get Iceberg partition transform for a column."""
        col_name = col.name
        col_type = col.type.upper() if col.type else ""

        # Check for explicit transform
        if platform_opts:
            col_transforms = getattr(platform_opts, "partition_transforms", {})
            if isinstance(col_transforms, dict) and col_name in col_transforms:
                return col_transforms[col_name]

        # Auto-detect based on column type
        if "DATE" in col_type:
            return f"months({col_name})"
        elif "TIMESTAMP" in col_type:
            return f"days({col_name})"
        elif "INT" in col_type or "BIGINT" in col_type:
            return f"bucket({self._default_bucket_count}, {col_name})"
        else:
            return col_name


class ParquetDDLGenerator(SparkBaseDDLGenerator):
    """DDL generator for Apache Parquet tables.

    Supports:
    - PARTITIONED BY (col1, col2)
    - CLUSTERED BY (col) INTO N BUCKETS
    - SORTED BY (col1, col2)

    Tuning Configuration Mapping:
    - partitioning → PARTITIONED BY
    - distribution → CLUSTERED BY ... INTO N BUCKETS
    - sorting → SORTED BY
    - clustering → treated same as sorting
    """

    TABLE_FORMAT = "parquet"

    @property
    def platform_name(self) -> str:
        return "parquet"

    def generate_tuning_clauses(
        self,
        table_tuning: TableTuning | None,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> TuningClauses:
        """Generate Apache Parquet tuning clauses."""
        clauses = TuningClauses()

        if not table_tuning:
            clauses.additional_clauses.append("USING PARQUET")
            return clauses

        from benchbox.core.tuning.interface import TuningType

        # Set table format
        clauses.additional_clauses.append("USING PARQUET")

        # Handle partitioning
        partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
        if partition_columns:
            sorted_cols = sorted(partition_columns, key=lambda c: c.order)
            col_names = [c.name for c in sorted_cols]
            clauses.partition_by = f"PARTITIONED BY ({', '.join(col_names)})"

        # Handle distribution via CLUSTERED BY
        distribution_columns = table_tuning.get_columns_by_type(TuningType.DISTRIBUTION)
        if distribution_columns:
            sorted_cols = sorted(distribution_columns, key=lambda c: c.order)
            col_names = [c.name for c in sorted_cols]

            bucket_count = self._default_bucket_count
            if platform_opts:
                bucket_count = getattr(platform_opts, "bucket_count", bucket_count)

            clauses.distribute_by = f"CLUSTERED BY ({', '.join(col_names)}) INTO {bucket_count} BUCKETS"

        # Handle sorting
        sort_columns = table_tuning.get_columns_by_type(TuningType.SORTING)
        cluster_columns = table_tuning.get_columns_by_type(TuningType.CLUSTERING)

        all_sort_cols = list(sort_columns) + [c for c in cluster_columns if c not in sort_columns]
        if all_sort_cols:
            sorted_cols = sorted(all_sort_cols, key=lambda c: c.order)
            sort_parts = []
            for col in sorted_cols:
                direction = getattr(col, "sort_order", "ASC")
                nulls = getattr(col, "nulls_position", "DEFAULT")
                part = col.name
                if direction != "ASC":
                    part = f"{part} {direction}"
                if nulls != "DEFAULT":
                    part = f"{part} NULLS {nulls}"
                sort_parts.append(part)
            clauses.sort_by = f"SORTED BY ({', '.join(sort_parts)})"

        return clauses


class HiveDDLGenerator(SparkBaseDDLGenerator):
    """DDL generator for legacy Hive tables.

    Supports:
    - STORED AS (PARQUET, ORC, etc.)
    - PARTITIONED BY (col TYPE)
    - CLUSTERED BY (col) SORTED BY (col) INTO N BUCKETS

    Tuning Configuration Mapping:
    - partitioning → PARTITIONED BY (with types)
    - distribution → CLUSTERED BY ... INTO N BUCKETS
    - sorting → SORTED BY (within CLUSTERED BY)
    - clustering → treated same as sorting
    """

    TABLE_FORMAT = "hive"

    def __init__(
        self,
        storage_format: str = "PARQUET",
        default_bucket_count: int = 32,
    ):
        """Initialize Hive DDL generator.

        Args:
            storage_format: Storage format (PARQUET, ORC, etc.).
            default_bucket_count: Default bucket count.
        """
        super().__init__(default_bucket_count)
        self._storage_format = storage_format

    @property
    def platform_name(self) -> str:
        return "hive"

    def generate_tuning_clauses(
        self,
        table_tuning: TableTuning | None,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> TuningClauses:
        """Generate legacy Hive tuning clauses."""
        clauses = TuningClauses()

        # Set storage format
        storage_format = self._storage_format
        if platform_opts:
            storage_format = getattr(platform_opts, "storage_format", storage_format)
        clauses.additional_clauses.append(f"STORED AS {storage_format}")

        if not table_tuning:
            return clauses

        from benchbox.core.tuning.interface import TuningType

        # Handle partitioning
        partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
        if partition_columns:
            sorted_cols = sorted(partition_columns, key=lambda c: c.order)
            parts = []
            for col in sorted_cols:
                col_type = col.type if col.type else "STRING"
                parts.append(f"{col.name} {col_type}")
            clauses.partition_by = f"PARTITIONED BY ({', '.join(parts)})"

        # Handle distribution and sorting via CLUSTERED BY
        distribution_columns = table_tuning.get_columns_by_type(TuningType.DISTRIBUTION)
        sort_columns = table_tuning.get_columns_by_type(TuningType.SORTING)
        cluster_columns = table_tuning.get_columns_by_type(TuningType.CLUSTERING)

        all_sort_cols = list(sort_columns) + [c for c in cluster_columns if c not in sort_columns]

        if distribution_columns:
            sorted_dist = sorted(distribution_columns, key=lambda c: c.order)
            dist_names = [c.name for c in sorted_dist]

            bucket_count = self._default_bucket_count
            if platform_opts:
                bucket_count = getattr(platform_opts, "bucket_count", bucket_count)

            bucket_clause = f"CLUSTERED BY ({', '.join(dist_names)})"

            if all_sort_cols:
                sorted_sort = sorted(all_sort_cols, key=lambda c: c.order)
                sort_names = [c.name for c in sorted_sort]
                bucket_clause += f" SORTED BY ({', '.join(sort_names)})"

            bucket_clause += f" INTO {bucket_count} BUCKETS"
            clauses.distribute_by = bucket_clause

        return clauses


__all__ = [
    "DeltaDDLGenerator",
    "HiveDDLGenerator",
    "IcebergDDLGenerator",
    "ParquetDDLGenerator",
    "SparkBaseDDLGenerator",
    "SparkTableFormat",
]
