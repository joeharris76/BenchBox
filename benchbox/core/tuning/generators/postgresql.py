"""PostgreSQL DDL Generator.

Generates CREATE TABLE statements with PostgreSQL-specific physical tuning:
- PARTITION BY RANGE/LIST/HASH (PostgreSQL 10+)
- CLUSTER command for physical reordering
- Partition child table generation

Example:
    >>> from benchbox.core.tuning.generators.postgresql import PostgreSQLDDLGenerator
    >>> generator = PostgreSQLDDLGenerator()
    >>> clauses = generator.generate_tuning_clauses(table_tuning)
    >>> print(generator.generate_create_table_ddl("orders", columns, clauses))
    CREATE TABLE orders (
        o_orderkey BIGINT NOT NULL,
        o_orderdate DATE NOT NULL
    )
    PARTITION BY RANGE (o_orderdate);

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
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


class PartitionStrategy(str, Enum):
    """PostgreSQL partitioning strategies."""

    RANGE = "RANGE"  # Partition by value ranges
    LIST = "LIST"  # Partition by discrete values
    HASH = "HASH"  # Partition by hash of column(s)


class PostgreSQLDDLGenerator(BaseDDLGenerator):
    """DDL generator for PostgreSQL physical tuning.

    Supports:
    - PARTITION BY RANGE/LIST/HASH (PostgreSQL 10+)
    - CLUSTER command for physical reordering by index
    - Partition child table generation

    PostgreSQL Tuning Notes:
    - Declarative partitioning requires PostgreSQL 10+
    - CLUSTER is a one-time operation (not maintained on inserts)
    - No distribution tuning (single-node database)

    Tuning Configuration Mapping:
    - partitioning → PARTITION BY clause
    - clustering → CLUSTER command (via post_create_statements)
    - sorting → Treated as clustering (creates index + CLUSTER)
    - distribution → Not applicable (logged as warning)
    """

    IDENTIFIER_QUOTE = '"'
    SUPPORTS_IF_NOT_EXISTS = True
    STATEMENT_TERMINATOR = ";"

    SUPPORTED_TUNING_TYPES = frozenset({"partitioning", "clustering", "sorting"})

    def __init__(
        self,
        default_hash_partitions: int = 4,
        default_date_granularity: str = "YEARLY",
    ):
        """Initialize the PostgreSQL DDL generator.

        Args:
            default_hash_partitions: Default number of partitions for HASH.
            default_date_granularity: Default granularity for date partitions
                (DAILY, MONTHLY, YEARLY).
        """
        self._default_hash_partitions = default_hash_partitions
        self._default_date_granularity = default_date_granularity

    @property
    def platform_name(self) -> str:
        return "postgresql"

    def generate_tuning_clauses(
        self,
        table_tuning: TableTuning | None,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> TuningClauses:
        """Generate PostgreSQL tuning clauses.

        Args:
            table_tuning: Table tuning configuration.
            platform_opts: Platform-specific options.

        Returns:
            TuningClauses with partition_by and post_create_statements.
        """
        clauses = TuningClauses()

        if not table_tuning:
            return clauses

        from benchbox.core.tuning.interface import TuningType

        # Handle distribution warning (not applicable for PostgreSQL)
        distribution_columns = table_tuning.get_columns_by_type(TuningType.DISTRIBUTION)
        if distribution_columns:
            logger.warning(
                f"Distribution tuning not applicable for PostgreSQL "
                f"(table: {table_tuning.table_name}). "
                f"PostgreSQL is single-node. "
                f"Configured columns {[c.name for c in distribution_columns]} will be ignored."
            )

        # Handle partitioning
        partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
        if partition_columns:
            sorted_cols = sorted(partition_columns, key=lambda c: c.order)

            # Determine strategy from platform_opts or column type
            strategy = self._determine_partition_strategy(sorted_cols, platform_opts)

            col_names = [c.name for c in sorted_cols]
            clauses.partition_by = f"PARTITION BY {strategy.value} ({', '.join(col_names)})"

        # Handle clustering (creates index + CLUSTER)
        cluster_columns = table_tuning.get_columns_by_type(TuningType.CLUSTERING)
        sort_columns = table_tuning.get_columns_by_type(TuningType.SORTING)

        all_cluster_cols = list(cluster_columns) + [c for c in sort_columns if c not in cluster_columns]

        if all_cluster_cols:
            sorted_cols = sorted(all_cluster_cols, key=lambda c: c.order)
            col_names = [c.name for c in sorted_cols]
            table_name = table_tuning.table_name

            # Generate index name
            index_name = f"idx_{table_name.lower()}_cluster"

            # Add post-create statements for index and CLUSTER
            clauses.post_create_statements.append(
                f"CREATE INDEX IF NOT EXISTS {index_name} ON {{table_name}} ({', '.join(col_names)})"
            )
            clauses.post_create_statements.append(f"CLUSTER {{table_name}} USING {index_name}")

        return clauses

    def _determine_partition_strategy(
        self,
        partition_columns,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> PartitionStrategy:
        """Determine the partitioning strategy based on configuration or column types.

        Args:
            partition_columns: List of partition columns.
            platform_opts: Platform-specific options (may contain strategy).

        Returns:
            PartitionStrategy enum value.
        """
        # Check for explicit strategy in platform_opts
        if platform_opts:
            strategy_str = getattr(platform_opts, "partition_strategy", None)
            if strategy_str:
                try:
                    return PartitionStrategy(strategy_str.upper())
                except ValueError:
                    logger.warning(f"Invalid partition_strategy '{strategy_str}', auto-detecting")

        # Auto-detect from first column type
        if partition_columns:
            first_col = partition_columns[0]
            col_type = first_col.type.upper() if first_col.type else ""

            if "DATE" in col_type or "TIMESTAMP" in col_type:
                return PartitionStrategy.RANGE
            elif "INT" in col_type:
                # Could be either RANGE or HASH - default to HASH for integers
                return PartitionStrategy.HASH
            else:
                # VARCHAR, TEXT, etc. - use LIST
                return PartitionStrategy.LIST

        return PartitionStrategy.RANGE

    def generate_partition_children(
        self,
        parent_table: str,
        columns: list[ColumnDefinition],
        tuning: TuningClauses,
        table_tuning: TableTuning | None = None,
        platform_opts: PlatformOptimizationConfiguration | None = None,
        schema: str | None = None,
    ) -> list[str]:
        """Generate DDL for partition child tables.

        PostgreSQL requires explicit child partition creation after the
        parent table with PARTITION BY.

        Args:
            parent_table: Name of the parent partitioned table.
            columns: Column definitions (for reference).
            tuning: Tuning clauses from generate_tuning_clauses.
            table_tuning: Original table tuning configuration.
            platform_opts: Platform-specific options.
            schema: Optional schema prefix.

        Returns:
            List of CREATE TABLE statements for partition children.
        """
        if not tuning.partition_by or not table_tuning:
            return []

        from benchbox.core.tuning.interface import TuningType

        partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
        if not partition_columns:
            return []

        strategy = self._determine_partition_strategy(partition_columns, platform_opts)
        qualified_parent = self.format_qualified_name(parent_table, schema)

        statements = []

        if strategy == PartitionStrategy.HASH:
            statements = self._generate_hash_partitions(parent_table, qualified_parent, platform_opts)
        elif strategy == PartitionStrategy.RANGE:
            statements = self._generate_range_partitions(
                parent_table, qualified_parent, partition_columns, platform_opts
            )
        elif strategy == PartitionStrategy.LIST:
            statements = self._generate_list_partitions(parent_table, qualified_parent, platform_opts)

        return statements

    def _generate_hash_partitions(
        self,
        parent_table: str,
        qualified_parent: str,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> list[str]:
        """Generate HASH partition child tables."""
        num_partitions = self._default_hash_partitions
        if platform_opts:
            num_partitions = getattr(platform_opts, "hash_partitions", num_partitions)

        statements = []
        for i in range(num_partitions):
            child_name = f"{parent_table}_p{i}"
            statements.append(
                f"CREATE TABLE {child_name} PARTITION OF {qualified_parent} "
                f"FOR VALUES WITH (MODULUS {num_partitions}, REMAINDER {i}){self.STATEMENT_TERMINATOR}"
            )

        return statements

    def _generate_range_partitions(
        self,
        parent_table: str,
        qualified_parent: str,
        partition_columns,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> list[str]:
        """Generate RANGE partition child tables for date columns."""
        # Get granularity
        granularity = self._default_date_granularity
        if platform_opts:
            granularity = getattr(platform_opts, "partition_granularity", granularity)

        # Get date range
        start_year = 2020
        end_year = 2026
        if platform_opts:
            start_year = getattr(platform_opts, "range_start_year", start_year)
            end_year = getattr(platform_opts, "range_end_year", end_year)

        statements = []

        if granularity.upper() == "YEARLY":
            for year in range(start_year, end_year):
                child_name = f"{parent_table}_{year}"
                start_date = f"'{year}-01-01'"
                end_date = f"'{year + 1}-01-01'"
                statements.append(
                    f"CREATE TABLE {child_name} PARTITION OF {qualified_parent} "
                    f"FOR VALUES FROM ({start_date}) TO ({end_date}){self.STATEMENT_TERMINATOR}"
                )
        elif granularity.upper() == "MONTHLY":
            for year in range(start_year, end_year):
                for month in range(1, 13):
                    child_name = f"{parent_table}_{year}_{month:02d}"
                    start_date = date(year, month, 1)
                    end_date = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
                    statements.append(
                        f"CREATE TABLE {child_name} PARTITION OF {qualified_parent} "
                        f"FOR VALUES FROM ('{start_date}') TO ('{end_date}'){self.STATEMENT_TERMINATOR}"
                    )
        elif granularity.upper() == "DAILY":
            # For daily, only generate a sample (last 30 days would be impractical)
            logger.info(
                f"Daily partitioning for {parent_table}: generating sample partitions only. "
                f"Use platform_opts to specify exact date range."
            )
            base_date = date(start_year, 1, 1)
            for i in range(30):  # First 30 days as sample
                current_date = base_date + timedelta(days=i)
                next_date = current_date + timedelta(days=1)
                child_name = f"{parent_table}_{current_date.strftime('%Y_%m_%d')}"
                statements.append(
                    f"CREATE TABLE {child_name} PARTITION OF {qualified_parent} "
                    f"FOR VALUES FROM ('{current_date}') TO ('{next_date}'){self.STATEMENT_TERMINATOR}"
                )

        return statements

    def _generate_list_partitions(
        self,
        parent_table: str,
        qualified_parent: str,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> list[str]:
        """Generate LIST partition child tables."""
        # LIST partitions require explicit values from config
        if not platform_opts:
            logger.warning(
                f"LIST partitioning for {parent_table} requires list_values in platform_opts. "
                f"No partition children generated."
            )
            return []

        list_values = getattr(platform_opts, "list_values", None)
        if not list_values:
            logger.warning(
                f"No list_values provided for LIST partition {parent_table}. No partition children generated."
            )
            return []

        statements = []
        for value in list_values:
            # Sanitize value for table name
            safe_value = str(value).lower().replace(" ", "_").replace("-", "_")
            child_name = f"{parent_table}_{safe_value}"

            # Quote string values
            quoted_value = f"'{value}'" if isinstance(value, str) else str(value)

            statements.append(
                f"CREATE TABLE {child_name} PARTITION OF {qualified_parent} "
                f"FOR VALUES IN ({quoted_value}){self.STATEMENT_TERMINATOR}"
            )

        return statements

    def generate_create_table_ddl(
        self,
        table_name: str,
        columns: list[ColumnDefinition],
        tuning: TuningClauses | None = None,
        if_not_exists: bool = False,
        schema: str | None = None,
    ) -> str:
        """Generate PostgreSQL CREATE TABLE statement.

        PostgreSQL DDL structure for partitioned tables:
        CREATE TABLE t (...)
        PARTITION BY RANGE (col);
        """
        parts = ["CREATE TABLE"]

        if if_not_exists:
            parts.append("IF NOT EXISTS")

        parts.append(self.format_qualified_name(table_name, schema))

        statement = " ".join(parts)

        # Column definitions
        col_list = self.generate_column_list(columns)
        statement = f"{statement} (\n    {col_list}\n)"

        # Add PARTITION BY clause
        if tuning and tuning.partition_by:
            statement = f"{statement}\n{tuning.partition_by}"

        statement = f"{statement}{self.STATEMENT_TERMINATOR}"

        return statement


__all__ = [
    "PartitionStrategy",
    "PostgreSQLDDLGenerator",
]
