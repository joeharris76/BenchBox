"""BigQuery DDL Generator.

Generates CREATE TABLE statements with BigQuery-specific physical tuning:
- Time-based partitioning (DATE, DATETIME, TIMESTAMP)
- Integer range partitioning (RANGE_BUCKET)
- Clustering (up to 4 columns)
- Table options (require_partition_filter, etc.)

Example:
    >>> from benchbox.core.tuning.generators.bigquery import BigQueryDDLGenerator
    >>> generator = BigQueryDDLGenerator()
    >>> clauses = generator.generate_tuning_clauses(table_tuning)
    >>> print(generator.generate_create_table_ddl("lineitem", columns, clauses))
    CREATE TABLE lineitem (
        l_orderkey INT64 NOT NULL,
        l_shipdate DATE,
        ...
    )
    PARTITION BY l_shipdate
    CLUSTER BY l_orderkey, l_partkey
    OPTIONS (require_partition_filter = true);

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


class PartitionGranularity(str, Enum):
    """BigQuery partitioning granularity for time-based partitions."""

    DAY = "DAY"
    MONTH = "MONTH"
    YEAR = "YEAR"
    HOUR = "HOUR"  # Only for DATETIME/TIMESTAMP


class BigQueryDDLGenerator(BaseDDLGenerator):
    """DDL generator for BigQuery physical tuning.

    Supports:
    - PARTITION BY (time-based and integer range)
    - CLUSTER BY (up to 4 columns)
    - OPTIONS (require_partition_filter, description, etc.)

    BigQuery Tuning Notes:
    - Clustering is automatically maintained
    - Max 4 clustering columns
    - Clustering order matters (most selective first)
    - Partitioning recommended for tables > 10TB

    Tuning Configuration Mapping:
    - partitioning → PARTITION BY
    - clustering → CLUSTER BY
    - sorting → Maps to CLUSTER BY (BigQuery uses clustering for sort optimization)
    - distribution → Not applicable (logged as warning)
    """

    IDENTIFIER_QUOTE = "`"
    SUPPORTS_IF_NOT_EXISTS = True
    STATEMENT_TERMINATOR = ";"

    SUPPORTED_TUNING_TYPES = frozenset({"partitioning", "clustering", "sorting"})

    MAX_CLUSTER_COLUMNS = 4

    def __init__(
        self,
        require_partition_filter: bool = False,
        default_partition_granularity: PartitionGranularity = PartitionGranularity.DAY,
    ):
        """Initialize the BigQuery DDL generator.

        Args:
            require_partition_filter: Whether to enforce partition filter in queries.
            default_partition_granularity: Default granularity for time partitions.
        """
        self._require_partition_filter = require_partition_filter
        self._default_granularity = default_partition_granularity

    @property
    def platform_name(self) -> str:
        return "bigquery"

    def generate_tuning_clauses(
        self,
        table_tuning: TableTuning | None,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> TuningClauses:
        """Generate BigQuery tuning clauses.

        Args:
            table_tuning: Table tuning configuration.
            platform_opts: Platform-specific options.

        Returns:
            TuningClauses with partition_by, cluster_by, and table_options.
        """
        clauses = TuningClauses()

        if not table_tuning:
            return clauses

        from benchbox.core.tuning.interface import TuningType

        # Handle distribution warning (not applicable for BigQuery)
        distribution_columns = table_tuning.get_columns_by_type(TuningType.DISTRIBUTION)
        if distribution_columns:
            logger.warning(
                f"Distribution tuning not applicable for BigQuery "
                f"(table: {table_tuning.table_name}). "
                f"BigQuery handles data distribution internally. "
                f"Configured columns {[c.name for c in distribution_columns]} will be ignored."
            )

        # Handle partitioning
        partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
        if partition_columns:
            sorted_cols = sorted(partition_columns, key=lambda c: c.order)
            if len(sorted_cols) > 1:
                logger.warning(
                    f"BigQuery only supports single-column partitioning. "
                    f"Using first column '{sorted_cols[0].name}' from {[c.name for c in sorted_cols]}."
                )

            partition_col = sorted_cols[0]
            partition_clause = self._generate_partition_clause(partition_col, platform_opts)
            clauses.partition_by = partition_clause

            # Add require_partition_filter option if enabled
            if self._require_partition_filter:
                clauses.table_options["require_partition_filter"] = True

        # Handle clustering (from both clustering and sorting config)
        cluster_columns = table_tuning.get_columns_by_type(TuningType.CLUSTERING)
        sort_columns = table_tuning.get_columns_by_type(TuningType.SORTING)

        all_cluster_cols = list(cluster_columns) + [c for c in sort_columns if c not in cluster_columns]

        if all_cluster_cols:
            sorted_cols = sorted(all_cluster_cols, key=lambda c: c.order)

            # Limit to max 4 columns
            if len(sorted_cols) > self.MAX_CLUSTER_COLUMNS:
                logger.warning(
                    f"BigQuery supports max {self.MAX_CLUSTER_COLUMNS} clustering columns. "
                    f"Using first {self.MAX_CLUSTER_COLUMNS} of {len(sorted_cols)} columns."
                )
                sorted_cols = sorted_cols[: self.MAX_CLUSTER_COLUMNS]

            col_names = [c.name for c in sorted_cols]
            clauses.cluster_by = f"CLUSTER BY {', '.join(col_names)}"

        return clauses

    def _generate_partition_clause(
        self,
        partition_col,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> str:
        """Generate PARTITION BY clause based on column type.

        Args:
            partition_col: Partition column with type info.
            platform_opts: Platform-specific options (may contain granularity).

        Returns:
            PARTITION BY clause string.
        """
        col_name = partition_col.name
        col_type = partition_col.type.upper() if partition_col.type else ""

        # Get granularity from platform opts or use default
        granularity = self._default_granularity
        if platform_opts:
            gran_str = getattr(platform_opts, "partition_granularity", None)
            if gran_str:
                try:
                    granularity = PartitionGranularity(gran_str.upper())
                except ValueError:
                    logger.warning(f"Invalid partition_granularity '{gran_str}', using default")

        # Determine partition type based on column type
        if "INT" in col_type:
            # Integer range partitioning requires RANGE_BUCKET
            # Use sensible defaults; can be customized via platform_opts
            range_start = 0
            range_end = 1000000
            range_interval = 10000

            if platform_opts:
                range_start = getattr(platform_opts, "range_start", range_start)
                range_end = getattr(platform_opts, "range_end", range_end)
                range_interval = getattr(platform_opts, "range_interval", range_interval)

            return (
                f"PARTITION BY RANGE_BUCKET({col_name}, GENERATE_ARRAY({range_start}, {range_end}, {range_interval}))"
            )

        elif "TIMESTAMP" in col_type or "DATETIME" in col_type:
            # DATETIME/TIMESTAMP needs DATETIME_TRUNC or TIMESTAMP_TRUNC
            trunc_fn = "TIMESTAMP_TRUNC" if "TIMESTAMP" in col_type else "DATETIME_TRUNC"
            return f"PARTITION BY {trunc_fn}({col_name}, {granularity.value})"

        elif "DATE" in col_type:
            # DATE columns can use direct partitioning for DAY, or DATE_TRUNC for MONTH/YEAR
            if granularity == PartitionGranularity.DAY:
                return f"PARTITION BY {col_name}"
            else:
                return f"PARTITION BY DATE_TRUNC({col_name}, {granularity.value})"

        else:
            # Unknown type - try direct partitioning
            logger.info(
                f"Unknown partition column type '{col_type}' for column '{col_name}'. Using direct PARTITION BY."
            )
            return f"PARTITION BY {col_name}"

    def generate_create_table_ddl(
        self,
        table_name: str,
        columns: list[ColumnDefinition],
        tuning: TuningClauses | None = None,
        if_not_exists: bool = False,
        schema: str | None = None,
    ) -> str:
        """Generate BigQuery CREATE TABLE statement.

        BigQuery DDL structure:
        CREATE TABLE t (...)
        PARTITION BY col
        CLUSTER BY col1, col2
        OPTIONS (...);
        """
        parts = ["CREATE TABLE"]

        if if_not_exists:
            parts.append("IF NOT EXISTS")

        parts.append(self.format_qualified_name(table_name, schema))

        statement = " ".join(parts)

        # Column definitions
        col_list = self.generate_column_list(columns)
        statement = f"{statement} (\n    {col_list}\n)"

        # Add tuning clauses
        if tuning:
            if tuning.partition_by:
                statement = f"{statement}\n{tuning.partition_by}"

            if tuning.cluster_by:
                statement = f"{statement}\n{tuning.cluster_by}"

            # Add OPTIONS clause
            options_clause = tuning.get_table_options_clause()
            if options_clause:
                statement = f"{statement}\n{options_clause}"

        statement = f"{statement}{self.STATEMENT_TERMINATOR}"

        return statement


__all__ = [
    "BigQueryDDLGenerator",
    "PartitionGranularity",
]
