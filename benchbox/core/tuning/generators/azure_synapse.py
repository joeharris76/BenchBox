"""Azure Synapse Dedicated SQL Pool DDL Generator.

Generates CREATE TABLE statements with Azure Synapse-specific tuning clauses.

Azure Synapse uses:
- DISTRIBUTION: HASH(column), ROUND_ROBIN, or REPLICATE
- CLUSTERED COLUMNSTORE INDEX: Default index type for analytics
- PARTITION: Range-based partitioning

Example:
    >>> from benchbox.core.tuning.generators.azure_synapse import AzureSynapseDDLGenerator
    >>> generator = AzureSynapseDDLGenerator()
    >>> clauses = generator.generate_tuning_clauses(table_tuning)
    >>> print(clauses.distribute_by)  # "HASH([l_orderkey])"

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
    ColumnNullability,
    TuningClauses,
)

if TYPE_CHECKING:
    from benchbox.core.tuning.interface import (
        PlatformOptimizationConfiguration,
        TableTuning,
    )

logger = logging.getLogger(__name__)


class DistributionType(str, Enum):
    """Azure Synapse distribution types."""

    HASH = "HASH"
    ROUND_ROBIN = "ROUND_ROBIN"
    REPLICATE = "REPLICATE"


class IndexType(str, Enum):
    """Azure Synapse index types."""

    CLUSTERED_COLUMNSTORE = "CLUSTERED COLUMNSTORE INDEX"
    CLUSTERED = "CLUSTERED INDEX"
    HEAP = "HEAP"


class AzureSynapseDDLGenerator(BaseDDLGenerator):
    """DDL generator for Azure Synapse Dedicated SQL Pool physical tuning.

    Generates Azure Synapse table DDL with:
    - DISTRIBUTION: Controls data distribution across nodes
    - INDEX: CLUSTERED COLUMNSTORE (default), CLUSTERED, or HEAP
    - PARTITION: Range-based partitioning

    Azure Synapse Tuning Mapping:
    - DISTRIBUTION → DISTRIBUTION = HASH([column]) or ROUND_ROBIN
    - PARTITIONING → PARTITION ([column] RANGE RIGHT FOR VALUES (...))
    - INDEXING → CLUSTERED COLUMNSTORE/CLUSTERED/HEAP
    - SORTING → Not directly supported (handled by index)

    Example DDL:
        CREATE TABLE lineitem (
            l_orderkey BIGINT NOT NULL,
            l_shipdate DATE
        )
        WITH (
            DISTRIBUTION = HASH([l_orderkey]),
            CLUSTERED COLUMNSTORE INDEX,
            PARTITION ([l_shipdate] RANGE RIGHT FOR VALUES ())
        );
    """

    IDENTIFIER_QUOTE = "["
    IDENTIFIER_QUOTE_END = "]"
    SUPPORTS_IF_NOT_EXISTS = False  # Synapse doesn't support IF NOT EXISTS
    STATEMENT_TERMINATOR = ";"

    SUPPORTED_TUNING_TYPES = frozenset({"distribution", "partitioning", "indexing"})

    def __init__(
        self,
        distribution_default: DistributionType = DistributionType.ROUND_ROBIN,
        index_type: IndexType = IndexType.CLUSTERED_COLUMNSTORE,
    ):
        """Initialize the Azure Synapse DDL generator.

        Args:
            distribution_default: Default distribution type when not specified.
            index_type: Index type to use (default CLUSTERED COLUMNSTORE).
        """
        self._distribution_default = distribution_default
        self._index_type = index_type

    @property
    def platform_name(self) -> str:
        return "azure_synapse"

    def format_qualified_name(
        self,
        table_name: str,
        schema: str | None = None,
    ) -> str:
        """Format table name with Azure Synapse bracket quoting."""
        if schema:
            return f"[{schema}].[{table_name}]"
        return f"[{table_name}]"

    def generate_tuning_clauses(
        self,
        table_tuning: TableTuning | None,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> TuningClauses:
        """Generate Azure Synapse tuning clauses.

        Args:
            table_tuning: Table tuning configuration.
            platform_opts: Platform-specific options.

        Returns:
            TuningClauses with distribute_by (full distribution clause) and partition_by.
        """
        clauses = TuningClauses()

        if not table_tuning:
            clauses.distribute_by = self._distribution_default.value
            return clauses

        from benchbox.core.tuning.interface import TuningType

        # Handle sorting info (not directly supported in Synapse)
        sort_columns = table_tuning.get_columns_by_type(TuningType.SORTING)
        if sort_columns:
            logger.info(
                f"Sorting hint for Azure Synapse table {table_tuning.table_name}: "
                f"{[c.name for c in sort_columns]}. "
                f"Azure Synapse sorting is handled by the index type (CLUSTERED INDEX on specific columns)."
            )

        # Handle clustering info (not directly supported)
        cluster_columns = table_tuning.get_columns_by_type(TuningType.CLUSTERING)
        if cluster_columns:
            logger.info(
                f"Clustering hint for Azure Synapse table {table_tuning.table_name}: "
                f"{[c.name for c in cluster_columns]}. "
                f"Clustering is achieved via DISTRIBUTION and CLUSTERED INDEX."
            )

        # Handle DISTRIBUTION
        distribution_columns = table_tuning.get_columns_by_type(TuningType.DISTRIBUTION)
        if distribution_columns:
            sorted_cols = sorted(distribution_columns, key=lambda c: c.order)
            # Synapse only supports single-column HASH distribution
            dist_col = sorted_cols[0]
            clauses.distribute_by = f"HASH([{dist_col.name}])"
        else:
            clauses.distribute_by = self._distribution_default.value

        # Handle partitioning
        partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
        if partition_columns:
            sorted_cols = sorted(partition_columns, key=lambda c: c.order)
            # Synapse only supports single-column partitioning
            part_col = sorted_cols[0]
            clauses.partition_by = f"[{part_col.name}]"

        return clauses

    def generate_create_table_ddl(
        self,
        table_name: str,
        columns: list[ColumnDefinition],
        tuning: TuningClauses | None = None,
        if_not_exists: bool = False,
        schema: str | None = None,
    ) -> str:
        """Generate Azure Synapse CREATE TABLE statement.

        Args:
            table_name: Table name.
            columns: Column definitions.
            tuning: Tuning clauses from generate_tuning_clauses().
            if_not_exists: Ignored (Synapse doesn't support IF NOT EXISTS).
            schema: Schema name.

        Returns:
            Complete CREATE TABLE DDL string.
        """
        if if_not_exists:
            logger.warning("Azure Synapse doesn't support IF NOT EXISTS clause")

        parts = ["CREATE TABLE"]
        parts.append(self.format_qualified_name(table_name, schema))

        statement = " ".join(parts)

        # Column definitions
        col_list = self.generate_column_list(columns)
        statement = f"{statement}\n(\n    {col_list}\n)"

        # WITH clause for tuning
        with_clauses = []

        if tuning:
            # Distribution (distribute_by contains full clause like "HASH([col])" or "ROUND_ROBIN")
            if tuning.distribute_by:
                with_clauses.append(f"DISTRIBUTION = {tuning.distribute_by}")
            else:
                with_clauses.append(f"DISTRIBUTION = {self._distribution_default.value}")

            # Partitioning
            if tuning.partition_by:
                with_clauses.append(f"PARTITION ({tuning.partition_by} RANGE RIGHT FOR VALUES ())")
        else:
            with_clauses.append(f"DISTRIBUTION = {self._distribution_default.value}")

        # Always add index type
        with_clauses.append(self._index_type.value)

        statement = f"{statement}\nWITH ({', '.join(with_clauses)})"
        statement = f"{statement}{self.STATEMENT_TERMINATOR}"

        return statement

    def generate_column_list(self, columns: list[ColumnDefinition]) -> str:
        """Generate Azure Synapse column list with proper type mapping."""
        col_defs = []
        for column in columns:
            parts = [f"[{column.name}]"]

            # Map to Synapse types
            data_type = self._map_to_synapse_type(column.data_type)
            parts.append(data_type)

            # NULL/NOT NULL constraint
            if column.nullable == ColumnNullability.NOT_NULL:
                parts.append("NOT NULL")
            else:
                parts.append("NULL")

            if column.default_value is not None:
                parts.append(f"DEFAULT {column.default_value}")

            col_defs.append(" ".join(parts))

        return ",\n    ".join(col_defs)

    def get_post_load_statements(
        self,
        table_name: str,
        tuning: TuningClauses | None = None,
        schema: str | None = None,
    ) -> list[str]:
        """Get post-load statements for Azure Synapse.

        Azure Synapse benefits from UPDATE STATISTICS after bulk loading.

        Args:
            table_name: Table name.
            tuning: Tuning clauses.
            schema: Schema name.

        Returns:
            List of UPDATE STATISTICS statements.
        """
        statements = []
        qualified_name = self.format_qualified_name(table_name, schema)

        # UPDATE STATISTICS to help query optimizer
        statements.append(f"UPDATE STATISTICS {qualified_name}")

        return statements

    def _map_to_synapse_type(self, sql_type: str) -> str:
        """Map standard SQL types to Azure Synapse types.

        Args:
            sql_type: Standard SQL type name.

        Returns:
            Synapse-specific type name.
        """
        type_mapping = {
            # Integer types
            "INTEGER": "INT",
            "INT": "INT",
            "BIGINT": "BIGINT",
            "SMALLINT": "SMALLINT",
            "TINYINT": "TINYINT",
            # Floating point
            "FLOAT": "FLOAT",
            "DOUBLE": "FLOAT",  # Synapse uses FLOAT for double precision
            "REAL": "REAL",
            "DOUBLE PRECISION": "FLOAT",
            # Decimal
            "DECIMAL": "DECIMAL(38, 9)",
            "NUMERIC": "NUMERIC(38, 9)",
            # String types
            "VARCHAR": "NVARCHAR(4000)",
            "CHAR": "NCHAR(255)",
            "TEXT": "NVARCHAR(MAX)",
            "STRING": "NVARCHAR(4000)",
            # Date/time
            "DATE": "DATE",
            "TIMESTAMP": "DATETIME2",
            "DATETIME": "DATETIME2",
            "TIME": "TIME",
            # Boolean (Synapse doesn't have native BOOLEAN)
            "BOOLEAN": "BIT",
            "BOOL": "BIT",
        }

        # Check for type with precision (e.g., "DECIMAL(15,2)")
        base_type = sql_type.split("(")[0].upper().strip()

        return type_mapping.get(base_type, sql_type)


__all__ = [
    "AzureSynapseDDLGenerator",
    "DistributionType",
    "IndexType",
]
