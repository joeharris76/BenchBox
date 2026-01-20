"""DuckDB DDL Generator.

Generates CREATE TABLE statements and sorted data loading patterns for DuckDB.

DuckDB does NOT support inline ORDER BY in CREATE TABLE statements (e.g.,
`CREATE TABLE t (...) ORDER BY col` is not valid syntax). Instead, sorted
tables are created by:

1. CREATE TABLE AS SELECT ... ORDER BY - for initial sorted data load
2. Inserting data with ORDER BY in the SELECT source

This generator provides:
- Standard CREATE TABLE DDL for schema definition
- CTAS (CREATE TABLE AS) patterns for sorted table creation
- Hive-style partitioned exports via COPY TO

Example:
    >>> from benchbox.core.tuning.generators.duckdb import DuckDBDDLGenerator
    >>> generator = DuckDBDDLGenerator()
    >>> clauses = generator.generate_tuning_clauses(table_tuning)
    >>> # Get sort clause for CTAS pattern
    >>> if clauses.sort_by:
    ...     print(f"CREATE TABLE sorted_table AS SELECT * FROM src {clauses.sort_by}")
    CREATE TABLE sorted_table AS SELECT * FROM src ORDER BY l_shipdate, l_orderkey

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
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

# Note: DuckDB doesn't have native CREATE TABLE ... ORDER BY syntax.
# Sorting is achieved through CREATE TABLE AS SELECT ... ORDER BY.
# This version constant is kept for API compatibility but no longer affects behavior.
MIN_ORDER_BY_VERSION = (0, 10, 0)


def parse_version(version_str: str) -> tuple[int, ...]:
    """Parse a version string into a tuple of integers.

    Args:
        version_str: Version string like "0.10.2" or "v0.10.2"

    Returns:
        Tuple of version components.
    """
    # Remove 'v' prefix if present
    version_str = version_str.lstrip("v")
    # Handle dev versions like "0.10.2-dev123"
    version_str = version_str.split("-")[0]
    # Parse into integers
    try:
        return tuple(int(x) for x in version_str.split("."))
    except ValueError:
        return (0, 0, 0)


def get_duckdb_version() -> tuple[int, ...]:
    """Get the installed DuckDB version.

    Returns:
        Tuple of version components (major, minor, patch).
    """
    try:
        import duckdb

        version = duckdb.__version__
        return parse_version(version)
    except ImportError:
        return (0, 0, 0)


def supports_order_by() -> bool:
    """Check if DuckDB supports sorted table creation.

    Note: DuckDB doesn't have native CREATE TABLE ... ORDER BY syntax.
    Sorting is achieved via CREATE TABLE AS SELECT ... ORDER BY.
    This function always returns True for modern DuckDB versions.

    Returns:
        True (DuckDB always supports sorted table creation via CTAS).
    """
    # DuckDB has always supported ORDER BY in SELECT statements
    # The "sorting support" is about CTAS patterns, not inline CREATE TABLE syntax
    return True


class DuckDBDDLGenerator(BaseDDLGenerator):
    """DDL generator for DuckDB physical tuning.

    DuckDB doesn't have native CREATE TABLE ... ORDER BY syntax. Instead,
    sorted tables are created through CTAS (CREATE TABLE AS SELECT) patterns.

    Supports:
    - Standard CREATE TABLE DDL for schema definition
    - Sort clauses for use in CTAS patterns (stored in sort_by)
    - Hive-style partitioned exports via COPY TO

    Does NOT support (single-node database):
    - Distribution (not applicable)
    - Clustering (no explicit syntax beyond sorting)

    Usage Pattern:
        The generator produces sort_by clauses that should be appended to
        SELECT statements during data loading:

        ```python
        clauses = generator.generate_tuning_clauses(table_tuning)
        if clauses.sort_by:
            sql = f"CREATE TABLE {table} AS SELECT * FROM source {clauses.sort_by}"
        ```
    """

    IDENTIFIER_QUOTE = '"'
    SUPPORTS_IF_NOT_EXISTS = True
    STATEMENT_TERMINATOR = ";"

    SUPPORTED_TUNING_TYPES = frozenset({"sorting", "partitioning"})

    def __init__(self, check_version: bool = True):
        """Initialize the DuckDB DDL generator.

        Args:
            check_version: Legacy parameter, kept for API compatibility.
                DuckDB sorting is always supported via CTAS patterns.
        """
        self._check_version = check_version
        self._version: tuple[int, ...] | None = None

    @property
    def platform_name(self) -> str:
        return "duckdb"

    @property
    def duckdb_version(self) -> tuple[int, ...]:
        """Get the DuckDB version (cached)."""
        if self._version is None:
            self._version = get_duckdb_version()
        return self._version

    @property
    def supports_order_by_clause(self) -> bool:
        """Check if sorted table creation is supported.

        Note: DuckDB doesn't have inline ORDER BY in CREATE TABLE.
        This property indicates support for sorted CTAS patterns.
        """
        # Always True - DuckDB supports sorting via CTAS
        return True

    def generate_tuning_clauses(
        self,
        table_tuning: TableTuning | None,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> TuningClauses:
        """Generate DuckDB tuning clauses.

        For DuckDB, sorting is achieved via CTAS patterns, not inline CREATE TABLE
        syntax. The sort_by clause should be appended to SELECT statements during
        data loading.

        Args:
            table_tuning: Table tuning configuration.
            platform_opts: Platform-specific options (currently unused for DuckDB).

        Returns:
            TuningClauses with sort_by for CTAS patterns.
        """
        clauses = TuningClauses()

        if not table_tuning:
            return clauses

        from benchbox.core.tuning.interface import TuningType

        # Handle distribution warning (not applicable for DuckDB)
        distribution_columns = table_tuning.get_columns_by_type(TuningType.DISTRIBUTION)
        if distribution_columns:
            logger.warning(
                f"Distribution tuning not applicable for single-node DuckDB "
                f"(table: {table_tuning.table_name}). "
                f"Configured columns {[c.name for c in distribution_columns]} will be ignored."
            )

        # Handle clustering warning (no explicit syntax in DuckDB)
        cluster_columns = table_tuning.get_columns_by_type(TuningType.CLUSTERING)
        if cluster_columns:
            logger.info(
                f"Clustering hint for DuckDB table {table_tuning.table_name}: "
                f"{[c.name for c in cluster_columns]}. "
                f"DuckDB handles clustering automatically based on sorting."
            )

        # Generate sort clause for CTAS patterns
        # DuckDB doesn't support inline ORDER BY in CREATE TABLE, so we use sort_by
        # which should be appended to SELECT statements during data load
        sort_columns = table_tuning.get_columns_by_type(TuningType.SORTING)
        if sort_columns:
            sorted_cols = sorted(sort_columns, key=lambda c: c.order)
            col_names = [c.name for c in sorted_cols]
            # Use sort_by for CTAS patterns: CREATE TABLE t AS SELECT * FROM src ORDER BY ...
            clauses.sort_by = f"ORDER BY {', '.join(col_names)}"

        # Handle partitioning - log for COPY TO usage
        partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
        if partition_columns:
            sorted_cols = sorted(partition_columns, key=lambda c: c.order)
            col_names = [c.name for c in sorted_cols]
            logger.info(
                f"Partitioning hint for DuckDB table {table_tuning.table_name}: "
                f"columns [{', '.join(col_names)}]. "
                f"Use PARTITION_BY in COPY TO for Hive-style partitioned exports."
            )

        return clauses

    def generate_create_table_ddl(
        self,
        table_name: str,
        columns: list[ColumnDefinition],
        tuning: TuningClauses | None = None,
        if_not_exists: bool = False,
        schema: str | None = None,
    ) -> str:
        """Generate DuckDB CREATE TABLE statement.

        Note: DuckDB does NOT support inline ORDER BY in CREATE TABLE.
        This method generates standard CREATE TABLE DDL for schema definition.
        For sorted tables, use CTAS patterns with the sort_by clause from
        generate_tuning_clauses().
        """
        parts = ["CREATE TABLE"]

        if if_not_exists:
            parts.append("IF NOT EXISTS")

        parts.append(self.format_qualified_name(table_name, schema))

        statement = " ".join(parts)

        # Column definitions
        col_list = self.generate_column_list(columns)
        statement = f"{statement} (\n    {col_list}\n)"

        # Note: tuning.order_by is NOT appended here because DuckDB doesn't
        # support inline ORDER BY in CREATE TABLE statements.
        # Sorting is achieved via CTAS: CREATE TABLE t AS SELECT * FROM src ORDER BY ...

        statement = f"{statement}{self.STATEMENT_TERMINATOR}"

        return statement

    def generate_ctas_ddl(
        self,
        table_name: str,
        source_query: str,
        tuning: TuningClauses | None = None,
        if_not_exists: bool = False,
        schema: str | None = None,
    ) -> str:
        """Generate DuckDB CREATE TABLE AS statement with optional sorting.

        This is the recommended pattern for creating sorted tables in DuckDB.

        Args:
            table_name: Target table name.
            source_query: SELECT query providing the data (without ORDER BY).
            tuning: Tuning clauses containing sort_by.
            if_not_exists: Add IF NOT EXISTS clause.
            schema: Schema name for qualified table name.

        Returns:
            CTAS DDL string, e.g., "CREATE TABLE t AS SELECT * FROM src ORDER BY col;"
        """
        parts = ["CREATE"]

        # Note: DuckDB uses CREATE OR REPLACE TABLE, not CREATE TABLE IF NOT EXISTS
        # for CTAS. We'll use CREATE OR REPLACE for the 'if_not_exists' case.
        if if_not_exists:
            parts.append("OR REPLACE")

        parts.append("TABLE")
        parts.append(self.format_qualified_name(table_name, schema))
        parts.append("AS")

        statement = " ".join(parts)
        statement = f"{statement} {source_query}"

        # Add ORDER BY clause for sorted table creation
        if tuning and tuning.sort_by:
            statement = f"{statement} {tuning.sort_by}"

        statement = f"{statement}{self.STATEMENT_TERMINATOR}"

        return statement

    def generate_copy_to_partitioned(
        self,
        source_query: str,
        destination_path: str,
        partition_columns: list[str],
        file_format: str = "PARQUET",
    ) -> str:
        """Generate a COPY TO statement with Hive-style partitioning.

        Args:
            source_query: SELECT query or table name.
            destination_path: Output directory path.
            partition_columns: Columns to partition by.
            file_format: Output format (PARQUET, CSV, etc.).

        Returns:
            COPY TO SQL statement.
        """
        partition_clause = ", ".join(partition_columns)
        return (
            f"COPY ({source_query}) TO '{destination_path}' (FORMAT {file_format}, PARTITION_BY ({partition_clause}))"
        )


__all__ = [
    "DuckDBDDLGenerator",
    "get_duckdb_version",
    "parse_version",
    "supports_order_by",
]
