"""QuestDB DDL Generator.

Generates CREATE TABLE statements with QuestDB-specific physical tuning:
- Designated timestamp columns via ``timestamp(col)``
- PARTITION BY for time-series partitioning (DAY, MONTH, YEAR)
- Symbol type mapping for low-cardinality string columns
- Timestamp type mapping for date columns

QuestDB is a high-performance time-series database with a columnar storage
engine optimized for fast ingestion and analytical queries.

Example:
    >>> from benchbox.core.tuning.generators.questdb import QuestDBDDLGenerator
    >>> generator = QuestDBDDLGenerator()
    >>> clauses = generator.generate_tuning_clauses(table_tuning)
    >>> ddl = generator.generate_create_table_ddl("lineitem", columns, clauses)

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

# ──────────────────────────────────────────────────────────────────────
# TPC-H tuning defaults for QuestDB.
# These define the optimal designated timestamp column, partition
# granularity, and symbol columns for each TPC-H table.
# ──────────────────────────────────────────────────────────────────────

# Designated timestamp column per table (None = no timestamp)
TPCH_DESIGNATED_TIMESTAMP: dict[str, str | None] = {
    "lineitem": "l_shipdate",
    "orders": "o_orderdate",
    "partsupp": None,
    "part": None,
    "supplier": None,
    "customer": None,
    "nation": None,
    "region": None,
}

# Partition granularity per table (only for tables with a timestamp)
TPCH_PARTITION_BY: dict[str, str] = {
    "lineitem": "MONTH",
    "orders": "MONTH",
}

# Columns that should use QuestDB ``symbol`` type instead of VARCHAR
TPCH_SYMBOL_COLUMNS: dict[str, list[str]] = {
    "lineitem": ["l_returnflag", "l_linestatus", "l_shipinstruct", "l_shipmode"],
    "orders": ["o_orderstatus", "o_orderpriority"],
    "part": ["p_brand", "p_type", "p_container", "p_mfgr"],
    "customer": ["c_mktsegment"],
    "nation": ["n_name"],
    "region": ["r_name"],
}

# Columns that should use ``timestamp`` instead of ``date``
TPCH_TIMESTAMP_COLUMNS: dict[str, list[str]] = {
    "lineitem": ["l_shipdate", "l_commitdate", "l_receiptdate"],
    "orders": ["o_orderdate"],
}

# Default partition granularity when not specified per table
DEFAULT_PARTITION_BY = "MONTH"


class QuestDBDDLGenerator(BaseDDLGenerator):
    """DDL generator for QuestDB physical tuning.

    QuestDB's DDL supports:
    - ``timestamp(column)`` for designating the time-series ordering column
    - ``PARTITION BY {DAY|MONTH|YEAR}`` for automatic time-based partitioning
    - ``SYMBOL`` type for indexed, interned low-cardinality strings
    - ``TIMESTAMP`` type for date/time columns

    Tuning Configuration Mapping:
    - partitioning -> designated timestamp + PARTITION BY
    - sorting -> not applicable (QuestDB sorts by designated timestamp)
    - distribution -> not applicable (single-node)
    - clustering -> not applicable
    """

    IDENTIFIER_QUOTE = '"'
    SUPPORTS_IF_NOT_EXISTS = True
    STATEMENT_TERMINATOR = ";"

    SUPPORTED_TUNING_TYPES = frozenset({"partitioning"})

    def __init__(
        self,
        default_partition_by: str = DEFAULT_PARTITION_BY,
    ):
        """Initialize the QuestDB DDL generator.

        Args:
            default_partition_by: Default partition granularity for tables with
                a designated timestamp. One of DAY, MONTH, YEAR.
        """
        self._default_partition_by = default_partition_by

    @property
    def platform_name(self) -> str:
        return "questdb"

    def generate_tuning_clauses(
        self,
        table_tuning: TableTuning | None,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> TuningClauses:
        """Generate QuestDB tuning clauses.

        For QuestDB, tuning clauses include:
        - ``table_properties`` containing timestamp() and PARTITION BY suffix
        - Column type overrides stored in ``extra`` for symbol/timestamp mapping

        The actual type remapping is handled by the QuestDBAdapter's
        ``_apply_questdb_schema_enhancements`` during schema creation.

        Args:
            table_tuning: Table tuning configuration.
            platform_opts: Platform-specific options.

        Returns:
            TuningClauses with QuestDB-specific configuration.
        """
        clauses = TuningClauses()

        if not table_tuning:
            return clauses

        table_name = table_tuning.table_name.lower() if table_tuning.table_name else ""

        from benchbox.core.tuning.interface import TuningType

        # Handle partitioning -> designated timestamp + PARTITION BY
        partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
        if partition_columns:
            sorted_cols = sorted(partition_columns, key=lambda c: c.order)
            ts_column = sorted_cols[0].name

            partition_by = self._default_partition_by
            if platform_opts:
                partition_by = getattr(platform_opts, "partition_by", partition_by)

            # Store as table properties suffix
            suffix = f"timestamp({ts_column})"
            if partition_by and partition_by.upper() != "NONE":
                suffix += f" PARTITION BY {partition_by.upper()}"

            clauses.table_properties = suffix
            logger.info(f"QuestDB table {table_name}: designated timestamp={ts_column}, partition={partition_by}")
        else:
            # Check TPC-H defaults
            ts_col = TPCH_DESIGNATED_TIMESTAMP.get(table_name)
            if ts_col:
                partition_by = TPCH_PARTITION_BY.get(table_name, self._default_partition_by)
                suffix = f"timestamp({ts_col})"
                if partition_by and partition_by.upper() != "NONE":
                    suffix += f" PARTITION BY {partition_by.upper()}"
                clauses.table_properties = suffix
                logger.info(
                    f"QuestDB table {table_name}: using TPC-H default timestamp={ts_col}, partition={partition_by}"
                )

        # Warn about unsupported tuning types
        sort_columns = table_tuning.get_columns_by_type(TuningType.SORTING)
        if sort_columns:
            logger.info(
                f"Sorting tuning for QuestDB table {table_name}: "
                f"{[c.name for c in sort_columns]}. "
                f"QuestDB sorts by designated timestamp automatically."
            )

        distribution_columns = table_tuning.get_columns_by_type(TuningType.DISTRIBUTION)
        if distribution_columns:
            logger.warning(
                f"Distribution tuning not applicable for single-node QuestDB "
                f"(table: {table_name}). "
                f"Configured columns {[c.name for c in distribution_columns]} will be ignored."
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
        """Generate QuestDB CREATE TABLE statement.

        Produces DDL like:
            CREATE TABLE lineitem (
                l_orderkey LONG,
                l_shipdate TIMESTAMP,
                l_returnflag SYMBOL,
                ...
            ) timestamp(l_shipdate) PARTITION BY MONTH;
        """
        parts = ["CREATE TABLE"]

        if if_not_exists:
            parts.append("IF NOT EXISTS")

        parts.append(self.format_qualified_name(table_name, schema))

        statement = " ".join(parts)

        # Apply QuestDB type mappings to columns
        mapped_columns = self._apply_type_mappings(table_name, columns)
        col_list = self.generate_column_list(mapped_columns)
        statement = f"{statement} (\n    {col_list}\n)"

        # Append timestamp() and PARTITION BY from tuning
        if tuning and tuning.table_properties:
            statement = f"{statement} {tuning.table_properties}"

        statement = f"{statement}{self.STATEMENT_TERMINATOR}"

        return statement

    def _apply_type_mappings(
        self,
        table_name: str,
        columns: list[ColumnDefinition],
    ) -> list[ColumnDefinition]:
        """Apply QuestDB-specific type mappings to column definitions.

        Maps:
        - Low-cardinality VARCHAR/TEXT columns -> SYMBOL
        - DATE columns -> TIMESTAMP (for designated timestamp compatibility)

        Args:
            table_name: Table name for looking up TPC-H defaults.
            columns: Original column definitions.

        Returns:
            New list of ColumnDefinition with mapped types.
        """
        table_lower = table_name.lower()
        symbol_cols = set(TPCH_SYMBOL_COLUMNS.get(table_lower, []))
        ts_cols = set(TPCH_TIMESTAMP_COLUMNS.get(table_lower, []))

        mapped = []
        for col in columns:
            col_lower = col.name.lower()
            new_type = col.data_type

            if col_lower in symbol_cols:
                new_type = "SYMBOL"
            elif col_lower in ts_cols:
                new_type = "TIMESTAMP"

            if new_type != col.data_type:
                mapped.append(
                    ColumnDefinition(
                        name=col.name,
                        data_type=new_type,
                        nullable=col.nullable,
                        default_value=col.default_value,
                        primary_key=col.primary_key,
                        comment=col.comment,
                    )
                )
            else:
                mapped.append(col)

        return mapped


__all__ = [
    "QuestDBDDLGenerator",
]
