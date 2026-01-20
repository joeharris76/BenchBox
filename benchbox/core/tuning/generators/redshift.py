"""Redshift DDL Generator.

Generates CREATE TABLE statements with Amazon Redshift-specific physical tuning:
- Distribution styles (ALL, KEY, EVEN, AUTO)
- Distribution keys (DISTKEY)
- Sort keys (COMPOUND, INTERLEAVED)
- Column encoding (ENCODE)

Example:
    >>> from benchbox.core.tuning.generators.redshift import RedshiftDDLGenerator
    >>> generator = RedshiftDDLGenerator()
    >>> clauses = generator.generate_tuning_clauses(table_tuning)
    >>> print(generator.generate_create_table_ddl("lineitem", columns, clauses))
    CREATE TABLE lineitem (
        l_orderkey BIGINT NOT NULL,
        l_shipdate DATE,
        ...
    )
    DISTSTYLE KEY
    DISTKEY (l_orderkey)
    COMPOUND SORTKEY (l_shipdate, l_orderkey);

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


class DistStyle(str, Enum):
    """Redshift distribution styles."""

    ALL = "ALL"  # Copy entire table to every node (small dimension tables)
    KEY = "KEY"  # Hash distribute by DISTKEY column (fact tables)
    EVEN = "EVEN"  # Round-robin distribution (default)
    AUTO = "AUTO"  # Let Redshift choose based on data size


class SortStyle(str, Enum):
    """Redshift sort key styles."""

    COMPOUND = "COMPOUND"  # Columns sorted in specified order (default)
    INTERLEAVED = "INTERLEAVED"  # Equal weight to each column


class ColumnEncoding(str, Enum):
    """Redshift column compression encodings."""

    RAW = "raw"  # No compression
    AZ64 = "az64"  # Amazon's encoding for numeric/date (best compression)
    BYTEDICT = "bytedict"  # Dictionary encoding for low cardinality
    DELTA = "delta"  # Delta encoding for sorted data
    DELTA32K = "delta32k"  # Delta with larger range
    LZO = "lzo"  # LZO compression for VARCHAR
    MOSTLY8 = "mostly8"  # For mostly 8-bit values
    MOSTLY16 = "mostly16"  # For mostly 16-bit values
    MOSTLY32 = "mostly32"  # For mostly 32-bit values
    RUNLENGTH = "runlength"  # Run-length encoding
    TEXT255 = "text255"  # Dictionary for words < 255 chars
    TEXT32K = "text32k"  # Dictionary for words < 32K chars
    ZSTD = "zstd"  # Zstandard compression
    AUTO = "AUTO"  # Let Redshift choose


def recommend_encoding(data_type: str) -> ColumnEncoding:
    """Recommend an encoding based on data type.

    Args:
        data_type: SQL data type (e.g., "BIGINT", "VARCHAR(100)").

    Returns:
        Recommended encoding for the data type.
    """
    dt_upper = data_type.upper()

    # Numeric types
    if any(t in dt_upper for t in ["INT", "BIGINT", "SMALLINT", "DECIMAL", "NUMERIC", "DOUBLE", "FLOAT", "REAL"]):
        return ColumnEncoding.AZ64

    # Date/time types
    if any(t in dt_upper for t in ["DATE", "TIME", "TIMESTAMP"]):
        return ColumnEncoding.AZ64

    # Boolean
    if "BOOL" in dt_upper:
        return ColumnEncoding.RUNLENGTH

    # Character types
    if any(t in dt_upper for t in ["VARCHAR", "CHAR", "TEXT"]):
        return ColumnEncoding.LZO

    # Default to AUTO
    return ColumnEncoding.AUTO


class RedshiftDDLGenerator(BaseDDLGenerator):
    """DDL generator for Amazon Redshift physical tuning.

    Supports:
    - DISTSTYLE (ALL, KEY, EVEN, AUTO)
    - DISTKEY (distribution column)
    - SORTKEY with style (COMPOUND, INTERLEAVED)
    - Column ENCODE clauses

    Tuning Configuration Mapping:
    - distribution → DISTSTYLE + DISTKEY
    - sorting → SORTKEY
    - clustering → Treated same as sorting for Redshift
    - partitioning → Not directly supported (logged as info)
    """

    IDENTIFIER_QUOTE = '"'
    SUPPORTS_IF_NOT_EXISTS = True
    STATEMENT_TERMINATOR = ";"

    SUPPORTED_TUNING_TYPES = frozenset({"distribution", "sorting", "clustering"})

    def __init__(
        self,
        default_dist_style: DistStyle = DistStyle.AUTO,
        default_sort_style: SortStyle = SortStyle.COMPOUND,
        auto_encoding: bool = True,
    ):
        """Initialize the Redshift DDL generator.

        Args:
            default_dist_style: Default distribution style when none specified.
            default_sort_style: Default sort key style (COMPOUND or INTERLEAVED).
            auto_encoding: Whether to add ENCODE clauses automatically.
        """
        self._default_dist_style = default_dist_style
        self._default_sort_style = default_sort_style
        self._auto_encoding = auto_encoding

    @property
    def platform_name(self) -> str:
        return "redshift"

    def generate_tuning_clauses(
        self,
        table_tuning: TableTuning | None,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> TuningClauses:
        """Generate Redshift tuning clauses.

        Args:
            table_tuning: Table tuning configuration.
            platform_opts: Platform-specific options (may contain dist_style, sort_style).

        Returns:
            TuningClauses with distribute_by, sort_by for Redshift.
        """
        clauses = TuningClauses()

        if not table_tuning:
            return clauses

        from benchbox.core.tuning.interface import TuningType

        # Extract platform-specific options
        dist_style = self._default_dist_style
        sort_style = self._default_sort_style

        if platform_opts:
            if hasattr(platform_opts, "dist_style") and platform_opts.dist_style:
                try:
                    dist_style = DistStyle(platform_opts.dist_style.upper())
                except ValueError:
                    logger.warning(f"Invalid dist_style '{platform_opts.dist_style}', using default")

            if hasattr(platform_opts, "sort_style") and platform_opts.sort_style:
                try:
                    sort_style = SortStyle(platform_opts.sort_style.upper())
                except ValueError:
                    logger.warning(f"Invalid sort_style '{platform_opts.sort_style}', using default")

        # Handle partitioning warning (Redshift doesn't have native partitioning)
        partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
        if partition_columns:
            logger.info(
                f"Partitioning hint for Redshift table {table_tuning.table_name}: "
                f"{[c.name for c in partition_columns]}. "
                f"Redshift doesn't support native partitioning. Consider using sort key "
                f"and date predicates for similar query performance."
            )

        # Handle distribution (DISTSTYLE + DISTKEY)
        distribution_columns = table_tuning.get_columns_by_type(TuningType.DISTRIBUTION)
        if distribution_columns:
            sorted_cols = sorted(distribution_columns, key=lambda c: c.order)
            if len(sorted_cols) == 1:
                # Single distribution column → DISTSTYLE KEY
                clauses.distribute_by = f"DISTSTYLE KEY\nDISTKEY ({sorted_cols[0].name})"
            else:
                # Multiple columns - use the first one and warn
                logger.warning(
                    f"Redshift only supports single-column DISTKEY. "
                    f"Using first column '{sorted_cols[0].name}' from {[c.name for c in sorted_cols]}."
                )
                clauses.distribute_by = f"DISTSTYLE KEY\nDISTKEY ({sorted_cols[0].name})"
        elif dist_style != DistStyle.AUTO:
            # No distribution column but explicit style requested
            clauses.distribute_by = f"DISTSTYLE {dist_style.value}"

        # Handle sorting (SORTKEY)
        # Combine sorting and clustering columns (Redshift uses same concept)
        sort_columns = table_tuning.get_columns_by_type(TuningType.SORTING)
        cluster_columns = table_tuning.get_columns_by_type(TuningType.CLUSTERING)

        all_sort_cols = list(sort_columns) + [c for c in cluster_columns if c not in sort_columns]
        if all_sort_cols:
            sorted_cols = sorted(all_sort_cols, key=lambda c: c.order)
            col_names = [c.name for c in sorted_cols]

            if sort_style == SortStyle.INTERLEAVED:
                clauses.sort_by = f"INTERLEAVED SORTKEY ({', '.join(col_names)})"
            else:
                clauses.sort_by = f"COMPOUND SORTKEY ({', '.join(col_names)})"

        return clauses

    def generate_column_definition(
        self,
        column: ColumnDefinition,
        include_encoding: bool | None = None,
    ) -> str:
        """Generate a single column definition with optional ENCODE clause.

        Args:
            column: Column definition.
            include_encoding: Whether to add ENCODE clause. None uses self._auto_encoding.

        Returns:
            Column DDL string like "col_name BIGINT NOT NULL ENCODE az64".
        """
        parts = [column.name, column.data_type]

        # Nullability
        if column.nullable == ColumnNullability.NOT_NULL:
            parts.append("NOT NULL")
        elif column.nullable == ColumnNullability.NULLABLE:
            parts.append("NULL")

        # Default value
        if column.default_value is not None:
            parts.append(f"DEFAULT {column.default_value}")

        # Encoding
        should_encode = include_encoding if include_encoding is not None else self._auto_encoding
        if should_encode:
            encoding = recommend_encoding(column.data_type)
            if encoding != ColumnEncoding.AUTO:
                parts.append(f"ENCODE {encoding.value}")

        return " ".join(parts)

    def generate_column_list(
        self,
        columns: list[ColumnDefinition],
        include_encoding: bool | None = None,
    ) -> str:
        """Generate column list with optional ENCODE clauses.

        Args:
            columns: List of column definitions.
            include_encoding: Whether to include ENCODE clauses.

        Returns:
            Comma-separated column definitions.
        """
        col_defs = [self.generate_column_definition(col, include_encoding) for col in columns]
        return ",\n    ".join(col_defs)

    def generate_create_table_ddl(
        self,
        table_name: str,
        columns: list[ColumnDefinition],
        tuning: TuningClauses | None = None,
        if_not_exists: bool = False,
        schema: str | None = None,
    ) -> str:
        """Generate Redshift CREATE TABLE statement.

        Redshift DDL places tuning clauses after the column list:
        CREATE TABLE t (...) DISTSTYLE KEY DISTKEY(col) SORTKEY(col1, col2);
        """
        parts = ["CREATE TABLE"]

        if if_not_exists:
            parts.append("IF NOT EXISTS")

        parts.append(self.format_qualified_name(table_name, schema))

        statement = " ".join(parts)

        # Column definitions with optional ENCODE
        col_list = self.generate_column_list(columns)
        statement = f"{statement} (\n    {col_list}\n)"

        # Add tuning clauses
        tuning_parts = []
        if tuning:
            if tuning.distribute_by:
                tuning_parts.append(tuning.distribute_by)
            if tuning.sort_by:
                tuning_parts.append(tuning.sort_by)

        if tuning_parts:
            statement = f"{statement}\n{chr(10).join(tuning_parts)}"

        statement = f"{statement}{self.STATEMENT_TERMINATOR}"

        return statement

    def generate_copy_command(
        self,
        table_name: str,
        s3_path: str,
        iam_role: str,
        file_format: str = "PARQUET",
        schema: str | None = None,
    ) -> str:
        """Generate a COPY command for loading data from S3.

        Args:
            table_name: Target table name.
            s3_path: S3 path (e.g., 's3://bucket/prefix/').
            iam_role: IAM role ARN for S3 access.
            file_format: File format (PARQUET, CSV, JSON, etc.).
            schema: Schema name for qualified table name.

        Returns:
            COPY command SQL string.
        """
        qualified_name = self.format_qualified_name(table_name, schema)

        parts = [
            f"COPY {qualified_name}",
            f"FROM '{s3_path}'",
            f"IAM_ROLE '{iam_role}'",
            f"FORMAT AS {file_format}",
        ]

        return "\n".join(parts) + self.STATEMENT_TERMINATOR


__all__ = [
    "ColumnEncoding",
    "DistStyle",
    "RedshiftDDLGenerator",
    "SortStyle",
    "recommend_encoding",
]
