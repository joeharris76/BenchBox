"""Trino/Presto/Athena DDL Generators.

Generates CREATE TABLE statements for the Trino/Presto family with support
for different connectors (Hive, Iceberg, Delta Lake).

The Trino family uses WITH clause properties for tuning:
- partitioned_by: Column-based partitioning
- bucketed_by / bucket_count: Hash bucketing
- sorted_by: Sort within buckets
- format: File format (PARQUET, ORC, etc.)
- location: External table location

Example:
    >>> from benchbox.core.tuning.generators.trino import TrinoDDLGenerator
    >>> generator = TrinoDDLGenerator(connector="hive")
    >>> clauses = generator.generate_tuning_clauses(table_tuning)
    >>> print(generator.generate_create_table_ddl("lineitem", columns, clauses))
    CREATE TABLE lineitem (
        l_orderkey BIGINT,
        l_shipdate DATE
    )
    WITH (
        format = 'PARQUET',
        partitioned_by = ARRAY['l_shipdate']
    );

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


class ConnectorType(str, Enum):
    """Trino connector types with different DDL syntax."""

    HIVE = "hive"  # Hive Metastore connector
    ICEBERG = "iceberg"  # Apache Iceberg tables
    DELTA = "delta"  # Delta Lake tables
    MEMORY = "memory"  # In-memory tables (limited tuning)


class FileFormat(str, Enum):
    """Supported file formats for Trino tables."""

    PARQUET = "PARQUET"
    ORC = "ORC"
    AVRO = "AVRO"
    JSON = "JSON"
    CSV = "CSV"


class TrinoDDLGenerator(BaseDDLGenerator):
    """DDL generator for Trino/Presto/Athena.

    Supports multiple connectors with different tuning capabilities:

    Hive Connector:
    - partitioned_by: Column-based partitioning
    - bucketed_by + bucket_count: Hash bucketing
    - sorted_by: Sort within buckets

    Iceberg Connector:
    - partitioning: Partition transforms (year, month, day, hour, bucket, truncate)
    - sorted_by: Sorted table writes

    Delta Lake Connector:
    - partitioned_by: Column-based partitioning
    - location: External table location

    Tuning Configuration Mapping:
    - partitioning → partitioned_by or partitioning (connector-dependent)
    - distribution → bucketed_by + bucket_count (Hive)
    - sorting → sorted_by
    - clustering → Treated same as sorting for Trino
    """

    IDENTIFIER_QUOTE = '"'
    SUPPORTS_IF_NOT_EXISTS = True
    STATEMENT_TERMINATOR = ";"

    SUPPORTED_TUNING_TYPES = frozenset({"partitioning", "distribution", "sorting", "clustering"})

    def __init__(
        self,
        connector: ConnectorType | str = ConnectorType.HIVE,
        default_format: FileFormat = FileFormat.PARQUET,
        default_bucket_count: int = 32,
        external: bool = False,
        location: str | None = None,
    ):
        """Initialize the Trino DDL generator.

        Args:
            connector: Connector type (hive, iceberg, delta, memory).
            default_format: Default file format for tables.
            default_bucket_count: Default bucket count for hash bucketing.
            external: Whether to create EXTERNAL TABLE (Athena style).
            location: Default external table location.
        """
        self._connector = ConnectorType(connector) if isinstance(connector, str) else connector
        self._default_format = default_format
        self._default_bucket_count = default_bucket_count
        self._external = external
        self._location = location

    @property
    def platform_name(self) -> str:
        return "trino"

    @property
    def connector(self) -> ConnectorType:
        """Get the connector type."""
        return self._connector

    def generate_tuning_clauses(
        self,
        table_tuning: TableTuning | None,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> TuningClauses:
        """Generate Trino tuning clauses as WITH properties.

        Args:
            table_tuning: Table tuning configuration.
            platform_opts: Platform-specific options.

        Returns:
            TuningClauses with table_properties for WITH clause.
        """
        clauses = TuningClauses()

        if not table_tuning:
            # Still add format if specified
            clauses.table_properties["format"] = f"'{self._default_format.value}'"
            return clauses

        from benchbox.core.tuning.interface import TuningType

        # Set file format
        clauses.table_properties["format"] = f"'{self._default_format.value}'"

        # Handle partitioning
        partition_columns = table_tuning.get_columns_by_type(TuningType.PARTITIONING)
        if partition_columns:
            sorted_cols = sorted(partition_columns, key=lambda c: c.order)

            if self._connector == ConnectorType.ICEBERG:
                # Iceberg uses partition transforms
                transforms = self._generate_iceberg_partitioning(sorted_cols, platform_opts)
                clauses.table_properties["partitioning"] = transforms
            else:
                # Hive/Delta use simple column array
                col_names = [f"'{c.name}'" for c in sorted_cols]
                clauses.table_properties["partitioned_by"] = f"ARRAY[{', '.join(col_names)}]"

        # Handle distribution (bucketing for Hive connector)
        distribution_columns = table_tuning.get_columns_by_type(TuningType.DISTRIBUTION)
        if distribution_columns:
            if self._connector == ConnectorType.HIVE:
                sorted_cols = sorted(distribution_columns, key=lambda c: c.order)
                col_names = [f"'{c.name}'" for c in sorted_cols]
                clauses.table_properties["bucketed_by"] = f"ARRAY[{', '.join(col_names)}]"

                # Get bucket count from platform opts or use default
                bucket_count = self._default_bucket_count
                if platform_opts and hasattr(platform_opts, "bucket_count"):
                    bucket_count = platform_opts.bucket_count
                clauses.table_properties["bucket_count"] = str(bucket_count)
            elif self._connector == ConnectorType.ICEBERG:
                # Iceberg uses bucket transform in partitioning
                logger.info(
                    f"Distribution columns for Iceberg table {table_tuning.table_name}: "
                    f"{[c.name for c in distribution_columns]}. "
                    f"Use bucket() transform in partitioning instead."
                )
            else:
                logger.warning(
                    f"Distribution not supported for {self._connector.value} connector. "
                    f"Columns {[c.name for c in distribution_columns]} will be ignored."
                )

        # Handle sorting
        sort_columns = table_tuning.get_columns_by_type(TuningType.SORTING)
        cluster_columns = table_tuning.get_columns_by_type(TuningType.CLUSTERING)

        all_sort_cols = list(sort_columns) + [c for c in cluster_columns if c not in sort_columns]
        if all_sort_cols:
            if self._connector in (ConnectorType.HIVE, ConnectorType.ICEBERG):
                sorted_cols = sorted(all_sort_cols, key=lambda c: c.order)
                col_names = [f"'{c.name}'" for c in sorted_cols]
                clauses.table_properties["sorted_by"] = f"ARRAY[{', '.join(col_names)}]"
            else:
                logger.info(
                    f"Sorting hints for {self._connector.value} table {table_tuning.table_name}: "
                    f"{[c.name for c in all_sort_cols]}. May not be directly supported."
                )

        # Add location if specified
        if self._location:
            clauses.table_properties["location"] = f"'{self._location}'"

        return clauses

    def _generate_iceberg_partitioning(
        self,
        partition_columns,
        platform_opts: PlatformOptimizationConfiguration | None = None,
    ) -> str:
        """Generate Iceberg partition transforms.

        Iceberg supports transforms like:
        - year(col), month(col), day(col), hour(col)
        - bucket(col, N)
        - truncate(col, N)
        - identity(col)

        Args:
            partition_columns: List of partition columns.
            platform_opts: May contain transform specifications.

        Returns:
            ARRAY syntax with partition transforms.
        """
        transforms = []

        for col in partition_columns:
            col_name = col.name
            col_type = col.type.upper() if col.type else ""

            # Check for explicit transform in platform_opts
            transform = None
            if platform_opts:
                col_transforms = getattr(platform_opts, "partition_transforms", {})
                if isinstance(col_transforms, dict):
                    transform = col_transforms.get(col_name)

            if transform:
                transforms.append(f"'{transform}'")
            elif "DATE" in col_type or "TIMESTAMP" in col_type:
                # Default to month for date/time columns
                transforms.append(f"'month({col_name})'")
            else:
                # Default to identity transform
                transforms.append(f"'{col_name}'")

        return f"ARRAY[{', '.join(transforms)}]"

    def format_qualified_name(self, table_name: str, schema: str | None = None) -> str:
        """Format a fully qualified table name.

        Trino uses catalog.schema.table format without quoting the catalog/schema parts
        when they are valid identifiers.

        Args:
            table_name: Table name.
            schema: Optional catalog.schema prefix (e.g., "hive.tpch").

        Returns:
            Qualified name like "catalog.schema.table" or just "table".
        """
        if schema:
            # For Trino, schema can be catalog.schema - don't quote the whole thing
            return f"{schema}.{self.quote_identifier(table_name)}"
        return self.quote_identifier(table_name)

    def _format_with_clause(self, properties: dict[str, str]) -> str:
        """Format Trino WITH clause for table properties.

        Args:
            properties: Dictionary of property names to values.

        Returns:
            WITH clause string.
        """
        if not properties:
            return ""

        # Trino uses = without quotes around property names
        props = ", ".join(f"{k} = {v}" for k, v in sorted(properties.items()))
        return f"WITH (\n    {props}\n)"

    def generate_create_table_ddl(
        self,
        table_name: str,
        columns: list[ColumnDefinition],
        tuning: TuningClauses | None = None,
        if_not_exists: bool = False,
        schema: str | None = None,
    ) -> str:
        """Generate Trino CREATE TABLE statement.

        Trino DDL uses WITH clause for table properties:
        CREATE TABLE t (...) WITH (format = 'PARQUET', partitioned_by = ...);
        """
        parts = ["CREATE"]

        if self._external:
            parts.append("EXTERNAL")

        parts.append("TABLE")

        if if_not_exists:
            parts.append("IF NOT EXISTS")

        parts.append(self.format_qualified_name(table_name, schema))

        statement = " ".join(parts)

        # Column definitions
        col_list = self.generate_column_list(columns)
        statement = f"{statement} (\n    {col_list}\n)"

        # Add WITH clause for properties (Trino-specific, not TBLPROPERTIES)
        if tuning and tuning.table_properties:
            with_clause = self._format_with_clause(tuning.table_properties)
            if with_clause:
                statement = f"{statement}\n{with_clause}"

        statement = f"{statement}{self.STATEMENT_TERMINATOR}"

        return statement


class AthenaDDLGenerator(TrinoDDLGenerator):
    """DDL generator for AWS Athena.

    Athena is a managed Trino/Presto service with some differences:
    - Uses EXTERNAL TABLE by default
    - Requires LOCATION for external tables
    - Uses STORED AS instead of format property
    - Uses TBLPROPERTIES for additional settings
    """

    def __init__(
        self,
        location: str | None = None,
        default_format: FileFormat = FileFormat.PARQUET,
        default_bucket_count: int = 32,
    ):
        """Initialize the Athena DDL generator.

        Args:
            location: S3 location for external table (required for Athena).
            default_format: Default file format.
            default_bucket_count: Default bucket count for bucketing.
        """
        super().__init__(
            connector=ConnectorType.HIVE,
            default_format=default_format,
            default_bucket_count=default_bucket_count,
            external=True,
            location=location,
        )

    @property
    def platform_name(self) -> str:
        return "athena"

    def generate_create_table_ddl(
        self,
        table_name: str,
        columns: list[ColumnDefinition],
        tuning: TuningClauses | None = None,
        if_not_exists: bool = False,
        schema: str | None = None,
    ) -> str:
        """Generate Athena CREATE EXTERNAL TABLE statement.

        Athena uses slightly different DDL syntax:
        CREATE EXTERNAL TABLE t (...)
        PARTITIONED BY (col TYPE)
        STORED AS PARQUET
        LOCATION 's3://...'
        TBLPROPERTIES (...);
        """
        parts = ["CREATE EXTERNAL TABLE"]

        if if_not_exists:
            parts.append("IF NOT EXISTS")

        parts.append(self.format_qualified_name(table_name, schema))

        statement = " ".join(parts)

        # Column definitions
        col_list = self.generate_column_list(columns)
        statement = f"{statement} (\n    {col_list}\n)"

        # Add PARTITIONED BY clause (Athena style with column types)
        if tuning and "partitioned_by" in tuning.table_properties:
            # For Athena, we need the original partition column info
            # This is a simplified version - in full implementation,
            # we'd need access to column definitions
            statement = f"{statement}\n-- Note: PARTITIONED BY clause should be added with column types"

        # Add STORED AS clause
        file_format = self._default_format.value
        if tuning and "format" in tuning.table_properties:
            # Extract format from the quoted value
            format_val = tuning.table_properties["format"].strip("'")
            file_format = format_val

        statement = f"{statement}\nSTORED AS {file_format}"

        # Add LOCATION clause
        location = self._location
        if tuning and "location" in tuning.table_properties:
            location = tuning.table_properties["location"].strip("'")

        if location:
            statement = f"{statement}\nLOCATION '{location}'"

        # Add TBLPROPERTIES for remaining properties
        remaining_props = {
            k: v
            for k, v in (tuning.table_properties if tuning else {}).items()
            if k not in ("format", "location", "partitioned_by", "bucketed_by", "bucket_count", "sorted_by")
        }
        if remaining_props:
            props_str = ", ".join(f"'{k}' = {v}" for k, v in remaining_props.items())
            statement = f"{statement}\nTBLPROPERTIES ({props_str})"

        statement = f"{statement}{self.STATEMENT_TERMINATOR}"

        return statement


__all__ = [
    "AthenaDDLGenerator",
    "ConnectorType",
    "FileFormat",
    "TrinoDDLGenerator",
]
