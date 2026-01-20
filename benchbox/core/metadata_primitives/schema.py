"""Schema definitions for Metadata Primitives benchmark.

This module provides schema generation for testing metadata introspection.
It creates the TPC-H schema as the baseline for metadata queries, providing
8 well-defined tables with realistic structure (fact/dimension relationships,
foreign keys, varied column types).

The complexity testing features (MetadataGenerator) can add additional stress
test structures like wide tables, nested views, and large catalogs on top of
this baseline.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

# Import TPC-H schema
from benchbox.core.tpch.schema import (
    TABLES as TPCH_TABLES,
    get_create_all_tables_sql as get_tpch_schema_sql,
)

if TYPE_CHECKING:
    from benchbox.core.tuning import UnifiedTuningConfiguration

logger = logging.getLogger(__name__)


def get_table_names() -> list[str]:
    """Get all table names in the metadata primitives schema.

    Returns:
        List of TPC-H table names (8 tables)
    """
    return [table.name for table in TPCH_TABLES]


def get_schema() -> dict[str, dict[str, Any]]:
    """Get schema metadata for all tables.

    Returns:
        Dictionary mapping table names to their column definitions,
        in the format expected by the benchmark framework.
    """
    schema = {}
    for table in TPCH_TABLES:
        schema[table.name] = {
            "name": table.name,
            "columns": [
                {
                    "name": col.name,
                    "type": col.get_sql_type(),
                    "nullable": col.nullable,
                    "primary_key": col.primary_key,
                }
                for col in table.columns
            ],
        }
    return schema


def get_create_tables_sql(
    dialect: str = "standard",
    tuning_config: UnifiedTuningConfiguration | None = None,
) -> str:
    """Generate CREATE TABLE SQL for metadata primitives schema.

    Creates the TPC-H schema (8 tables) to provide a baseline metadata
    environment for testing INFORMATION_SCHEMA queries.

    Args:
        dialect: Target SQL dialect (currently uses standard SQL)
        tuning_config: Optional tuning configuration for constraints

    Returns:
        SQL script to create all TPC-H tables
    """
    # Determine constraint settings from tuning config
    enable_primary_keys = True
    enable_foreign_keys = False  # FK disabled by default for faster setup

    if tuning_config is not None:
        if hasattr(tuning_config, "primary_keys"):
            enable_primary_keys = tuning_config.primary_keys.enabled
        if hasattr(tuning_config, "foreign_keys"):
            enable_foreign_keys = tuning_config.foreign_keys.enabled

    logger.debug(f"Generating metadata schema SQL: pk={enable_primary_keys}, fk={enable_foreign_keys}")

    sql_parts = []

    # Add header comment
    sql_parts.append("-- Metadata Primitives Benchmark Schema")
    sql_parts.append("-- Creates TPC-H tables for metadata introspection testing")
    sql_parts.append("-- Tables: region, nation, supplier, part, partsupp, customer, orders, lineitem")
    sql_parts.append("")

    tpch_sql = get_tpch_schema_sql(
        enable_primary_keys=enable_primary_keys,
        enable_foreign_keys=enable_foreign_keys,
    )
    sql_parts.append(tpch_sql)

    return "\n".join(sql_parts)


def get_table_count() -> int:
    """Get the number of tables in the schema."""
    return len(TPCH_TABLES)


def get_total_column_count() -> int:
    """Get total number of columns across all tables."""
    return sum(len(table.columns) for table in TPCH_TABLES)


__all__ = [
    "get_create_tables_sql",
    "get_schema",
    "get_table_names",
    "get_table_count",
    "get_total_column_count",
]
