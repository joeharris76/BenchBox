"""H2O DB benchmark schema definitions.

The H2O DB benchmark is designed to test analytical database performance
using taxi trip data. It consists of a single large table with trip records
and a set of analytical queries.

The benchmark is based on the NYC Taxi & Limousine Commission Trip Record Data
and tests various aspects of analytical query performance including:
- Aggregations
- Grouping
- Joins
- Window functions
- String operations

For more information, see:
- https://h2oai.github.io/db-benchmark/

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import cast

# Main trips table - based on NYC taxi data structure
TRIPS = {
    "name": "trips",
    "columns": [
        {"name": "vendor_id", "type": "INTEGER"},
        {"name": "pickup_datetime", "type": "TIMESTAMP"},
        {"name": "dropoff_datetime", "type": "TIMESTAMP"},
        {"name": "passenger_count", "type": "INTEGER"},
        {"name": "trip_distance", "type": "DECIMAL(8,2)"},
        {"name": "pickup_longitude", "type": "DECIMAL(18,14)"},
        {"name": "pickup_latitude", "type": "DECIMAL(18,14)"},
        {"name": "rate_code_id", "type": "INTEGER"},
        {"name": "store_and_fwd_flag", "type": "VARCHAR(1)"},
        {"name": "dropoff_longitude", "type": "DECIMAL(18,14)"},
        {"name": "dropoff_latitude", "type": "DECIMAL(18,14)"},
        {"name": "payment_type", "type": "INTEGER"},
        {"name": "fare_amount", "type": "DECIMAL(8,2)"},
        {"name": "extra", "type": "DECIMAL(8,2)"},
        {"name": "mta_tax", "type": "DECIMAL(8,2)"},
        {"name": "tip_amount", "type": "DECIMAL(8,2)"},
        {"name": "tolls_amount", "type": "DECIMAL(8,2)"},
        {"name": "improvement_surcharge", "type": "DECIMAL(8,2)"},
        {"name": "total_amount", "type": "DECIMAL(8,2)"},
        {"name": "pickup_location_id", "type": "INTEGER"},
        {"name": "dropoff_location_id", "type": "INTEGER"},
        {"name": "congestion_surcharge", "type": "DECIMAL(8,2)"},
    ],
}

# All tables in the H2O DB schema (just one table)
TABLES = {"trips": TRIPS}


def get_create_table_sql(
    table_name: str,
    dialect: str = "standard",
    enable_primary_keys: bool = True,
    enable_foreign_keys: bool = True,
) -> str:
    """Generate CREATE TABLE SQL for a given table.

    Args:
        table_name: Name of the table to create
        dialect: SQL dialect to use (standard, postgres, mysql, etc.)
        enable_primary_keys: Whether to include primary key constraints
        enable_foreign_keys: Whether to include foreign key constraints

    Returns:
        CREATE TABLE SQL statement

    Raises:
        ValueError: If table_name is not valid
    """
    if table_name not in TABLES:
        raise ValueError(f"Unknown table: {table_name}")

    table = TABLES[table_name]
    columns = []

    for col in table["columns"]:
        col_def = f"{cast(str, col['name'])} {cast(str, col['type'])}"
        if col.get("primary_key") and enable_primary_keys:
            col_def += " PRIMARY KEY"
        columns.append(col_def)

    sql = f"CREATE TABLE {table['name']} (\n"
    sql += ",\n".join(f"  {col}" for col in columns)
    sql += "\n);"

    return sql


def get_all_create_table_sql(
    dialect: str = "standard",
    enable_primary_keys: bool = True,
    enable_foreign_keys: bool = True,
) -> str:
    """Generate CREATE TABLE SQL for all H2O DB tables.

    Args:
        dialect: SQL dialect to use
        enable_primary_keys: Whether to include primary key constraints
        enable_foreign_keys: Whether to include foreign key constraints

    Returns:
        Complete SQL schema creation script
    """
    sql_statements = []
    for table_name in TABLES:
        sql_statements.append(get_create_table_sql(table_name, dialect, enable_primary_keys, enable_foreign_keys))

    return "\n\n".join(sql_statements)
