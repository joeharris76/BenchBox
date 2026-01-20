"""Primitives benchmark schema definitions.

This module defines the TPC-H schema for the primitives benchmark, which tests
fundamental database operations on standard TPC-H tables.

The TPC-H schema consists of:
- 8 base tables (CUSTOMER, LINEITEM, NATION, ORDERS, PART, PARTSUPP, REGION, SUPPLIER)

For more information, see:
- TPC-H Specification: https://www.tpc.org/tpch/

Copyright 2026 Joe Harris / BenchBox Project

This implementation is derived from TPC Benchmark™ H (TPC-H) - Copyright © Transaction Processing Performance Council

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import cast

# REGION table
REGION = {
    "name": "region",
    "columns": [
        {"name": "r_regionkey", "type": "INTEGER", "primary_key": True},
        {"name": "r_name", "type": "VARCHAR(25)"},
        {"name": "r_comment", "type": "VARCHAR(152)"},
    ],
}

# NATION table
NATION = {
    "name": "nation",
    "columns": [
        {"name": "n_nationkey", "type": "INTEGER", "primary_key": True},
        {"name": "n_name", "type": "VARCHAR(25)"},
        {"name": "n_regionkey", "type": "INTEGER", "foreign_key": "region.r_regionkey"},
        {"name": "n_comment", "type": "VARCHAR(152)"},
    ],
}

# CUSTOMER table
CUSTOMER = {
    "name": "customer",
    "columns": [
        {"name": "c_custkey", "type": "INTEGER", "primary_key": True},
        {"name": "c_name", "type": "VARCHAR(25)"},
        {"name": "c_address", "type": "VARCHAR(40)"},
        {"name": "c_nationkey", "type": "INTEGER", "foreign_key": "nation.n_nationkey"},
        {"name": "c_phone", "type": "VARCHAR(15)"},
        {"name": "c_acctbal", "type": "DECIMAL(15,2)"},
        {"name": "c_mktsegment", "type": "VARCHAR(10)"},
        {"name": "c_comment", "type": "VARCHAR(117)"},
    ],
}

# SUPPLIER table
SUPPLIER = {
    "name": "supplier",
    "columns": [
        {"name": "s_suppkey", "type": "INTEGER", "primary_key": True},
        {"name": "s_name", "type": "VARCHAR(25)"},
        {"name": "s_address", "type": "VARCHAR(40)"},
        {"name": "s_nationkey", "type": "INTEGER", "foreign_key": "nation.n_nationkey"},
        {"name": "s_phone", "type": "VARCHAR(15)"},
        {"name": "s_acctbal", "type": "DECIMAL(15,2)"},
        {"name": "s_comment", "type": "VARCHAR(101)"},
    ],
}

# PART table
PART = {
    "name": "part",
    "columns": [
        {"name": "p_partkey", "type": "INTEGER", "primary_key": True},
        {"name": "p_name", "type": "VARCHAR(55)"},
        {"name": "p_mfgr", "type": "VARCHAR(25)"},
        {"name": "p_brand", "type": "VARCHAR(10)"},
        {"name": "p_type", "type": "VARCHAR(25)"},
        {"name": "p_size", "type": "INTEGER"},
        {"name": "p_container", "type": "VARCHAR(10)"},
        {"name": "p_retailprice", "type": "DECIMAL(15,2)"},
        {"name": "p_comment", "type": "VARCHAR(23)"},
    ],
}

# PARTSUPP table
PARTSUPP = {
    "name": "partsupp",
    "columns": [
        {"name": "ps_partkey", "type": "INTEGER", "foreign_key": "part.p_partkey"},
        {"name": "ps_suppkey", "type": "INTEGER", "foreign_key": "supplier.s_suppkey"},
        {"name": "ps_availqty", "type": "INTEGER"},
        {"name": "ps_supplycost", "type": "DECIMAL(15,2)"},
        {"name": "ps_comment", "type": "VARCHAR(199)"},
    ],
    "primary_key": ["ps_partkey", "ps_suppkey"],
}

# ORDERS table
ORDERS = {
    "name": "orders",
    "columns": [
        {"name": "o_orderkey", "type": "INTEGER", "primary_key": True},
        {"name": "o_custkey", "type": "INTEGER", "foreign_key": "customer.c_custkey"},
        {"name": "o_orderstatus", "type": "VARCHAR(1)"},
        {"name": "o_totalprice", "type": "DECIMAL(15,2)"},
        {"name": "o_orderdate", "type": "DATE"},
        {"name": "o_orderpriority", "type": "VARCHAR(15)"},
        {"name": "o_clerk", "type": "VARCHAR(15)"},
        {"name": "o_shippriority", "type": "INTEGER"},
        {"name": "o_comment", "type": "VARCHAR(79)"},
    ],
}

# LINEITEM table
LINEITEM = {
    "name": "lineitem",
    "columns": [
        {"name": "l_orderkey", "type": "INTEGER", "foreign_key": "orders.o_orderkey"},
        {"name": "l_partkey", "type": "INTEGER", "foreign_key": "part.p_partkey"},
        {"name": "l_suppkey", "type": "INTEGER", "foreign_key": "supplier.s_suppkey"},
        {"name": "l_linenumber", "type": "INTEGER"},
        {"name": "l_quantity", "type": "DECIMAL(15,2)"},
        {"name": "l_extendedprice", "type": "DECIMAL(15,2)"},
        {"name": "l_discount", "type": "DECIMAL(15,2)"},
        {"name": "l_tax", "type": "DECIMAL(15,2)"},
        {"name": "l_returnflag", "type": "VARCHAR(1)"},
        {"name": "l_linestatus", "type": "VARCHAR(1)"},
        {"name": "l_shipdate", "type": "DATE"},
        {"name": "l_commitdate", "type": "DATE"},
        {"name": "l_receiptdate", "type": "DATE"},
        {"name": "l_shipinstruct", "type": "VARCHAR(25)"},
        {"name": "l_shipmode", "type": "VARCHAR(10)"},
        {"name": "l_comment", "type": "VARCHAR(44)"},
    ],
    "primary_key": ["l_orderkey", "l_linenumber"],
}

# All tables in the TPC-H schema
TABLES = {
    "region": REGION,
    "nation": NATION,
    "customer": CUSTOMER,
    "supplier": SUPPLIER,
    "part": PART,
    "partsupp": PARTSUPP,
    "orders": ORDERS,
    "lineitem": LINEITEM,
}


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

    for col in cast(list, table["columns"]):
        col_def = f"{cast(str, col['name'])} {cast(str, col['type'])}"
        # Only add PRIMARY KEY constraint if enabled
        if enable_primary_keys and col.get("primary_key"):
            col_def += " PRIMARY KEY"
        columns.append(col_def)

    # Handle composite primary keys (only if enabled)
    if enable_primary_keys and "primary_key" in table and isinstance(table["primary_key"], list):
        pk_cols = ", ".join(cast(list[str], table["primary_key"]))
        columns.append(f"PRIMARY KEY ({pk_cols})")

    # Handle foreign keys (only if enabled)
    if enable_foreign_keys and "foreign_keys" in table:
        for fk in cast(list, table["foreign_keys"]):
            cast(str, fk["name"])
            fk_column = cast(str, fk["column"])
            ref_table = cast(str, fk["references"]["table"])
            ref_column = cast(str, fk["references"]["column"])
            columns.append(f"FOREIGN KEY ({fk_column}) REFERENCES {ref_table}({ref_column})")

    sql = f"CREATE TABLE {table['name']} (\n"
    sql += ",\n".join(f"  {col}" for col in columns)
    sql += "\n);"

    return sql


def get_all_create_table_sql(
    dialect: str = "standard",
    enable_primary_keys: bool = True,
    enable_foreign_keys: bool = True,
) -> str:
    """Generate CREATE TABLE SQL for all TPC-H tables.

    Args:
        dialect: SQL dialect to use
        enable_primary_keys: Whether to include primary key constraints
        enable_foreign_keys: Whether to include foreign key constraints

    Returns:
        Complete SQL schema creation script
    """
    # Order tables by dependencies (referenced tables first)
    table_order = [
        "region",
        "nation",
        "customer",
        "supplier",
        "part",
        "partsupp",
        "orders",
        "lineitem",
    ]

    sql_statements = []
    for table_name in table_order:
        sql_statements.append(
            get_create_table_sql(
                table_name,
                dialect,
                enable_primary_keys=enable_primary_keys,
                enable_foreign_keys=enable_foreign_keys,
            )
        )

    return "\n\n".join(sql_statements)
