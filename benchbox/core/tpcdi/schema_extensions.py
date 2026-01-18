"""TPC-DI schema extensions - additional dimension and fact tables for complete TPC-DI implementation.

This module defines the additional tables required for full TPC-DI compliance based on the
official specification and reference implementations. These tables extend the basic schema
to achieve publication-ready coverage.

Additional tables include:
- DimBroker: Broker hierarchy and classification
- FactCashBalances: Account balance changes over time
- FactHoldings: Security positions and valuations
- FactMarketHistory: Daily security price and volume data
- FactWatches: Customer watch list activity
- Financial reference and lookup tables

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DI (TPC-DI) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-DI specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import Any

# DimBroker - Broker dimension with hierarchy and regional classification
DIMBROKER = {
    "name": "DimBroker",
    "columns": [
        {"name": "SK_BrokerID", "type": "BIGINT", "primary_key": True},
        {"name": "BrokerID", "type": "BIGINT"},
        {"name": "ManagerID", "type": "BIGINT"},  # Hierarchy
        {"name": "FirstName", "type": "VARCHAR(50)"},
        {"name": "LastName", "type": "VARCHAR(50)"},
        {"name": "MiddleInitial", "type": "VARCHAR(1)"},
        {"name": "Branch", "type": "VARCHAR(50)"},
        {"name": "Office", "type": "VARCHAR(50)"},
        {"name": "Phone", "type": "VARCHAR(30)"},
        {"name": "IsCurrent", "type": "BOOLEAN"},
        {"name": "BatchID", "type": "BIGINT"},
        {"name": "EffectiveDate", "type": "DATE"},
        {"name": "EndDate", "type": "DATE"},
    ],
}

# FactCashBalances - Account balance changes over time
FACTCASHBALANCES = {
    "name": "FactCashBalances",
    "columns": [
        {"name": "SK_CustomerID", "type": "BIGINT"},
        {"name": "SK_AccountID", "type": "BIGINT"},
        {"name": "SK_DateID", "type": "BIGINT"},
        {"name": "Cash", "type": "DECIMAL(15,2)"},
        {"name": "BatchID", "type": "BIGINT"},
    ],
    # Composite key (SK_CustomerID, SK_AccountID, SK_DateID)
    "primary_key": ["SK_CustomerID", "SK_AccountID", "SK_DateID"],
}

# FactHoldings - Security positions and valuations
FACTHOLDINGS = {
    "name": "FactHoldings",
    "columns": [
        {"name": "SK_CustomerID", "type": "BIGINT"},
        {"name": "SK_AccountID", "type": "BIGINT"},
        {"name": "SK_SecurityID", "type": "BIGINT"},
        {"name": "SK_CompanyID", "type": "BIGINT"},
        {"name": "SK_DateID", "type": "BIGINT"},
        {"name": "SK_TimeID", "type": "BIGINT"},
        {"name": "CurrentPrice", "type": "DECIMAL(8,2)"},
        {"name": "CurrentHolding", "type": "BIGINT"},
        {"name": "BatchID", "type": "BIGINT"},
    ],
    # Composite key (SK_CustomerID, SK_AccountID, SK_SecurityID, SK_DateID)
    "primary_key": ["SK_CustomerID", "SK_AccountID", "SK_SecurityID", "SK_DateID"],
}

# FactMarketHistory - Daily security price and volume data
FACTMARKETHISTORY = {
    "name": "FactMarketHistory",
    "columns": [
        {"name": "SK_SecurityID", "type": "BIGINT"},
        {"name": "SK_CompanyID", "type": "BIGINT"},
        {"name": "SK_DateID", "type": "BIGINT"},
        {"name": "PERatio", "type": "DECIMAL(10,2)"},
        {"name": "Yield", "type": "DECIMAL(5,4)"},
        {"name": "FiftyTwoWeekHigh", "type": "DECIMAL(8,2)"},
        {"name": "SK_FiftyTwoWeekHighDate", "type": "BIGINT"},
        {"name": "FiftyTwoWeekLow", "type": "DECIMAL(8,2)"},
        {"name": "SK_FiftyTwoWeekLowDate", "type": "BIGINT"},
        {"name": "DividendPerShare", "type": "DECIMAL(8,4)"},
        {"name": "ClosePrice", "type": "DECIMAL(8,2)"},
        {"name": "DayHigh", "type": "DECIMAL(8,2)"},
        {"name": "DayLow", "type": "DECIMAL(8,2)"},
        {"name": "Volume", "type": "BIGINT"},
        {"name": "BatchID", "type": "BIGINT"},
    ],
    # Composite key (SK_SecurityID, SK_DateID)
    "primary_key": ["SK_SecurityID", "SK_DateID"],
}

# FactWatches - Customer watch list activity
FACTWATCHES = {
    "name": "FactWatches",
    "columns": [
        {"name": "SK_CustomerID", "type": "BIGINT"},
        {"name": "SK_SecurityID", "type": "BIGINT"},
        {"name": "SK_DateID_DatePlaced", "type": "BIGINT"},
        {"name": "SK_DateID_DateRemoved", "type": "BIGINT"},
        {"name": "BatchID", "type": "BIGINT"},
    ],
    # Composite key (SK_CustomerID, SK_SecurityID, SK_DateID_DatePlaced)
    "primary_key": ["SK_CustomerID", "SK_SecurityID", "SK_DateID_DatePlaced"],
}

# Industry classification lookup table
INDUSTRY = {
    "name": "Industry",
    "columns": [
        {"name": "IN_ID", "type": "VARCHAR(2)", "primary_key": True},
        {"name": "IN_NAME", "type": "VARCHAR(50)"},
        {"name": "IN_SC_ID", "type": "VARCHAR(4)"},  # Sector classification
    ],
}

# Status type lookup table
STATUSTYPE = {
    "name": "StatusType",
    "columns": [
        {"name": "ST_ID", "type": "VARCHAR(4)", "primary_key": True},
        {"name": "ST_NAME", "type": "VARCHAR(10)"},
    ],
}

# Tax rate lookup table
TAXRATE = {
    "name": "TaxRate",
    "columns": [
        {"name": "TX_ID", "type": "VARCHAR(4)", "primary_key": True},
        {"name": "TX_NAME", "type": "VARCHAR(50)"},
        {"name": "TX_RATE", "type": "DECIMAL(6,5)"},
    ],
}

# Trade type lookup table
TRADETYPE = {
    "name": "TradeType",
    "columns": [
        {"name": "TT_ID", "type": "VARCHAR(3)", "primary_key": True},
        {"name": "TT_NAME", "type": "VARCHAR(12)"},
        {"name": "TT_IS_SELL", "type": "BOOLEAN"},
        {"name": "TT_IS_MRKT", "type": "BOOLEAN"},
    ],
}

# Financial reference data - all additional tables
EXTENSION_TABLES = {
    "DimBroker": DIMBROKER,
    "FactCashBalances": FACTCASHBALANCES,
    "FactHoldings": FACTHOLDINGS,
    "FactMarketHistory": FACTMARKETHISTORY,
    "FactWatches": FACTWATCHES,
    "Industry": INDUSTRY,
    "StatusType": STATUSTYPE,
    "TaxRate": TAXRATE,
    "TradeType": TRADETYPE,
}


def get_extended_table_order() -> list[str]:
    """Get the dependency order for creating extended tables.

    Returns:
        List of table names in creation order (dependencies first)
    """
    return [
        # Reference/lookup tables first (no dependencies)
        "Industry",
        "StatusType",
        "TaxRate",
        "TradeType",
        # Dimension tables (depend on lookups)
        "DimBroker",
        # Fact tables (depend on dimensions)
        "FactCashBalances",
        "FactHoldings",
        "FactMarketHistory",
        "FactWatches",
    ]


def get_foreign_key_constraints() -> dict[str, list[dict[str, Any]]]:
    """Get foreign key constraint definitions for extended tables.

    Returns:
        Dictionary mapping table names to their foreign key constraints
    """
    return {
        "DimBroker": [
            {
                "column": "ManagerID",
                "references_table": "DimBroker",
                "references_column": "BrokerID",
                "constraint_name": "FK_DimBroker_Manager",
            }
        ],
        "FactCashBalances": [
            {
                "column": "SK_CustomerID",
                "references_table": "DimCustomer",
                "references_column": "SK_CustomerID",
                "constraint_name": "FK_FactCashBalances_Customer",
            },
            {
                "column": "SK_AccountID",
                "references_table": "DimAccount",
                "references_column": "SK_AccountID",
                "constraint_name": "FK_FactCashBalances_Account",
            },
            {
                "column": "SK_DateID",
                "references_table": "DimDate",
                "references_column": "SK_DateID",
                "constraint_name": "FK_FactCashBalances_Date",
            },
        ],
        "FactHoldings": [
            {
                "column": "SK_CustomerID",
                "references_table": "DimCustomer",
                "references_column": "SK_CustomerID",
                "constraint_name": "FK_FactHoldings_Customer",
            },
            {
                "column": "SK_AccountID",
                "references_table": "DimAccount",
                "references_column": "SK_AccountID",
                "constraint_name": "FK_FactHoldings_Account",
            },
            {
                "column": "SK_SecurityID",
                "references_table": "DimSecurity",
                "references_column": "SK_SecurityID",
                "constraint_name": "FK_FactHoldings_Security",
            },
            {
                "column": "SK_CompanyID",
                "references_table": "DimCompany",
                "references_column": "SK_CompanyID",
                "constraint_name": "FK_FactHoldings_Company",
            },
            {
                "column": "SK_DateID",
                "references_table": "DimDate",
                "references_column": "SK_DateID",
                "constraint_name": "FK_FactHoldings_Date",
            },
            {
                "column": "SK_TimeID",
                "references_table": "DimTime",
                "references_column": "SK_TimeID",
                "constraint_name": "FK_FactHoldings_Time",
            },
        ],
        "FactMarketHistory": [
            {
                "column": "SK_SecurityID",
                "references_table": "DimSecurity",
                "references_column": "SK_SecurityID",
                "constraint_name": "FK_FactMarketHistory_Security",
            },
            {
                "column": "SK_CompanyID",
                "references_table": "DimCompany",
                "references_column": "SK_CompanyID",
                "constraint_name": "FK_FactMarketHistory_Company",
            },
            {
                "column": "SK_DateID",
                "references_table": "DimDate",
                "references_column": "SK_DateID",
                "constraint_name": "FK_FactMarketHistory_Date",
            },
            {
                "column": "SK_FiftyTwoWeekHighDate",
                "references_table": "DimDate",
                "references_column": "SK_DateID",
                "constraint_name": "FK_FactMarketHistory_HighDate",
            },
            {
                "column": "SK_FiftyTwoWeekLowDate",
                "references_table": "DimDate",
                "references_column": "SK_DateID",
                "constraint_name": "FK_FactMarketHistory_LowDate",
            },
        ],
        "FactWatches": [
            {
                "column": "SK_CustomerID",
                "references_table": "DimCustomer",
                "references_column": "SK_CustomerID",
                "constraint_name": "FK_FactWatches_Customer",
            },
            {
                "column": "SK_SecurityID",
                "references_table": "DimSecurity",
                "references_column": "SK_SecurityID",
                "constraint_name": "FK_FactWatches_Security",
            },
            {
                "column": "SK_DateID_DatePlaced",
                "references_table": "DimDate",
                "references_column": "SK_DateID",
                "constraint_name": "FK_FactWatches_DatePlaced",
            },
            {
                "column": "SK_DateID_DateRemoved",
                "references_table": "DimDate",
                "references_column": "SK_DateID",
                "constraint_name": "FK_FactWatches_DateRemoved",
            },
        ],
    }


def get_extended_create_table_sql(
    table_name: str,
    dialect: str = "standard",
    enable_primary_keys: bool = True,
    enable_foreign_keys: bool = True,
) -> str:
    """Generate CREATE TABLE SQL for extended TPC-DI tables.

    Args:
        table_name: Name of the table to create
        dialect: SQL dialect to use
        enable_primary_keys: Whether to include primary key constraints
        enable_foreign_keys: Whether to include foreign key constraints

    Returns:
        CREATE TABLE SQL statement

    Raises:
        ValueError: If table_name is not valid
    """
    if table_name not in EXTENSION_TABLES:
        raise ValueError(f"Unknown extended table: {table_name}. Available: {', '.join(EXTENSION_TABLES.keys())}")

    table = EXTENSION_TABLES[table_name]
    columns = []

    for col in table["columns"]:
        col_def = f"{col['name']} {col['type']}"
        if col.get("primary_key") and enable_primary_keys:
            col_def += " PRIMARY KEY"
        columns.append(col_def)

    sql = f"CREATE TABLE IF NOT EXISTS {table['name']} (\n"
    sql += ",\n".join(f"  {col}" for col in columns)

    # Add composite primary key if defined
    if "primary_key" in table and enable_primary_keys:
        pk_columns = ", ".join(table["primary_key"])
        sql += f",\n  PRIMARY KEY ({pk_columns})"

    sql += "\n);"

    return sql


def get_all_extended_create_table_sql(
    dialect: str = "standard",
    enable_primary_keys: bool = True,
    enable_foreign_keys: bool = True,
) -> str:
    """Generate CREATE TABLE SQL for all extended TPC-DI tables.

    Args:
        dialect: SQL dialect to use
        enable_primary_keys: Whether to include primary key constraints
        enable_foreign_keys: Whether to include foreign key constraints

    Returns:
        Complete SQL schema creation script for extended tables
    """
    table_order = get_extended_table_order()

    sql_statements = []
    for table_name in table_order:
        sql_statements.append(
            get_extended_create_table_sql(table_name, dialect, enable_primary_keys, enable_foreign_keys)
        )

    return "\n\n".join(sql_statements)
