"""TPC-H benchmark package.

This package provides a complete implementation of the TPC-H benchmark,
including:

1. Data generation according to the TPC-H specification
2. Query execution for the 22 TPC-H queries
3. Schema definition for all TPC-H tables
4. Result validation and benchmark reporting

For more information on TPC-H, see http://www.tpc.org/tpch

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ H (TPC-H) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-H specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from .benchmark import TPCHBenchmark
from .generator import TPCHDataGenerator
from .queries import TPCHQueryManager
from .schema import (
    CUSTOMER,
    LINEITEM,
    NATION,
    ORDERS,
    PART,
    PARTSUPP,
    REGION,
    SUPPLIER,
    TABLES,
)

__all__ = [
    "TPCHBenchmark",
    "TPCHDataGenerator",
    "TPCHQueryManager",
    "CUSTOMER",
    "LINEITEM",
    "NATION",
    "ORDERS",
    "PART",
    "PARTSUPP",
    "REGION",
    "SUPPLIER",
    "TABLES",
]
