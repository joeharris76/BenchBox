"""TPC-DI (Data Integration) benchmark package.

This package provides a complete implementation of the TPC-DI benchmark,
which tests data integration and ETL processes for data warehousing scenarios.

The TPC-DI benchmark features:
1. A financial services data warehouse schema with dimension and fact tables
2. Data integration scenarios with Slowly Changing Dimensions (SCD)
3. Validation queries for data quality
4. Analytical queries for business intelligence
5. Focus on ETL performance and data integration processes

Note: This is a simplified implementation that generates sample target data.
The full TPC-DI benchmark includes complex ETL transformations from various
source file formats (CSV, XML, text) to the target data warehouse.

For more information, see:
- http://www.tpc.org/tpcdi/

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DI (TPC-DI) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-DI specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from .benchmark import TPCDIBenchmark
from .generator import TPCDIDataGenerator
from .queries import TPCDIQueryManager
from .schema import (
    DIMACCOUNT,
    DIMCOMPANY,
    DIMCUSTOMER,
    DIMDATE,
    DIMSECURITY,
    DIMTIME,
    FACTTRADE,
    TABLES,
    get_all_create_table_sql,
    get_create_table_sql,
)

__all__ = [
    "TPCDIBenchmark",
    "TPCDIDataGenerator",
    "TPCDIQueryManager",
    "DIMCUSTOMER",
    "DIMACCOUNT",
    "DIMSECURITY",
    "DIMCOMPANY",
    "FACTTRADE",
    "DIMDATE",
    "DIMTIME",
    "TABLES",
    "get_create_table_sql",
    "get_all_create_table_sql",
]
