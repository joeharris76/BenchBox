"""H2O DB benchmark package.

This package provides a complete implementation of the H2O DB benchmark,
which tests analytical database performance using taxi trip data.

The H2O DB benchmark features:
1. A single large table with synthetic taxi trip records
2. 10 analytical queries testing various aspects of performance
3. Focus on aggregations, grouping, and analytical functions
4. Based on NYC Taxi & Limousine Commission Trip Record Data structure

For more information, see:
- https://h2oai.github.io/db-benchmark/

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from .benchmark import H2OBenchmark
from .generator import H2ODataGenerator
from .queries import H2OQueryManager
from .schema import (
    TABLES,
    TRIPS,
    get_all_create_table_sql,
    get_create_table_sql,
)

__all__ = [
    "H2OBenchmark",
    "H2ODataGenerator",
    "H2OQueryManager",
    "TRIPS",
    "TABLES",
    "get_create_table_sql",
    "get_all_create_table_sql",
]
