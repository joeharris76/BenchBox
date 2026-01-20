"""ClickBench (ClickHouse Analytics Benchmark) package.

ClickBench is a benchmark for analytical database management systems that consists
of 43 SQL queries running against a single flat table with web analytics data.

The benchmark was created to evaluate various DBMS for web analytics systems using
realistic data distributions from production traffic logs.

Key characteristics:
- Single flat table with ~100M records
- 43 analytical queries covering different patterns
- Web analytics domain (traffic, pageviews, user sessions)
- Real-world data distributions from anonymized production data

For more information, see:
- https://github.com/ClickHouse/ClickBench
- https://benchmark.clickhouse.com/

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from .benchmark import ClickBenchBenchmark
from .generator import ClickBenchDataGenerator
from .queries import ClickBenchQueryManager
from .schema import HITS_TABLE, get_create_table_sql

__all__ = [
    "ClickBenchBenchmark",
    "ClickBenchDataGenerator",
    "ClickBenchQueryManager",
    "HITS_TABLE",
    "get_create_table_sql",
]
