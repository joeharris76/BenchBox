"""AMPLab Big Data Benchmark package.

This package provides a complete implementation of the AMPLab Big Data Benchmark,
which tests big data processing systems using web analytics workloads.

The AMPLab benchmark features:
1. Three tables representing web analytics data (rankings, uservisits, documents)
2. Multiple query types testing scan, join, and analytical operations
3. Focus on big data processing performance and scalability
4. Synthetic data generation based on web crawl and user interaction patterns

For more information, see:
- https://amplab.cs.berkeley.edu/benchmark/

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from .benchmark import AMPLabBenchmark
from .generator import AMPLabDataGenerator
from .queries import AMPLabQueryManager
from .schema import (
    DOCUMENTS,
    RANKINGS,
    TABLES,
    USERVISITS,
    get_all_create_table_sql,
    get_create_table_sql,
)

__all__ = [
    "AMPLabBenchmark",
    "AMPLabDataGenerator",
    "AMPLabQueryManager",
    "RANKINGS",
    "USERVISITS",
    "DOCUMENTS",
    "TABLES",
    "get_create_table_sql",
    "get_all_create_table_sql",
]
