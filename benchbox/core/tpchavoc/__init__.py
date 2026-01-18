"""TPC-Havoc benchmark implementation module.

TPC-Havoc is a benchmark based on TPC-H that generates 10 structural variants
of each TPC-H query to stress different aspects of query optimizers.

Copyright 2026 Joe Harris / BenchBox Project

This implementation is derived from TPC Benchmark™ H (TPC-H) - Copyright © Transaction Processing Performance Council

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from .benchmark import TPCHavocBenchmark

__all__ = ["TPCHavocBenchmark"]
