"""Benchmark-specific constants and defaults.

Contains specification-defined defaults for TPC benchmarks and other
configuration values that should be consistent across the codebase.

Copyright 2026 Joe Harris / BenchBox Project
Licensed under the MIT License. See LICENSE file in the project root for details.
"""

# TPC-H Power Test Defaults (TPC-H Specification Section 4.2)
# The Power Test measures query execution performance with a single stream
# Specification requires: 1 qualification run (warmup) + measured runs
TPCH_POWER_DEFAULT_WARMUP_ITERATIONS = 1
TPCH_POWER_DEFAULT_MEASUREMENT_ITERATIONS = 3

# TPC-DS Power Test Defaults (TPC-DS Specification Section 4.2.2)
# TPC-DS uses different defaults than TPC-H
TPCDS_POWER_DEFAULT_WARMUP_ITERATIONS = 1
TPCDS_POWER_DEFAULT_MEASUREMENT_ITERATIONS = 3

# TPC-H Throughput Test Defaults (TPC-H Specification Section 4.3)
TPCH_THROUGHPUT_DEFAULT_STREAMS = 2

# TPC-DS Throughput Test Defaults (TPC-DS Specification Section 4.3)
TPCDS_THROUGHPUT_DEFAULT_STREAMS = 2

# Generic Power Test Defaults (for non-TPC benchmarks)
# Based on TPC-H best practices: warmup to prime caches, multiple measurements for statistical validity
GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS = 1
GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS = 3

# Generic Throughput Test Defaults (for non-TPC benchmarks)
GENERIC_THROUGHPUT_DEFAULT_STREAMS = 2
