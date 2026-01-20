"""TPC-H Skew benchmark implementation.

This module provides a TPC-H benchmark variant that introduces data skew
to test database performance under realistic data distribution patterns.

Based on the research: "Introducing Skew into the TPC-H Benchmark"
Reference: https://www.tpc.org/tpctc/tpctc2011/slides_and_papers/introducing_skew_into_the_tpc_h_benchmark.pdf

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ H (TPC-H) - Copyright © Transaction Processing Performance Council
This implementation extends the TPC-H specification with skew distributions.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from benchbox.core.tpch_skew.benchmark import TPCHSkewBenchmark
from benchbox.core.tpch_skew.distributions import (
    ExponentialDistribution,
    NormalDistribution,
    SkewDistribution,
    UniformDistribution,
    ZipfianDistribution,
)
from benchbox.core.tpch_skew.generator import TPCHSkewDataGenerator
from benchbox.core.tpch_skew.skew_config import (
    AttributeSkewConfig,
    JoinSkewConfig,
    SkewConfiguration,
    SkewPreset,
    SkewType,
    TemporalSkewConfig,
    get_preset_config,
)

__all__ = [
    "TPCHSkewBenchmark",
    "TPCHSkewDataGenerator",
    "SkewConfiguration",
    "SkewType",
    "SkewPreset",
    "AttributeSkewConfig",
    "JoinSkewConfig",
    "TemporalSkewConfig",
    "get_preset_config",
    "SkewDistribution",
    "ZipfianDistribution",
    "NormalDistribution",
    "ExponentialDistribution",
    "UniformDistribution",
]
