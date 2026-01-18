"""Metadata Primitives benchmark module.

This module provides functionality to run metadata introspection benchmarks that test
database catalog performance. Unlike Read/Write/Transaction primitives that test data
operations, this benchmark focuses on metadata operations critical for:

- Data catalog integration
- Schema discovery tools
- IDE autocomplete performance
- BI tool connectivity
- Data governance workflows

The benchmark tests INFORMATION_SCHEMA views, SHOW commands, DESCRIBE operations,
and query execution plans across multiple database platforms.

Complexity Testing:
The module also supports metadata complexity stress testing through the MetadataGenerator
class, which creates complex metadata structures (wide tables, nested views, complex types)
to measure how introspection performance scales with schema complexity.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from .benchmark import (
    AclBenchmarkResult,
    AclMutationResult,
    ComplexityBenchmarkResult,
    MetadataBenchmarkResult,
    MetadataPrimitivesBenchmark,
    MetadataQueryResult,
)
from .complexity import (
    COMPLEXITY_PRESETS,
    AclGrant,
    ConstraintDensity,
    GeneratedMetadata,
    MetadataComplexityConfig,
    PermissionDensity,
    RoleHierarchyDepth,
    TypeComplexity,
    get_complexity_preset,
)
from .generator import MetadataGenerator
from .queries import MetadataPrimitivesQueryManager

__all__ = [
    # Benchmark classes
    "AclBenchmarkResult",
    "AclMutationResult",
    "ComplexityBenchmarkResult",
    "MetadataBenchmarkResult",
    "MetadataPrimitivesBenchmark",
    "MetadataPrimitivesQueryManager",
    "MetadataQueryResult",
    # Complexity testing
    "COMPLEXITY_PRESETS",
    "AclGrant",
    "ConstraintDensity",
    "GeneratedMetadata",
    "MetadataComplexityConfig",
    "MetadataGenerator",
    "PermissionDensity",
    "RoleHierarchyDepth",
    "TypeComplexity",
    "get_complexity_preset",
]
