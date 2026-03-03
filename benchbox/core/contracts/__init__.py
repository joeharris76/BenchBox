"""Runtime contracts for core benchmark/lifecycle integration."""

from benchbox.core.contracts.benchmark_runtime import (
    EXTENDED_RUNTIME_METHODS,
    REQUIRED_RUNTIME_METHODS,
    BenchmarkExtendedRuntimeContract,
    BenchmarkRuntimeContract,
    get_missing_methods,
    has_extended_runtime_contract,
    has_runtime_contract,
)

__all__ = [
    "BenchmarkRuntimeContract",
    "BenchmarkExtendedRuntimeContract",
    "REQUIRED_RUNTIME_METHODS",
    "EXTENDED_RUNTIME_METHODS",
    "get_missing_methods",
    "has_runtime_contract",
    "has_extended_runtime_contract",
]
