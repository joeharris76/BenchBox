"""Structural runtime contracts for loader-resolved benchmark instances."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol, runtime_checkable

# Methods needed for lifecycle result construction on all loader-resolved benchmarks.
REQUIRED_RUNTIME_METHODS: tuple[str, ...] = (
    "generate_data",
    "get_query",
    "get_queries",
    "create_enhanced_benchmark_result",
)

# Extended lifecycle/validation methods targeted for full harmonization.
EXTENDED_RUNTIME_METHODS: tuple[str, ...] = (
    "create_minimal_benchmark_result",
    "validate_preflight",
    "validate_manifest",
    "validate_loaded_data",
)


@runtime_checkable
class BenchmarkRuntimeContract(Protocol):
    """Minimum benchmark runtime contract used by lifecycle execution."""

    scale_factor: float

    @property
    def benchmark_name(self) -> str:
        """Human-readable benchmark name."""

    def generate_data(self, *args: Any, **kwargs: Any) -> Any:
        """Generate benchmark data artifacts."""

    def get_queries(self, *args: Any, **kwargs: Any) -> dict[str, str]:
        """Return benchmark query catalog."""

    def get_query(self, query_id: int | str, *args: Any, **kwargs: Any) -> str:
        """Return one benchmark query."""

    def create_enhanced_benchmark_result(
        self,
        platform: str,
        query_results: list[dict[str, Any]],
        **kwargs: Any,
    ) -> Any:
        """Build the canonical BenchmarkResults payload."""


@runtime_checkable
class BenchmarkExtendedRuntimeContract(BenchmarkRuntimeContract, Protocol):
    """Target full lifecycle contract for harmonized benchmark behavior."""

    def create_minimal_benchmark_result(self, **kwargs: Any) -> Any:
        """Build minimal BenchmarkResults for fail/interrupt paths."""

    def validate_preflight(self, **kwargs: Any) -> Any:
        """Run preflight validation checks."""

    def validate_manifest(self, **kwargs: Any) -> Any:
        """Validate generated data manifest."""

    def validate_loaded_data(self, connection: Any, **kwargs: Any) -> Any:
        """Validate post-load database state."""


def get_missing_methods(instance: Any, method_names: Iterable[str]) -> list[str]:
    """Return sorted runtime methods missing from an instance."""

    missing: list[str] = []
    for method_name in method_names:
        attr = getattr(instance, method_name, None)
        if not callable(attr):
            missing.append(method_name)
    return sorted(missing)


def has_runtime_contract(instance: Any) -> bool:
    """Whether the instance satisfies the minimum runtime method set."""

    return not get_missing_methods(instance, REQUIRED_RUNTIME_METHODS)


def has_extended_runtime_contract(instance: Any) -> bool:
    """Whether the instance satisfies the extended lifecycle/validation set."""

    method_set = REQUIRED_RUNTIME_METHODS + EXTENDED_RUNTIME_METHODS
    return not get_missing_methods(instance, method_set)
