"""TPC-DS benchmark phase identifiers."""

from typing import ClassVar


class BenchmarkPhase:
    """TPC-DS benchmark phases (legacy compatibility)."""

    POWER_TEST: ClassVar[str] = "power_test"
    THROUGHPUT_TEST: ClassVar[str] = "throughput_test"
    MAINTENANCE_TEST: ClassVar[str] = "maintenance_test"


__all__ = ["BenchmarkPhase"]
