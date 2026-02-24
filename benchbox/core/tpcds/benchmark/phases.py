"""TPC-DS benchmark phase identifiers."""

from typing import ClassVar


class BenchmarkPhase:
    """TPC-DS benchmark phases."""

    POWER: ClassVar[str] = "power"
    THROUGHPUT: ClassVar[str] = "throughput"
    MAINTENANCE: ClassVar[str] = "maintenance"


__all__ = ["BenchmarkPhase"]
