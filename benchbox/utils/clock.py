"""Centralized clock helpers for elapsed timing and wall-clock timestamps.

Use monotonic clocks for elapsed duration and timeout calculations.
Use UTC wall-clock timestamps for user-facing/audit metadata.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Iterator


def mono_time() -> float:
    """Return a monotonic timestamp suitable for elapsed calculations."""
    return perf_counter()


def elapsed_seconds(start: float, end: float | None = None) -> float:
    """Return elapsed seconds between monotonic timestamps."""
    end_time = mono_time() if end is None else end
    return end_time - start


def utc_now() -> datetime:
    """Return the current timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


@dataclass
class Stopwatch:
    """Pair monotonic timing with wall-clock boundaries."""

    start_mono: float
    start_utc: datetime

    @classmethod
    def start(cls) -> Stopwatch:
        """Start a new stopwatch."""
        return cls(start_mono=mono_time(), start_utc=utc_now())

    def elapsed_seconds(self) -> float:
        """Return elapsed seconds since stopwatch start."""
        return elapsed_seconds(self.start_mono)

    def elapsed_ms(self) -> float:
        """Return elapsed milliseconds since stopwatch start."""
        return self.elapsed_seconds() * 1000.0

    def finish(self) -> tuple[datetime, float]:
        """Return finish UTC timestamp and elapsed seconds."""
        return utc_now(), self.elapsed_seconds()


@contextmanager
def measure_elapsed() -> Iterator[Stopwatch]:
    """Context manager yielding a started stopwatch."""
    stopwatch = Stopwatch.start()
    yield stopwatch
