"""Timeout management utilities for BenchBox.

Provides cross-platform timeout enforcement for benchmark operations,
preventing runaway queries from consuming resources indefinitely.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class TimeoutError(Exception):
    """Raised when an operation exceeds its timeout limit."""

    def __init__(self, message: str, timeout_seconds: float, operation: str = ""):
        self.timeout_seconds = timeout_seconds
        self.operation = operation
        super().__init__(message)


@dataclass
class TimeoutConfig:
    """Configuration for timeout behavior."""

    timeout_seconds: float
    operation_name: str = "operation"
    raise_on_timeout: bool = True
    on_timeout: Callable[[], None] | None = None

    def __post_init__(self):
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")


class TimeoutManager:
    """Manages timeout enforcement for operations.

    Uses threading-based timeout for cross-platform compatibility.
    Note: This cannot interrupt blocking I/O operations (like database queries)
    but can interrupt pure Python code and provide warning/logging.

    For database operations, prefer using database-specific timeout mechanisms
    (e.g., statement_timeout in PostgreSQL, query_timeout in drivers).

    Example:
        >>> manager = TimeoutManager()
        >>> with manager.timeout_context(30, "query_execution"):
        ...     run_query()  # Will raise TimeoutError if > 30 seconds
    """

    def __init__(self, default_timeout_seconds: float = 300):
        """Initialize timeout manager.

        Args:
            default_timeout_seconds: Default timeout for operations (5 minutes)
        """
        self.default_timeout_seconds = default_timeout_seconds
        self._active_timers: dict[int, threading.Timer] = {}
        self._lock = threading.Lock()

    @contextmanager
    def timeout_context(
        self,
        timeout_seconds: float | None = None,
        operation_name: str = "operation",
        raise_on_timeout: bool = True,
    ):
        """Context manager that enforces a timeout.

        Args:
            timeout_seconds: Timeout in seconds (None uses default)
            operation_name: Name for logging/error messages
            raise_on_timeout: Whether to raise TimeoutError on timeout

        Yields:
            TimeoutContext with status information

        Raises:
            TimeoutError: If operation exceeds timeout and raise_on_timeout=True

        Example:
            >>> with manager.timeout_context(60, "benchmark_run") as ctx:
            ...     result = run_benchmark()
            >>> if ctx.timed_out:
            ...     print("Operation timed out")
        """
        timeout = timeout_seconds or self.default_timeout_seconds
        context = _TimeoutContext(timeout, operation_name)

        # Start the timer
        timer = threading.Timer(timeout, context._trigger_timeout)
        timer.daemon = True

        timer_id = id(context)
        with self._lock:
            self._active_timers[timer_id] = timer

        timer.start()

        try:
            yield context
        finally:
            # Cancel the timer if operation completed
            timer.cancel()
            with self._lock:
                self._active_timers.pop(timer_id, None)

            if context.timed_out and raise_on_timeout:
                raise TimeoutError(
                    f"{operation_name} timed out after {timeout:.1f} seconds",
                    timeout_seconds=timeout,
                    operation=operation_name,
                )

    def cancel_all_timers(self):
        """Cancel all active timers. Useful during shutdown."""
        with self._lock:
            for timer in self._active_timers.values():
                timer.cancel()
            self._active_timers.clear()


class _TimeoutContext:
    """Internal context object tracking timeout state."""

    def __init__(self, timeout_seconds: float, operation_name: str):
        self.timeout_seconds = timeout_seconds
        self.operation_name = operation_name
        self.timed_out = False
        self._triggered_at: float | None = None

    def _trigger_timeout(self):
        """Called by timer when timeout expires."""
        import time

        self.timed_out = True
        self._triggered_at = time.time()
        logger.warning(f"Timeout triggered for {self.operation_name} after {self.timeout_seconds:.1f}s")

    @property
    def triggered_at(self) -> float | None:
        """Return timestamp when timeout was triggered, or None if not triggered."""
        return self._triggered_at


def run_with_timeout(
    func: Callable[..., T],
    timeout_seconds: float,
    operation_name: str = "operation",
    *args: Any,
    **kwargs: Any,
) -> tuple[T | None, bool]:
    """Run a function with a timeout.

    Note: This uses threading and cannot interrupt blocking I/O.
    For database operations, use database-level timeouts instead.

    Args:
        func: Function to execute
        timeout_seconds: Maximum execution time
        operation_name: Name for logging
        *args: Positional arguments for func
        **kwargs: Keyword arguments for func

    Returns:
        Tuple of (result, timed_out):
        - result: Function return value, or None if timed out
        - timed_out: True if operation timed out

    Example:
        >>> result, timed_out = run_with_timeout(slow_query, 30, "query_1")
        >>> if timed_out:
        ...     print("Query timed out")
    """
    result_container: dict[str, Any] = {"result": None, "exception": None}

    def wrapper():
        try:
            result_container["result"] = func(*args, **kwargs)
        except Exception as e:
            result_container["exception"] = e

    thread = threading.Thread(target=wrapper, daemon=True)
    thread.start()
    thread.join(timeout=timeout_seconds)

    if thread.is_alive():
        logger.warning(f"{operation_name} timed out after {timeout_seconds:.1f}s (thread still running)")
        return None, True

    if result_container["exception"] is not None:
        raise result_container["exception"]

    return result_container["result"], False


# Global singleton for convenience
_default_manager: TimeoutManager | None = None


def get_timeout_manager() -> TimeoutManager:
    """Get the global timeout manager instance."""
    global _default_manager
    if _default_manager is None:
        _default_manager = TimeoutManager()
    return _default_manager


@contextmanager
def timeout(timeout_seconds: float, operation_name: str = "operation", raise_on_timeout: bool = True):
    """Convenience function for timeout context.

    Args:
        timeout_seconds: Timeout in seconds
        operation_name: Name for logging/error messages
        raise_on_timeout: Whether to raise TimeoutError on timeout

    Yields:
        TimeoutContext with status information

    Example:
        >>> with timeout(60, "benchmark"):
        ...     run_benchmark()
    """
    manager = get_timeout_manager()
    with manager.timeout_context(timeout_seconds, operation_name, raise_on_timeout) as ctx:
        yield ctx
