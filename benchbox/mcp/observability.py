"""Observability module for BenchBox MCP server.

Provides structured logging, metrics collection, and correlation IDs
for improved debugging and monitoring of MCP tool calls.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import json
import logging
import sys
import time
import uuid
from collections import defaultdict
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any

# Correlation ID storage using contextvars for proper async/thread safety
# ContextVar provides isolation between concurrent async tasks and threads
_correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)


def generate_correlation_id() -> str:
    """Generate a unique correlation ID for request tracing."""
    return f"mcp_{uuid.uuid4().hex[:12]}"


def get_correlation_id() -> str | None:
    """Get the current correlation ID for this context."""
    return _correlation_id_var.get()


def set_correlation_id(correlation_id: str | None) -> None:
    """Set the correlation ID for this context."""
    _correlation_id_var.set(correlation_id)


class JSONFormatter(logging.Formatter):
    """JSON log formatter for structured logging.

    Outputs log records as JSON objects with consistent fields for
    easy parsing by log aggregation systems.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add correlation ID if available
        correlation_id = get_correlation_id()
        if correlation_id:
            log_data["correlation_id"] = correlation_id

        # Add tool context if present
        if hasattr(record, "tool_name"):
            log_data["tool_name"] = record.tool_name
        if hasattr(record, "tool_duration_ms"):
            log_data["tool_duration_ms"] = record.tool_duration_ms
        if hasattr(record, "tool_status"):
            log_data["tool_status"] = record.tool_status

        # Add extra fields from record
        if hasattr(record, "extra"):
            log_data.update(record.extra)

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, default=str)


def setup_structured_logging(
    logger_name: str = "benchbox.mcp",
    level: int = logging.INFO,
    use_json: bool = True,
) -> logging.Logger:
    """Configure structured logging for the MCP server.

    Args:
        logger_name: Logger name to configure
        level: Logging level
        use_json: Whether to use JSON format (True) or standard format (False)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Remove existing handlers
    logger.handlers.clear()

    # Create handler for stderr (stdout reserved for MCP JSON-RPC)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)

    if use_json:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

    logger.addHandler(handler)

    return logger


@dataclass
class ToolCallMetrics:
    """Metrics for a single tool call."""

    tool_name: str
    start_time: float = field(default_factory=time.perf_counter)
    end_time: float | None = None
    success: bool = True
    error_code: str | None = None

    @property
    def duration_ms(self) -> float | None:
        """Get duration in milliseconds."""
        if self.end_time is None:
            return None
        return (self.end_time - self.start_time) * 1000

    def complete(self, success: bool = True, error_code: str | None = None) -> None:
        """Mark the tool call as complete."""
        self.end_time = time.perf_counter()
        self.success = success
        self.error_code = error_code


@dataclass
class MetricsCollector:
    """Collects and aggregates metrics for MCP tool calls.

    Thread-safe collector that tracks:
    - Total call count per tool
    - Success/error counts
    - Duration statistics (p50, p95, p99)
    """

    _lock: Lock = field(default_factory=Lock, repr=False)
    _call_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _success_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _error_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _durations: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    _max_duration_samples: int = 1000  # Keep last N samples per tool

    def record_call(self, metrics: ToolCallMetrics) -> None:
        """Record a completed tool call.

        Args:
            metrics: Metrics from the completed call
        """
        with self._lock:
            tool = metrics.tool_name
            self._call_counts[tool] += 1

            if metrics.success:
                self._success_counts[tool] += 1
            else:
                self._error_counts[tool] += 1

            if metrics.duration_ms is not None:
                durations = self._durations[tool]
                durations.append(metrics.duration_ms)
                # Keep only recent samples
                if len(durations) > self._max_duration_samples:
                    self._durations[tool] = durations[-self._max_duration_samples :]

    def get_stats(self, tool_name: str | None = None) -> dict[str, Any]:
        """Get statistics for a tool or all tools.

        Args:
            tool_name: Specific tool name, or None for all tools

        Returns:
            Statistics dictionary
        """
        with self._lock:
            if tool_name:
                return self._get_tool_stats(tool_name)
            else:
                return {
                    "tools": {name: self._get_tool_stats(name) for name in self._call_counts},
                    "summary": {
                        "total_calls": sum(self._call_counts.values()),
                        "total_success": sum(self._success_counts.values()),
                        "total_errors": sum(self._error_counts.values()),
                    },
                }

    def _get_tool_stats(self, tool_name: str) -> dict[str, Any]:
        """Get statistics for a single tool."""
        durations = self._durations.get(tool_name, [])

        stats: dict[str, Any] = {
            "calls": self._call_counts.get(tool_name, 0),
            "success": self._success_counts.get(tool_name, 0),
            "errors": self._error_counts.get(tool_name, 0),
        }

        if durations:
            sorted_durations = sorted(durations)
            n = len(sorted_durations)
            stats["duration_ms"] = {
                "min": round(sorted_durations[0], 2),
                "max": round(sorted_durations[-1], 2),
                "avg": round(sum(sorted_durations) / n, 2),
                "p50": round(sorted_durations[n // 2], 2),
                "p95": round(sorted_durations[int(n * 0.95)], 2) if n >= 20 else None,
                "p99": round(sorted_durations[int(n * 0.99)], 2) if n >= 100 else None,
                "samples": n,
            }

        return stats

    def reset(self) -> None:
        """Reset all collected metrics."""
        with self._lock:
            self._call_counts.clear()
            self._success_counts.clear()
            self._error_counts.clear()
            self._durations.clear()


# Global metrics collector instance
_metrics_collector: MetricsCollector | None = None


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector instance."""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


class ToolCallContext:
    """Context manager for tracking tool calls.

    Usage:
        with ToolCallContext("list_platforms") as ctx:
            result = do_work()
            if error:
                ctx.mark_error("VALIDATION_ERROR")

    This automatically:
    - Generates/uses correlation ID
    - Tracks timing
    - Logs start/completion
    - Records metrics
    """

    def __init__(
        self,
        tool_name: str,
        logger: logging.Logger | None = None,
        correlation_id: str | None = None,
    ):
        self.tool_name = tool_name
        self.logger = logger or logging.getLogger("benchbox.mcp.tools")
        self.correlation_id = correlation_id or generate_correlation_id()
        self.metrics: ToolCallMetrics | None = None
        self._success = True
        self._error_code: str | None = None

    def __enter__(self) -> ToolCallContext:
        """Enter the context, starting timing and logging."""
        set_correlation_id(self.correlation_id)
        self.metrics = ToolCallMetrics(tool_name=self.tool_name)

        # Log tool call start
        extra = {"tool_name": self.tool_name}
        self.logger.info(f"Tool call started: {self.tool_name}", extra={"extra": extra})

        return self

    def __exit__(self, exc_type: type | None, exc_val: Exception | None, exc_tb: Any) -> None:
        """Exit the context, completing metrics and logging."""
        if self.metrics is None:
            return

        # Handle exceptions
        if exc_type is not None:
            self._success = False
            self._error_code = exc_type.__name__

        # Complete metrics
        self.metrics.complete(success=self._success, error_code=self._error_code)

        # Record to collector
        get_metrics_collector().record_call(self.metrics)

        # Log completion
        extra = {
            "tool_name": self.tool_name,
            "tool_duration_ms": round(self.metrics.duration_ms or 0, 2),
            "tool_status": "success" if self._success else "error",
        }
        if self._error_code:
            extra["error_code"] = self._error_code

        level = logging.INFO if self._success else logging.WARNING
        self.logger.log(
            level,
            f"Tool call completed: {self.tool_name} ({extra['tool_duration_ms']:.2f}ms)",
            extra={"extra": extra},
        )

        # Clear correlation ID
        set_correlation_id(None)

    def mark_error(self, error_code: str) -> None:
        """Mark the tool call as failed with an error code.

        Args:
            error_code: Error code (e.g., "VALIDATION_ERROR")
        """
        self._success = False
        self._error_code = error_code
