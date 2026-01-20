"""Monitoring utilities exposed for public consumption.

This module provides opt-in performance tracking helpers for custom benchmark
implementations. The monitoring functionality is stable but not automatically
enabled - users must explicitly create monitors and attach snapshots to results.

API Stability: Stable as of v1.0. Breaking changes will follow SemVer.

Example usage:
    >>> from benchbox.monitoring import PerformanceMonitor, attach_snapshot_to_result
    >>> monitor = PerformanceMonitor()
    >>> monitor.increment_counter("queries_executed", 22)
    >>> monitor.record_timing("query_1", 1.23)
    >>> snapshot = monitor.take_snapshot()
    >>> # Attach to BenchmarkResults if desired

Enhanced resource monitoring:
    >>> from benchbox.monitoring import EnhancedResourceProfiler, ResourceReporter
    >>> profiler = EnhancedResourceProfiler(sample_interval=1.0)
    >>> profiler.start()
    >>> # ... run benchmark ...
    >>> profiler.stop()
    >>> reporter = ResourceReporter()
    >>> report = reporter.generate_report(profiler.get_timeline())
    >>> print(report.generate_text_report())
"""

from .bottleneck import (
    BottleneckAnalysis,
    BottleneckDetector,
    BottleneckIndicator,
    BottleneckSeverity,
    BottleneckType,
    quick_bottleneck_check,
)
from .performance import (
    PerformanceHistory,
    PerformanceMonitor,
    PerformanceRegressionAlert,
    PerformanceSnapshot,
    PerformanceTracker,
    ResourceMonitor,
    TimingStats,
    attach_snapshot_to_result,
)
from .profiler import (
    EnhancedResourceProfiler,
    ResourceSample,
    ResourceTimeline,
    ResourceType,
    ResourceUtilization,
    calculate_utilization,
)
from .report import (
    ResourceChart,
    ResourceReport,
    ResourceReporter,
    format_bytes,
    format_duration,
    generate_ascii_chart,
)

__all__ = [
    # Performance monitoring (existing)
    "PerformanceHistory",
    "PerformanceMonitor",
    "PerformanceRegressionAlert",
    "PerformanceSnapshot",
    "PerformanceTracker",
    "ResourceMonitor",
    "TimingStats",
    "attach_snapshot_to_result",
    # Enhanced resource profiling
    "EnhancedResourceProfiler",
    "ResourceSample",
    "ResourceTimeline",
    "ResourceType",
    "ResourceUtilization",
    "calculate_utilization",
    # Bottleneck detection
    "BottleneckAnalysis",
    "BottleneckDetector",
    "BottleneckIndicator",
    "BottleneckSeverity",
    "BottleneckType",
    "quick_bottleneck_check",
    # Reporting
    "ResourceChart",
    "ResourceReport",
    "ResourceReporter",
    "format_bytes",
    "format_duration",
    "generate_ascii_chart",
]
