"""Resource utilization reporting and chart generation.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .bottleneck import BottleneckAnalysis, BottleneckDetector
from .profiler import ResourceTimeline, ResourceType, ResourceUtilization, calculate_utilization


@dataclass
class ResourceChart:
    """ASCII chart representation of resource utilization over time."""

    resource_type: ResourceType
    width: int = 60
    height: int = 10
    title: str = ""
    chart_lines: list[str] = field(default_factory=list)
    min_value: float = 0.0
    max_value: float = 0.0

    def render(self) -> str:
        """Render chart as string."""
        lines = []
        if self.title:
            lines.append(self.title)
            lines.append("=" * len(self.title))
        lines.extend(self.chart_lines)
        return "\n".join(lines)

    def __str__(self) -> str:
        return self.render()


def generate_ascii_chart(
    series: list[float],
    width: int = 60,
    height: int = 10,
    title: str = "",
    unit: str = "",
) -> ResourceChart:
    """Generate an ASCII chart from a time series.

    Args:
        series: List of values to chart.
        width: Chart width in characters.
        height: Chart height in rows.
        title: Optional chart title.
        unit: Unit label for values.

    Returns:
        ResourceChart with rendered ASCII chart.
    """
    if not series:
        chart = ResourceChart(
            resource_type=ResourceType.CPU,
            width=width,
            height=height,
            title=title,
            chart_lines=["(no data)"],
        )
        return chart

    min_val = min(series)
    max_val = max(series)

    # Normalize to chart height
    if max_val == min_val:
        normalized = [height // 2] * len(series)
    else:
        normalized = [int((v - min_val) / (max_val - min_val) * (height - 1)) for v in series]

    # Resample to chart width if needed
    if len(normalized) > width:
        step = len(normalized) / width
        resampled = []
        for i in range(width):
            idx = int(i * step)
            resampled.append(normalized[idx])
        normalized = resampled
    elif len(normalized) < width:
        # Stretch to fill width
        resampled = []
        for i in range(width):
            idx = int(i * len(normalized) / width)
            resampled.append(normalized[idx])
        normalized = resampled

    # Build chart lines (top to bottom)
    chart_lines = []

    # Y-axis label width
    label_width = 8

    for row in range(height - 1, -1, -1):
        # Y-axis label
        if row == height - 1:
            label = f"{max_val:>{label_width - 1}.1f}"
        elif row == 0:
            label = f"{min_val:>{label_width - 1}.1f}"
        elif row == height // 2:
            mid = (max_val + min_val) / 2
            label = f"{mid:>{label_width - 1}.1f}"
        else:
            label = " " * (label_width - 1)

        line_chars = []
        for col in range(len(normalized)):
            if normalized[col] >= row:
                line_chars.append("*")
            else:
                line_chars.append(" ")

        chart_lines.append(f"{label}|{''.join(line_chars)}")

    # X-axis
    chart_lines.append(" " * (label_width - 1) + "+" + "-" * len(normalized))
    chart_lines.append(" " * label_width + f"0{' ' * (len(normalized) - 10)}time ->{' ' * 5}")

    # Add unit if provided
    if unit:
        chart_lines.insert(0, f"  [{unit}]")

    chart = ResourceChart(
        resource_type=ResourceType.CPU,
        width=width,
        height=height,
        title=title,
        chart_lines=chart_lines,
        min_value=min_val,
        max_value=max_val,
    )
    return chart


@dataclass
class ResourceReport:
    """Comprehensive resource utilization report."""

    timeline: ResourceTimeline
    analysis: BottleneckAnalysis
    utilizations: dict[ResourceType, ResourceUtilization] = field(default_factory=dict)
    charts: dict[ResourceType, ResourceChart] = field(default_factory=dict)

    def generate_text_report(self, include_charts: bool = True) -> str:
        """Generate full text report.

        Args:
            include_charts: Whether to include ASCII charts.

        Returns:
            Formatted text report.
        """
        lines = []
        lines.append("=" * 70)
        lines.append("RESOURCE UTILIZATION REPORT")
        lines.append("=" * 70)
        lines.append("")

        # Summary section
        lines.append("SUMMARY")
        lines.append("-" * 70)
        lines.append(f"Duration: {self.timeline.duration_seconds:.1f} seconds")
        lines.append(f"Samples collected: {self.timeline.sample_count}")
        lines.append(f"Primary bottleneck: {self.analysis.primary_bottleneck.value}")
        lines.append(f"Severity: {self.analysis.primary_severity.value}")
        lines.append("")
        lines.append(self.analysis.summary)
        lines.append("")

        # Utilization table
        lines.append("RESOURCE UTILIZATION")
        lines.append("-" * 70)
        lines.append(f"{'Resource':<20} {'Min':>10} {'Avg':>10} {'Max':>10} {'P95':>10} {'Unit':>8}")
        lines.append("-" * 70)

        for resource_type in ResourceType:
            util = self.utilizations.get(resource_type)
            if util and util.sample_count > 0:
                lines.append(
                    f"{resource_type.value:<20} "
                    f"{util.min_value:>10.1f} "
                    f"{util.avg_value:>10.1f} "
                    f"{util.max_value:>10.1f} "
                    f"{util.p95_value:>10.1f} "
                    f"{util.unit:>8}"
                )
        lines.append("")

        # Bottleneck indicators
        lines.append("BOTTLENECK ANALYSIS")
        lines.append("-" * 70)

        for indicator in self.analysis.indicators:
            if indicator.score > 0.1:
                lines.append(
                    f"{indicator.bottleneck_type.value}: "
                    f"score={indicator.score:.2f}, "
                    f"severity={indicator.severity.value}"
                )
                for evidence in indicator.evidence:
                    lines.append(f"  - {evidence}")
                for rec in indicator.recommendations:
                    lines.append(f"  > {rec}")
                lines.append("")

        # Charts
        if include_charts:
            lines.append("RESOURCE CHARTS")
            lines.append("-" * 70)

            for resource_type, chart in self.charts.items():
                lines.append("")
                lines.append(chart.render())
                lines.append("")

        lines.append("=" * 70)
        return "\n".join(lines)

    def generate_json(self) -> dict[str, Any]:
        """Generate JSON-serializable report."""
        return {
            "summary": {
                "duration_seconds": self.timeline.duration_seconds,
                "sample_count": self.timeline.sample_count,
            },
            "timeline": self.timeline.to_dict(),
            "analysis": self.analysis.to_dict(),
            "utilizations": {k.value: v.to_dict() for k, v in self.utilizations.items()},
        }


class ResourceReporter:
    """Generate resource utilization reports from timeline data.

    Combines resource profiling data with bottleneck analysis to produce
    comprehensive reports with charts and recommendations.

    Example:
        >>> reporter = ResourceReporter()
        >>> report = reporter.generate_report(timeline)
        >>> print(report.generate_text_report())
    """

    def __init__(
        self,
        chart_width: int = 60,
        chart_height: int = 10,
        detector: BottleneckDetector | None = None,
    ):
        """Initialize resource reporter.

        Args:
            chart_width: Width of ASCII charts.
            chart_height: Height of ASCII charts.
            detector: Optional custom bottleneck detector.
        """
        self.chart_width = chart_width
        self.chart_height = chart_height
        self.detector = detector or BottleneckDetector()

    def generate_report(
        self,
        timeline: ResourceTimeline,
        include_charts: bool = True,
    ) -> ResourceReport:
        """Generate comprehensive resource report.

        Args:
            timeline: Resource timeline to report on.
            include_charts: Whether to generate ASCII charts.

        Returns:
            ResourceReport with analysis and visualizations.
        """
        # Run bottleneck analysis
        analysis = self.detector.analyze(timeline)

        # Calculate utilizations
        utilizations = {}
        for resource_type in ResourceType:
            utilizations[resource_type] = calculate_utilization(timeline, resource_type)

        # Generate charts
        charts = {}
        if include_charts:
            chart_configs = [
                (ResourceType.CPU, "CPU Utilization", "%"),
                (ResourceType.MEMORY, "Memory Usage", "MB"),
                (ResourceType.DISK_READ, "Disk Read IOPS", "IOPS"),
                (ResourceType.DISK_WRITE, "Disk Write IOPS", "IOPS"),
                (ResourceType.NETWORK_SEND, "Network Send", "Mbps"),
                (ResourceType.NETWORK_RECV, "Network Receive", "Mbps"),
            ]

            for resource_type, title, unit in chart_configs:
                series = timeline.get_resource_series(resource_type)
                if series and any(v > 0 for v in series):
                    chart = generate_ascii_chart(
                        series,
                        width=self.chart_width,
                        height=self.chart_height,
                        title=title,
                        unit=unit,
                    )
                    chart.resource_type = resource_type
                    charts[resource_type] = chart

        return ResourceReport(
            timeline=timeline,
            analysis=analysis,
            utilizations=utilizations,
            charts=charts,
        )

    def generate_summary_line(self, timeline: ResourceTimeline) -> str:
        """Generate a single-line summary of resource usage.

        Args:
            timeline: Resource timeline to summarize.

        Returns:
            Single-line summary string.
        """
        if timeline.sample_count == 0:
            return "No resource data collected"

        parts = []
        parts.append(f"CPU: {timeline.get_avg_cpu():.0f}% avg/{timeline.get_peak_cpu():.0f}% peak")
        parts.append(f"Mem: {timeline.get_avg_memory_mb():.0f}MB avg/{timeline.get_peak_memory_mb():.0f}MB peak")

        disk_read = timeline.get_avg_disk_read_iops()
        disk_write = timeline.get_avg_disk_write_iops()
        if disk_read > 0 or disk_write > 0:
            parts.append(f"Disk: {disk_read:.0f}r/{disk_write:.0f}w IOPS")

        net_send = timeline.get_avg_network_send_mbps()
        net_recv = timeline.get_avg_network_recv_mbps()
        if net_send > 0 or net_recv > 0:
            parts.append(f"Net: {net_send:.1f}tx/{net_recv:.1f}rx Mbps")

        return " | ".join(parts)


def format_bytes(num_bytes: int) -> str:
    """Format byte count to human-readable string.

    Args:
        num_bytes: Number of bytes.

    Returns:
        Formatted string like "1.5 GB" or "256 MB".
    """
    if num_bytes < 1024:
        return f"{num_bytes} B"
    elif num_bytes < 1024 * 1024:
        return f"{num_bytes / 1024:.1f} KB"
    elif num_bytes < 1024 * 1024 * 1024:
        return f"{num_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{num_bytes / (1024 * 1024 * 1024):.1f} GB"


def format_duration(seconds: float) -> str:
    """Format duration to human-readable string.

    Args:
        seconds: Duration in seconds.

    Returns:
        Formatted string like "2h 30m" or "45.2s".
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"
