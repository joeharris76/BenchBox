"""Tests for BenchBox MCP observability module.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import json
import logging
import time

import pytest

from benchbox.mcp.observability import (
    JSONFormatter,
    MetricsCollector,
    ToolCallContext,
    ToolCallMetrics,
    generate_correlation_id,
    get_correlation_id,
    get_metrics_collector,
    set_correlation_id,
    setup_structured_logging,
)


class TestCorrelationId:
    """Tests for correlation ID management."""

    def test_generate_correlation_id_format(self):
        """Test that correlation IDs have expected format."""
        cid = generate_correlation_id()
        assert cid.startswith("mcp_")
        assert len(cid) == 16  # "mcp_" + 12 hex chars

    def test_generate_unique_ids(self):
        """Test that generated IDs are unique."""
        ids = [generate_correlation_id() for _ in range(100)]
        assert len(set(ids)) == 100

    def test_get_set_correlation_id(self):
        """Test getting and setting correlation ID."""
        # Clear first
        set_correlation_id(None)
        assert get_correlation_id() is None

        # Set and get
        set_correlation_id("test_123")
        assert get_correlation_id() == "test_123"

        # Clean up
        set_correlation_id(None)


class TestJSONFormatter:
    """Tests for JSON log formatter."""

    def test_basic_format(self):
        """Test basic log formatting."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        data = json.loads(output)

        assert data["level"] == "INFO"
        assert data["logger"] == "test.logger"
        assert data["message"] == "Test message"
        assert "timestamp" in data

    def test_format_with_correlation_id(self):
        """Test formatting includes correlation ID."""
        set_correlation_id("test_cid_123")
        try:
            formatter = JSONFormatter()
            record = logging.LogRecord(
                name="test", level=logging.INFO, pathname="", lineno=0, msg="Test", args=(), exc_info=None
            )

            output = formatter.format(record)
            data = json.loads(output)

            assert data["correlation_id"] == "test_cid_123"
        finally:
            set_correlation_id(None)

    def test_format_with_tool_context(self):
        """Test formatting with tool context attributes."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg="Test", args=(), exc_info=None
        )
        record.tool_name = "list_platforms"
        record.tool_duration_ms = 42.5
        record.tool_status = "success"

        output = formatter.format(record)
        data = json.loads(output)

        assert data["tool_name"] == "list_platforms"
        assert data["tool_duration_ms"] == 42.5
        assert data["tool_status"] == "success"

    def test_format_with_exception(self):
        """Test formatting includes exception info."""
        formatter = JSONFormatter()
        try:
            raise ValueError("Test error")
        except ValueError:
            import sys

            record = logging.LogRecord(
                name="test",
                level=logging.ERROR,
                pathname="",
                lineno=0,
                msg="Error occurred",
                args=(),
                exc_info=sys.exc_info(),
            )

        output = formatter.format(record)
        data = json.loads(output)

        assert "exception" in data
        assert "ValueError" in data["exception"]


class TestSetupStructuredLogging:
    """Tests for setup_structured_logging function."""

    def test_creates_logger(self):
        """Test that logging setup creates a logger."""
        logger = setup_structured_logging("test.setup.logger", use_json=False)
        assert logger is not None
        assert logger.name == "test.setup.logger"

    def test_json_format(self):
        """Test that JSON format can be enabled."""
        logger = setup_structured_logging("test.json.logger", use_json=True)
        assert len(logger.handlers) == 1
        assert isinstance(logger.handlers[0].formatter, JSONFormatter)

    def test_standard_format(self):
        """Test that standard format can be used."""
        logger = setup_structured_logging("test.std.logger", use_json=False)
        assert len(logger.handlers) == 1
        assert not isinstance(logger.handlers[0].formatter, JSONFormatter)


class TestToolCallMetrics:
    """Tests for ToolCallMetrics dataclass."""

    def test_creation(self):
        """Test creating metrics."""
        metrics = ToolCallMetrics(tool_name="test_tool")
        assert metrics.tool_name == "test_tool"
        assert metrics.success is True
        assert metrics.end_time is None
        assert metrics.duration_ms is None

    def test_complete_success(self):
        """Test completing metrics successfully."""
        metrics = ToolCallMetrics(tool_name="test_tool")
        time.sleep(0.01)  # Small delay for measurable duration
        metrics.complete(success=True)

        assert metrics.success is True
        assert metrics.error_code is None
        assert metrics.duration_ms is not None
        assert metrics.duration_ms > 0

    def test_complete_error(self):
        """Test completing metrics with error."""
        metrics = ToolCallMetrics(tool_name="test_tool")
        metrics.complete(success=False, error_code="VALIDATION_ERROR")

        assert metrics.success is False
        assert metrics.error_code == "VALIDATION_ERROR"


class TestMetricsCollector:
    """Tests for MetricsCollector."""

    def test_record_successful_call(self):
        """Test recording a successful call."""
        collector = MetricsCollector()
        metrics = ToolCallMetrics(tool_name="test_tool")
        time.sleep(0.001)
        metrics.complete(success=True)

        collector.record_call(metrics)

        stats = collector.get_stats("test_tool")
        assert stats["calls"] == 1
        assert stats["success"] == 1
        assert stats["errors"] == 0

    def test_record_failed_call(self):
        """Test recording a failed call."""
        collector = MetricsCollector()
        metrics = ToolCallMetrics(tool_name="test_tool")
        metrics.complete(success=False, error_code="ERROR")

        collector.record_call(metrics)

        stats = collector.get_stats("test_tool")
        assert stats["calls"] == 1
        assert stats["success"] == 0
        assert stats["errors"] == 1

    def test_multiple_tools(self):
        """Test recording calls for multiple tools."""
        collector = MetricsCollector()

        # Tool 1: 2 calls
        for _ in range(2):
            m = ToolCallMetrics(tool_name="tool1")
            m.complete()
            collector.record_call(m)

        # Tool 2: 3 calls
        for _ in range(3):
            m = ToolCallMetrics(tool_name="tool2")
            m.complete()
            collector.record_call(m)

        all_stats = collector.get_stats()
        assert all_stats["summary"]["total_calls"] == 5
        assert all_stats["tools"]["tool1"]["calls"] == 2
        assert all_stats["tools"]["tool2"]["calls"] == 3

    def test_duration_percentiles(self):
        """Test duration percentile calculation."""
        collector = MetricsCollector()

        # Record 100 calls with increasing durations (1-100ms)
        for i in range(100):
            m = ToolCallMetrics(tool_name="test_tool")
            # Set start_time and end_time directly (don't call complete() which overwrites)
            m.start_time = 0
            m.end_time = (i + 1) / 1000  # 1-100ms in seconds -> duration_ms = 1-100
            m.success = True
            collector.record_call(m)

        stats = collector.get_stats("test_tool")
        assert "duration_ms" in stats
        assert stats["duration_ms"]["min"] == 1.0
        assert stats["duration_ms"]["max"] == 100.0
        assert stats["duration_ms"]["samples"] == 100

    def test_reset(self):
        """Test resetting metrics."""
        collector = MetricsCollector()
        m = ToolCallMetrics(tool_name="test")
        m.complete()
        collector.record_call(m)

        collector.reset()

        stats = collector.get_stats()
        assert stats["summary"]["total_calls"] == 0


class TestToolCallContext:
    """Tests for ToolCallContext context manager."""

    def test_basic_usage(self):
        """Test basic context manager usage."""
        collector = get_metrics_collector()
        collector.reset()

        with ToolCallContext("test_tool") as ctx:
            assert ctx.tool_name == "test_tool"
            assert ctx.correlation_id is not None
            assert get_correlation_id() == ctx.correlation_id

        # After exit, correlation should be cleared
        assert get_correlation_id() is None

        # Metrics should be recorded
        stats = collector.get_stats("test_tool")
        assert stats["calls"] >= 1

    def test_marks_error(self):
        """Test marking error in context."""
        collector = get_metrics_collector()
        collector.reset()

        with ToolCallContext("error_tool") as ctx:
            ctx.mark_error("VALIDATION_ERROR")

        stats = collector.get_stats("error_tool")
        assert stats["errors"] >= 1

    def test_exception_handling(self):
        """Test that exceptions are tracked."""
        collector = get_metrics_collector()
        collector.reset()

        with pytest.raises(ValueError):
            with ToolCallContext("exception_tool"):
                raise ValueError("Test error")

        stats = collector.get_stats("exception_tool")
        assert stats["errors"] >= 1

    def test_custom_correlation_id(self):
        """Test using custom correlation ID."""
        with ToolCallContext("test_tool", correlation_id="custom_123") as ctx:
            assert ctx.correlation_id == "custom_123"
            assert get_correlation_id() == "custom_123"


class TestGlobalMetricsCollector:
    """Tests for global metrics collector."""

    def test_get_metrics_collector_singleton(self):
        """Test that get_metrics_collector returns same instance."""
        collector1 = get_metrics_collector()
        collector2 = get_metrics_collector()
        assert collector1 is collector2
