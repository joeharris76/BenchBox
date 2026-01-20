"""Tests for BenchBox MCP async execution module.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from benchbox.mcp.execution import (
    ExecutionState,
    ExecutionStatus,
    ExecutionTracker,
    get_execution_tracker,
)


class TestExecutionStatus:
    """Tests for ExecutionStatus enum."""

    def test_status_values(self):
        """Test that all status values are defined."""
        assert ExecutionStatus.QUEUED.value == "queued"
        assert ExecutionStatus.RUNNING.value == "running"
        assert ExecutionStatus.COMPLETED.value == "completed"
        assert ExecutionStatus.FAILED.value == "failed"
        assert ExecutionStatus.CANCELLED.value == "cancelled"


class TestExecutionState:
    """Tests for ExecutionState dataclass."""

    def test_creation(self):
        """Test creating an execution state."""
        state = ExecutionState(
            execution_id="test_123",
            platform="duckdb",
            benchmark="tpch",
            scale_factor=0.01,
        )
        assert state.execution_id == "test_123"
        assert state.platform == "duckdb"
        assert state.benchmark == "tpch"
        assert state.scale_factor == 0.01
        assert state.status == ExecutionStatus.QUEUED
        assert state.progress_percent == 0.0
        assert state.result is None
        assert state.error is None

    def test_to_dict(self):
        """Test converting state to dictionary."""
        state = ExecutionState(
            execution_id="test_123",
            platform="duckdb",
            benchmark="tpch",
            scale_factor=0.01,
        )
        d = state.to_dict()

        assert d["execution_id"] == "test_123"
        assert d["platform"] == "duckdb"
        assert d["benchmark"] == "tpch"
        assert d["scale_factor"] == 0.01
        assert d["status"] == "queued"
        assert d["progress_percent"] == 0.0
        assert d["created_at"] is not None

    def test_to_dict_with_timestamps(self):
        """Test dictionary includes timestamp fields."""
        state = ExecutionState(
            execution_id="test_123",
            platform="duckdb",
            benchmark="tpch",
            scale_factor=0.01,
        )
        d = state.to_dict()

        assert "created_at" in d
        assert "started_at" in d
        assert "completed_at" in d


class TestExecutionTracker:
    """Tests for ExecutionTracker."""

    def test_create_execution(self):
        """Test creating a new execution."""
        tracker = ExecutionTracker()
        state = tracker.create_execution(
            platform="duckdb",
            benchmark="tpch",
            scale_factor=0.01,
        )

        assert state.execution_id.startswith("mcp_")
        assert state.platform == "duckdb"
        assert state.benchmark == "tpch"
        assert state.status == ExecutionStatus.QUEUED

    def test_get_execution(self):
        """Test retrieving execution by ID."""
        tracker = ExecutionTracker()
        state = tracker.create_execution("duckdb", "tpch", 0.01)

        retrieved = tracker.get_execution(state.execution_id)
        assert retrieved is not None
        assert retrieved.execution_id == state.execution_id

    def test_get_nonexistent_execution(self):
        """Test that getting nonexistent execution returns None."""
        tracker = ExecutionTracker()
        assert tracker.get_execution("nonexistent") is None

    def test_update_status_to_running(self):
        """Test updating status to running."""
        tracker = ExecutionTracker()
        state = tracker.create_execution("duckdb", "tpch", 0.01)

        result = tracker.update_status(
            state.execution_id,
            ExecutionStatus.RUNNING,
            current_phase="loading",
            progress_percent=20.0,
        )

        assert result is True
        assert state.status == ExecutionStatus.RUNNING
        assert state.current_phase == "loading"
        assert state.progress_percent == 20.0
        assert state.started_at is not None

    def test_update_status_to_completed(self):
        """Test updating status to completed."""
        tracker = ExecutionTracker()
        state = tracker.create_execution("duckdb", "tpch", 0.01)

        tracker.update_status(state.execution_id, ExecutionStatus.RUNNING)
        tracker.update_status(
            state.execution_id,
            ExecutionStatus.COMPLETED,
            progress_percent=100.0,
        )

        assert state.status == ExecutionStatus.COMPLETED
        assert state.completed_at is not None

    def test_update_status_to_failed(self):
        """Test updating status to failed with error."""
        tracker = ExecutionTracker()
        state = tracker.create_execution("duckdb", "tpch", 0.01)

        tracker.update_status(
            state.execution_id,
            ExecutionStatus.FAILED,
            error="Something went wrong",
            error_code="INTERNAL_ERROR",
        )

        assert state.status == ExecutionStatus.FAILED
        assert state.error == "Something went wrong"
        assert state.error_code == "INTERNAL_ERROR"

    def test_update_nonexistent_execution(self):
        """Test that updating nonexistent execution returns False."""
        tracker = ExecutionTracker()
        result = tracker.update_status("nonexistent", ExecutionStatus.RUNNING)
        assert result is False

    def test_set_result(self):
        """Test setting result for completed execution."""
        tracker = ExecutionTracker()
        state = tracker.create_execution("duckdb", "tpch", 0.01)

        result_data = {"total_time_ms": 1000, "queries_passed": 22}
        tracker.set_result(state.execution_id, result_data)

        assert state.result == result_data

    def test_list_executions(self):
        """Test listing executions."""
        tracker = ExecutionTracker()

        # Create multiple executions
        tracker.create_execution("duckdb", "tpch", 0.01)
        tracker.create_execution("polars", "tpch", 0.01)
        tracker.create_execution("duckdb", "tpcds", 1.0)

        executions = tracker.list_executions()
        assert len(executions) == 3

    def test_list_executions_with_filter(self):
        """Test listing executions with status filter."""
        tracker = ExecutionTracker()

        state1 = tracker.create_execution("duckdb", "tpch", 0.01)
        state2 = tracker.create_execution("polars", "tpch", 0.01)

        tracker.update_status(state1.execution_id, ExecutionStatus.RUNNING)
        tracker.update_status(state2.execution_id, ExecutionStatus.COMPLETED)

        running = tracker.list_executions(status_filter=ExecutionStatus.RUNNING)
        assert len(running) == 1
        assert running[0].execution_id == state1.execution_id

    def test_list_executions_with_limit(self):
        """Test listing executions with limit."""
        tracker = ExecutionTracker()

        for i in range(5):
            tracker.create_execution("duckdb", f"test_{i}", 0.01)

        executions = tracker.list_executions(limit=3)
        assert len(executions) == 3

    def test_cancel_execution(self):
        """Test cancelling a queued execution."""
        tracker = ExecutionTracker()
        state = tracker.create_execution("duckdb", "tpch", 0.01)

        result = tracker.cancel_execution(state.execution_id)
        assert result is True
        assert state.status == ExecutionStatus.CANCELLED
        assert state.completed_at is not None

    def test_cancel_running_execution(self):
        """Test cancelling a running execution."""
        tracker = ExecutionTracker()
        state = tracker.create_execution("duckdb", "tpch", 0.01)
        tracker.update_status(state.execution_id, ExecutionStatus.RUNNING)

        result = tracker.cancel_execution(state.execution_id)
        assert result is True
        assert state.status == ExecutionStatus.CANCELLED

    def test_cancel_completed_execution_fails(self):
        """Test that cancelling completed execution fails."""
        tracker = ExecutionTracker()
        state = tracker.create_execution("duckdb", "tpch", 0.01)
        tracker.update_status(state.execution_id, ExecutionStatus.COMPLETED)

        result = tracker.cancel_execution(state.execution_id)
        assert result is False
        assert state.status == ExecutionStatus.COMPLETED

    def test_cancel_nonexistent_execution(self):
        """Test that cancelling nonexistent execution returns False."""
        tracker = ExecutionTracker()
        result = tracker.cancel_execution("nonexistent")
        assert result is False

    def test_cleanup_old_completed(self):
        """Test that old completed executions are cleaned up."""
        tracker = ExecutionTracker(max_completed=3)

        # Create and complete 5 executions
        for i in range(5):
            state = tracker.create_execution("duckdb", f"test_{i}", 0.01)
            tracker.update_status(state.execution_id, ExecutionStatus.COMPLETED)

        # Create one more to trigger cleanup
        tracker.create_execution("duckdb", "test_new", 0.01)

        # Should have at most max_completed (3) completed + 1 queued = 4
        executions = tracker.list_executions(limit=100)
        completed = [e for e in executions if e.status == ExecutionStatus.COMPLETED]
        assert len(completed) <= 3


class TestGlobalTracker:
    """Tests for global execution tracker."""

    def test_get_execution_tracker_singleton(self):
        """Test that get_execution_tracker returns same instance."""
        tracker1 = get_execution_tracker()
        tracker2 = get_execution_tracker()
        assert tracker1 is tracker2
