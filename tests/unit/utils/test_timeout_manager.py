"""Tests for the timeout_manager module.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import time
from unittest.mock import MagicMock

import pytest

from benchbox.utils.timeout_manager import (
    TimeoutConfig,
    TimeoutError,
    TimeoutManager,
    _TimeoutContext,
    get_timeout_manager,
    run_with_timeout,
    timeout,
)

pytestmark = pytest.mark.fast


class TestTimeoutConfig:
    """Tests for TimeoutConfig dataclass."""

    def test_valid_timeout_config(self):
        """Test creating a valid timeout config."""
        config = TimeoutConfig(timeout_seconds=30.0, operation_name="test_op")
        assert config.timeout_seconds == 30.0
        assert config.operation_name == "test_op"
        assert config.raise_on_timeout is True
        assert config.on_timeout is None

    def test_timeout_config_with_callback(self):
        """Test timeout config with callback function."""
        callback = MagicMock()
        config = TimeoutConfig(
            timeout_seconds=60.0,
            operation_name="callback_test",
            on_timeout=callback,
        )
        assert config.on_timeout is callback

    def test_invalid_timeout_seconds(self):
        """Test that non-positive timeout raises error."""
        with pytest.raises(ValueError, match="timeout_seconds must be positive"):
            TimeoutConfig(timeout_seconds=0)

        with pytest.raises(ValueError, match="timeout_seconds must be positive"):
            TimeoutConfig(timeout_seconds=-1.0)


class TestTimeoutContext:
    """Tests for internal _TimeoutContext class."""

    def test_context_initialization(self):
        """Test timeout context initial state."""
        ctx = _TimeoutContext(timeout_seconds=30.0, operation_name="test")
        assert ctx.timeout_seconds == 30.0
        assert ctx.operation_name == "test"
        assert ctx.timed_out is False
        assert ctx.triggered_at is None

    def test_trigger_timeout(self):
        """Test triggering timeout."""
        ctx = _TimeoutContext(timeout_seconds=30.0, operation_name="test")
        assert ctx.timed_out is False

        ctx._trigger_timeout()

        assert ctx.timed_out is True
        assert ctx.triggered_at is not None
        assert ctx.triggered_at > 0


class TestTimeoutManager:
    """Tests for TimeoutManager class."""

    def test_manager_initialization(self):
        """Test manager initialization with default timeout."""
        manager = TimeoutManager(default_timeout_seconds=60.0)
        assert manager.default_timeout_seconds == 60.0

    def test_timeout_context_no_timeout(self):
        """Test context manager when operation completes before timeout."""
        manager = TimeoutManager(default_timeout_seconds=300.0)

        with manager.timeout_context(timeout_seconds=5.0, operation_name="fast_op") as ctx:
            time.sleep(0.01)  # Very short sleep

        assert ctx.timed_out is False

    def test_timeout_context_with_timeout_no_raise(self):
        """Test context manager when operation times out (no raise)."""
        manager = TimeoutManager(default_timeout_seconds=300.0)

        with manager.timeout_context(
            timeout_seconds=0.1,
            operation_name="slow_op",
            raise_on_timeout=False,
        ) as ctx:
            time.sleep(0.3)  # Sleep longer than timeout

        assert ctx.timed_out is True

    def test_timeout_context_with_timeout_raises(self):
        """Test context manager when operation times out (raises)."""
        manager = TimeoutManager(default_timeout_seconds=300.0)

        with (
            pytest.raises(TimeoutError) as exc_info,
            manager.timeout_context(
                timeout_seconds=0.1,
                operation_name="slow_op",
                raise_on_timeout=True,
            ),
        ):
            time.sleep(0.3)

        assert "slow_op" in str(exc_info.value)
        assert exc_info.value.timeout_seconds == 0.1
        assert exc_info.value.operation == "slow_op"

    def test_timeout_context_uses_default_timeout(self):
        """Test that context uses default timeout when not specified."""
        manager = TimeoutManager(default_timeout_seconds=60.0)

        with manager.timeout_context(operation_name="default_timeout_test") as ctx:
            pass

        assert ctx.timeout_seconds == 60.0

    def test_cancel_all_timers(self):
        """Test canceling all active timers."""
        manager = TimeoutManager(default_timeout_seconds=300.0)

        # Start a long timeout
        ctx = manager.timeout_context(timeout_seconds=300.0, operation_name="long_op")
        ctx.__enter__()

        # Should have at least one active timer
        assert len(manager._active_timers) > 0

        # Cancel all timers
        manager.cancel_all_timers()

        # Timers should be cleared
        assert len(manager._active_timers) == 0

        # Clean up
        ctx.__exit__(None, None, None)


class TestRunWithTimeout:
    """Tests for run_with_timeout function."""

    def test_function_completes_before_timeout(self):
        """Test function that completes before timeout."""

        def fast_func():
            return 42

        result, timed_out = run_with_timeout(fast_func, 5.0, "fast_func")
        assert result == 42
        assert timed_out is False

    def test_function_times_out(self):
        """Test function that exceeds timeout."""

        def slow_func():
            time.sleep(5.0)
            return 42

        result, timed_out = run_with_timeout(slow_func, 0.1, "slow_func")
        assert result is None
        assert timed_out is True

    def test_function_with_args(self):
        """Test function with positional arguments."""

        def add(a, b):
            return a + b

        result, timed_out = run_with_timeout(add, 5.0, "add", 3, 4)
        assert result == 7
        assert timed_out is False

    def test_function_with_kwargs(self):
        """Test function with keyword arguments."""

        def multiply(a, b=2):
            return a * b

        result, timed_out = run_with_timeout(multiply, 5.0, "multiply", 5, b=3)
        assert result == 15
        assert timed_out is False

    def test_function_raises_exception(self):
        """Test function that raises an exception."""

        def error_func():
            raise ValueError("test error")

        with pytest.raises(ValueError, match="test error"):
            run_with_timeout(error_func, 5.0, "error_func")


class TestTimeoutContextManager:
    """Tests for the timeout context manager convenience function."""

    def test_timeout_context_manager_no_timeout(self):
        """Test convenience context manager when operation completes."""
        with timeout(5.0, "fast_op") as ctx:
            time.sleep(0.01)

        assert ctx.timed_out is False

    def test_timeout_context_manager_with_timeout_no_raise(self):
        """Test convenience context manager timeout without raising."""
        with timeout(0.1, "slow_op", raise_on_timeout=False) as ctx:
            time.sleep(0.3)

        assert ctx.timed_out is True

    def test_timeout_context_manager_with_timeout_raises(self):
        """Test convenience context manager timeout with raising."""
        with pytest.raises(TimeoutError), timeout(0.1, "slow_op", raise_on_timeout=True):
            time.sleep(0.3)


class TestGetTimeoutManager:
    """Tests for the global timeout manager singleton."""

    def test_get_timeout_manager_returns_instance(self):
        """Test that get_timeout_manager returns a TimeoutManager instance."""
        manager = get_timeout_manager()
        assert isinstance(manager, TimeoutManager)

    def test_get_timeout_manager_returns_same_instance(self):
        """Test that get_timeout_manager returns the same singleton instance."""
        manager1 = get_timeout_manager()
        manager2 = get_timeout_manager()
        assert manager1 is manager2


class TestTimeoutError:
    """Tests for TimeoutError exception."""

    def test_timeout_error_attributes(self):
        """Test TimeoutError stores all attributes."""
        error = TimeoutError(
            message="Operation timed out",
            timeout_seconds=30.0,
            operation="test_op",
        )
        assert str(error) == "Operation timed out"
        assert error.timeout_seconds == 30.0
        assert error.operation == "test_op"

    def test_timeout_error_default_operation(self):
        """Test TimeoutError with default operation."""
        error = TimeoutError(message="Timed out", timeout_seconds=60.0)
        assert error.operation == ""
