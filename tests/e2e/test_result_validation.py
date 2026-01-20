"""End-to-end tests for result validation.

Tests validate that benchmark result files have the correct structure and content.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.e2e.utils import (
    ResultValidator,
    assert_benchmark_result_valid,
    assert_phase_timing_valid,
    assert_query_execution_valid,
    load_result_json,
    validate_execution_phases,
    validate_result_structure,
)

# ============================================================================
# Result Structure Validation Tests
# ============================================================================


class TestResultStructure:
    """Tests for result structure validation."""

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_valid_result_passes_validation(self) -> None:
        """Test that a valid result passes validation."""
        valid_result = {
            "benchmark_name": "TPC-H",
            "platform": "duckdb",
            "scale_factor": 0.01,
            "execution_id": "test-execution-123",
            "timestamp": "2024-01-15T10:30:00Z",
            "duration_seconds": 45.5,
            "total_queries": 22,
            "successful_queries": 22,
            "failed_queries": 0,
            "query_results": [
                {"query_id": "Q1", "status": "SUCCESS", "execution_time_ms": 100},
                {"query_id": "Q2", "status": "SUCCESS", "execution_time_ms": 150},
            ],
        }

        is_valid, errors = validate_result_structure(valid_result)
        assert is_valid, f"Validation failed with errors: {errors}"
        assert len(errors) == 0

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_missing_required_field_fails(self) -> None:
        """Test that missing required fields fail validation."""
        invalid_result = {
            "benchmark_name": "TPC-H",
            "platform": "duckdb",
            # Missing: scale_factor, execution_id, timestamp, duration_seconds, etc.
        }

        is_valid, errors = validate_result_structure(invalid_result)
        assert not is_valid
        assert len(errors) > 0

        # Check that missing fields are reported
        missing_fields = {e.field for e in errors}
        assert "scale_factor" in missing_fields or any("scale_factor" in e.message for e in errors)

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_null_required_field_fails(self) -> None:
        """Test that null required fields fail validation."""
        invalid_result = {
            "benchmark_name": "TPC-H",
            "platform": "duckdb",
            "scale_factor": None,  # Null instead of value
            "execution_id": "test-123",
            "timestamp": "2024-01-15T10:30:00Z",
            "duration_seconds": 45.5,
            "total_queries": 22,
            "successful_queries": 22,
            "failed_queries": 0,
        }

        is_valid, errors = validate_result_structure(invalid_result)
        assert not is_valid
        assert any("scale_factor" in e.field for e in errors)

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_invalid_timestamp_format_fails(self) -> None:
        """Test that invalid timestamp format fails validation."""
        invalid_result = {
            "benchmark_name": "TPC-H",
            "platform": "duckdb",
            "scale_factor": 0.01,
            "execution_id": "test-123",
            "timestamp": "invalid-timestamp-format",
            "duration_seconds": 45.5,
            "total_queries": 22,
            "successful_queries": 22,
            "failed_queries": 0,
        }

        is_valid, errors = validate_result_structure(invalid_result)
        assert not is_valid
        assert any("timestamp" in e.field for e in errors)

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_negative_duration_fails(self) -> None:
        """Test that negative duration fails validation."""
        invalid_result = {
            "benchmark_name": "TPC-H",
            "platform": "duckdb",
            "scale_factor": 0.01,
            "execution_id": "test-123",
            "timestamp": "2024-01-15T10:30:00Z",
            "duration_seconds": -10,  # Negative duration
            "total_queries": 22,
            "successful_queries": 22,
            "failed_queries": 0,
        }

        is_valid, errors = validate_result_structure(invalid_result)
        assert not is_valid
        assert any("duration" in e.field for e in errors)

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_query_count_mismatch_fails(self) -> None:
        """Test that mismatched query counts fail validation."""
        invalid_result = {
            "benchmark_name": "TPC-H",
            "platform": "duckdb",
            "scale_factor": 0.01,
            "execution_id": "test-123",
            "timestamp": "2024-01-15T10:30:00Z",
            "duration_seconds": 45.5,
            "total_queries": 22,
            "successful_queries": 20,
            "failed_queries": 1,  # 20 + 1 != 22
            "query_results": [],
        }

        is_valid, errors = validate_result_structure(invalid_result)
        assert not is_valid
        assert any("count" in e.field or "count" in e.message.lower() for e in errors)


# ============================================================================
# Query Execution Validation Tests
# ============================================================================


class TestQueryExecutionValidation:
    """Tests for query execution validation."""

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_valid_query_execution_passes(self) -> None:
        """Test that valid query execution passes validation."""
        query_exec = {
            "query_id": "Q1",
            "status": "SUCCESS",
            "execution_time_ms": 150,
            "rows_returned": 100,
        }

        # Should not raise
        assert_query_execution_valid(query_exec)

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_missing_query_id_fails(self) -> None:
        """Test that missing query_id fails validation."""
        query_exec = {
            "status": "SUCCESS",
            "execution_time_ms": 150,
        }

        with pytest.raises(AssertionError, match="query_id"):
            assert_query_execution_valid(query_exec)

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_empty_query_id_fails(self) -> None:
        """Test that empty query_id fails validation."""
        query_exec = {
            "query_id": "",
            "status": "SUCCESS",
            "execution_time_ms": 150,
        }

        with pytest.raises(AssertionError, match="non-empty"):
            assert_query_execution_valid(query_exec)

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_invalid_status_fails(self) -> None:
        """Test that invalid status fails validation."""
        query_exec = {
            "query_id": "Q1",
            "status": "INVALID_STATUS",
            "execution_time_ms": 150,
        }

        with pytest.raises(AssertionError, match="status"):
            assert_query_execution_valid(query_exec)

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_negative_execution_time_fails(self) -> None:
        """Test that negative execution time fails validation."""
        query_exec = {
            "query_id": "Q1",
            "status": "SUCCESS",
            "execution_time_ms": -100,  # Negative
        }

        with pytest.raises(AssertionError):
            assert_query_execution_valid(query_exec)

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_failed_query_no_time_required(self) -> None:
        """Test that failed queries don't require execution time."""
        query_exec = {
            "query_id": "Q1",
            "status": "FAILED",
            "error_message": "Query failed",
        }

        # Should not raise - execution_time_ms not required for failed queries
        assert_query_execution_valid(query_exec, require_positive_time=False)

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    @pytest.mark.parametrize(
        "status",
        ["SUCCESS", "FAILED", "ERROR", "TIMEOUT", "SKIPPED"],
    )
    def test_valid_statuses_accepted(self, status: str) -> None:
        """Test that all valid statuses are accepted."""
        query_exec = {
            "query_id": "Q1",
            "status": status,
            "execution_time_ms": 100 if status == "SUCCESS" else 0,
        }

        # Should not raise for any valid status
        assert_query_execution_valid(query_exec, require_positive_time=False)


# ============================================================================
# Phase Timing Validation Tests
# ============================================================================


class TestPhaseTimingValidation:
    """Tests for phase timing validation."""

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_valid_phase_timing_passes(self) -> None:
        """Test that valid phase timing passes validation."""
        phase = {
            "duration_ms": 5000,
            "status": "completed",
        }

        # Should not raise
        assert_phase_timing_valid(phase, "test_phase")

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_missing_duration_fails(self) -> None:
        """Test that missing duration_ms fails validation."""
        phase = {
            "status": "completed",
        }

        with pytest.raises(AssertionError, match="duration_ms"):
            assert_phase_timing_valid(phase, "test_phase")

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_negative_duration_fails(self) -> None:
        """Test that negative duration fails validation."""
        phase = {
            "duration_ms": -100,
        }

        with pytest.raises(AssertionError):
            assert_phase_timing_valid(phase, "test_phase")

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_zero_duration_allowed_when_not_required_positive(self) -> None:
        """Test that zero duration is allowed when require_positive_duration=False."""
        phase = {
            "duration_ms": 0,
        }

        # Should not raise with require_positive_duration=False
        assert_phase_timing_valid(phase, "test_phase", require_positive_duration=False)


# ============================================================================
# Execution Phases Validation Tests
# ============================================================================


class TestExecutionPhasesValidation:
    """Tests for execution phases structure validation."""

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_valid_execution_phases_passes(self) -> None:
        """Test that valid execution phases pass validation."""
        result = {
            "benchmark_name": "TPC-H",
            "platform": "duckdb",
            "scale_factor": 0.01,
            "execution_id": "test-123",
            "timestamp": "2024-01-15T10:30:00Z",
            "duration_seconds": 45.5,
            "total_queries": 22,
            "successful_queries": 22,
            "failed_queries": 0,
            "execution_phases": {
                "setup": {
                    "data_generation": {
                        "duration_ms": 1000,
                        "status": "completed",
                    },
                    "data_loading": {
                        "duration_ms": 2000,
                        "status": "completed",
                    },
                },
                "power_test": {
                    "duration_ms": 10000,
                    "start_time": "2024-01-15T10:30:00Z",
                    "end_time": "2024-01-15T10:30:10Z",
                    "query_executions": [
                        {"query_id": "Q1", "status": "SUCCESS", "execution_time_ms": 100},
                    ],
                },
            },
        }

        is_valid, errors = validate_execution_phases(result)
        assert is_valid, f"Validation failed: {errors}"

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_invalid_power_test_duration_fails(self) -> None:
        """Test that invalid power_test duration fails validation."""
        result = {
            "execution_phases": {
                "setup": {},
                "power_test": {
                    "duration_ms": -100,  # Invalid negative duration
                    "query_executions": [],
                },
            },
        }

        is_valid, errors = validate_execution_phases(result)
        assert not is_valid
        assert any("power_test" in e.field for e in errors)

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_invalid_throughput_streams_fails(self) -> None:
        """Test that invalid throughput test num_streams fails validation."""
        result = {
            "execution_phases": {
                "setup": {},
                "throughput_test": {
                    "duration_ms": 10000,
                    "num_streams": -1,  # Invalid negative
                    "streams": [],
                },
            },
        }

        is_valid, errors = validate_execution_phases(result)
        assert not is_valid
        assert any("num_streams" in e.field for e in errors)


# ============================================================================
# Result Validator Class Tests
# ============================================================================


class TestResultValidatorClass:
    """Tests for ResultValidator class."""

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_validator_collects_all_errors(self) -> None:
        """Test that validator collects all errors in one pass."""
        invalid_result = {
            "benchmark_name": "",  # Empty
            "platform": "",  # Empty
            "scale_factor": -1,  # Negative
            "execution_id": "",  # Empty
            "timestamp": "invalid",  # Invalid format
            "duration_seconds": -10,  # Negative
            "total_queries": 10,
            "successful_queries": 5,
            "failed_queries": 3,  # 5 + 3 != 10
        }

        validator = ResultValidator(invalid_result)
        is_valid = validator.validate_all()

        assert not is_valid
        # Should have collected multiple errors
        assert len(validator.errors) > 1

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_validator_reusable(self) -> None:
        """Test that validator can be reused with different data."""
        valid_result = {
            "benchmark_name": "TPC-H",
            "platform": "duckdb",
            "scale_factor": 0.01,
            "execution_id": "test-123",
            "timestamp": "2024-01-15T10:30:00Z",
            "duration_seconds": 45.5,
            "total_queries": 22,
            "successful_queries": 22,
            "failed_queries": 0,
        }

        validator = ResultValidator(valid_result)
        assert validator.validate_all()
        assert len(validator.errors) == 0

        # Validate again - errors should be cleared
        assert validator.validate_all()
        assert len(validator.errors) == 0


# ============================================================================
# Load and Validate Result File Tests
# ============================================================================


class TestLoadResultFile:
    """Tests for loading and validating result files."""

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_load_valid_json_file(self, tmp_path: Path) -> None:
        """Test loading a valid JSON result file."""
        result_data = {
            "benchmark_name": "TPC-H",
            "platform": "duckdb",
            "scale_factor": 0.01,
            "execution_id": "test-123",
            "timestamp": "2024-01-15T10:30:00Z",
            "duration_seconds": 45.5,
            "total_queries": 22,
            "successful_queries": 22,
            "failed_queries": 0,
        }

        result_file = tmp_path / "result.json"
        result_file.write_text(json.dumps(result_data))

        loaded = load_result_json(result_file)
        assert loaded == result_data

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_load_nonexistent_file_raises(self, tmp_path: Path) -> None:
        """Test that loading nonexistent file raises FileNotFoundError."""
        nonexistent = tmp_path / "nonexistent.json"

        with pytest.raises(FileNotFoundError):
            load_result_json(nonexistent)

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_load_invalid_json_raises(self, tmp_path: Path) -> None:
        """Test that loading invalid JSON raises JSONDecodeError."""
        invalid_file = tmp_path / "invalid.json"
        invalid_file.write_text("not valid json {{{")

        with pytest.raises(json.JSONDecodeError):
            load_result_json(invalid_file)

    @pytest.mark.e2e
    @pytest.mark.e2e_quick
    def test_load_and_validate_file(self, tmp_path: Path) -> None:
        """Test loading and validating a result file end-to-end."""
        result_data = {
            "benchmark_name": "TPC-H",
            "platform": "duckdb",
            "scale_factor": 0.01,
            "execution_id": "test-123",
            "timestamp": "2024-01-15T10:30:00Z",
            "duration_seconds": 45.5,
            "total_queries": 22,
            "successful_queries": 22,
            "failed_queries": 0,
            "query_results": [
                {"query_id": "Q1", "status": "SUCCESS", "execution_time_ms": 100},
            ],
        }

        result_file = tmp_path / "result.json"
        result_file.write_text(json.dumps(result_data))

        loaded = load_result_json(result_file)
        assert_benchmark_result_valid(loaded)


# ============================================================================
# Parametrized Validation Tests
# ============================================================================


@pytest.mark.e2e
@pytest.mark.e2e_quick
@pytest.mark.parametrize(
    "timestamp",
    [
        "2024-01-15T10:30:00Z",
        "2024-01-15T10:30:00+00:00",
        "2024-01-15T10:30:00.000Z",
        "2024-01-15T10:30:00.123456Z",
    ],
)
def test_valid_timestamp_formats(timestamp: str) -> None:
    """Test that various valid timestamp formats are accepted."""
    result = {
        "benchmark_name": "TPC-H",
        "platform": "duckdb",
        "scale_factor": 0.01,
        "execution_id": "test-123",
        "timestamp": timestamp,
        "duration_seconds": 45.5,
        "total_queries": 22,
        "successful_queries": 22,
        "failed_queries": 0,
    }

    is_valid, errors = validate_result_structure(result)
    assert is_valid, f"Timestamp {timestamp} should be valid: {errors}"


@pytest.mark.e2e
@pytest.mark.e2e_quick
@pytest.mark.parametrize(
    "scale_factor",
    [0.01, 0.1, 1, 10, 100, 1000],
)
def test_valid_scale_factors(scale_factor: float) -> None:
    """Test that various valid scale factors are accepted."""
    result = {
        "benchmark_name": "TPC-H",
        "platform": "duckdb",
        "scale_factor": scale_factor,
        "execution_id": "test-123",
        "timestamp": "2024-01-15T10:30:00Z",
        "duration_seconds": 45.5,
        "total_queries": 22,
        "successful_queries": 22,
        "failed_queries": 0,
    }

    is_valid, errors = validate_result_structure(result)
    assert is_valid, f"Scale factor {scale_factor} should be valid: {errors}"
