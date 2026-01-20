"""Unit tests for generic power test handler in platform adapters.

Tests the _execute_generic_power_test method that provides warmup + iteration
support for non-TPC benchmarks (ClickBench, SSB, H2O-DB, etc.).
"""

from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.fast


class TestGenericPowerTest:
    """Test suite for _execute_generic_power_test method."""

    @pytest.fixture
    def mock_adapter(self):
        """Create a mock platform adapter with required methods."""
        from benchbox.platforms.duckdb import DuckDBAdapter

        adapter = DuckDBAdapter()
        # Mock the _execute_all_queries method to avoid actual query execution
        adapter._execute_all_queries = MagicMock()
        return adapter

    @pytest.fixture
    def mock_benchmark(self):
        """Create a mock benchmark instance."""
        benchmark = MagicMock()
        benchmark._name = "ClickBench"
        benchmark.scale_factor = 1.0
        return benchmark

    @pytest.fixture
    def mock_connection(self):
        """Create a mock database connection."""
        return MagicMock()

    def test_warmup_results_discarded(self, mock_adapter, mock_benchmark, mock_connection):
        """Test that warmup run results are NOT included in final output."""
        # Configure warmup + measurement runs
        run_config = {
            "iterations": 2,
            "warm_up_iterations": 1,
            "scale_factor": 1.0,
        }

        # Mock _execute_all_queries to return different results each call
        mock_adapter._execute_all_queries.side_effect = [
            # Warmup run (should be discarded)
            [{"query_id": "Q1", "status": "SUCCESS", "execution_time": 1.0}],
            # Measurement run 1
            [{"query_id": "Q1", "status": "SUCCESS", "execution_time": 2.0}],
            # Measurement run 2
            [{"query_id": "Q1", "status": "SUCCESS", "execution_time": 3.0}],
        ]

        results = mock_adapter._execute_generic_power_test(mock_benchmark, mock_connection, run_config)

        # Assert _execute_all_queries called 3 times (1 warmup + 2 measurement)
        assert mock_adapter._execute_all_queries.call_count == 3

        # Assert only 2 results returned (warmup discarded)
        assert len(results) == 2

        # Assert warmup result NOT in final output
        assert all(r["execution_time"] != 1.0 for r in results)

        # Assert measurement results ARE in final output
        assert results[0]["execution_time"] == 2.0
        assert results[1]["execution_time"] == 3.0

    def test_iteration_tagging(self, mock_adapter, mock_benchmark, mock_connection):
        """Test that measurement results are tagged with iteration number."""
        run_config = {
            "iterations": 3,
            "warm_up_iterations": 0,
            "scale_factor": 1.0,
        }

        # Mock _execute_all_queries to return NEW dict each time (avoid reference reuse)
        def return_new_result():
            return [{"query_id": "Q1", "status": "SUCCESS", "execution_time": 1.0}]

        mock_adapter._execute_all_queries.side_effect = [
            return_new_result(),
            return_new_result(),
            return_new_result(),
        ]

        results = mock_adapter._execute_generic_power_test(mock_benchmark, mock_connection, run_config)

        # Assert 3 results (one per iteration)
        assert len(results) == 3

        # Assert each result tagged with iteration number
        assert results[0]["iteration"] == 1
        assert results[0]["run_type"] == "measurement"

        assert results[1]["iteration"] == 2
        assert results[1]["run_type"] == "measurement"

        assert results[2]["iteration"] == 3
        assert results[2]["run_type"] == "measurement"

    def test_no_warmup_runs(self, mock_adapter, mock_benchmark, mock_connection):
        """Test behavior when warm_up_iterations is 0."""
        run_config = {
            "iterations": 2,
            "warm_up_iterations": 0,
            "scale_factor": 1.0,
        }

        mock_adapter._execute_all_queries.return_value = [
            {"query_id": "Q1", "status": "SUCCESS", "execution_time": 1.0}
        ]

        results = mock_adapter._execute_generic_power_test(mock_benchmark, mock_connection, run_config)

        # Assert _execute_all_queries called exactly 2 times (no warmup)
        assert mock_adapter._execute_all_queries.call_count == 2

        # Assert 2 results
        assert len(results) == 2

    def test_default_iterations_value(self, mock_adapter, mock_benchmark, mock_connection):
        """Test that iterations defaults to 3 when not specified."""
        run_config = {
            "warm_up_iterations": 0,
            "scale_factor": 1.0,
        }

        mock_adapter._execute_all_queries.return_value = [
            {"query_id": "Q1", "status": "SUCCESS", "execution_time": 1.0}
        ]

        results = mock_adapter._execute_generic_power_test(mock_benchmark, mock_connection, run_config)

        # Assert _execute_all_queries called 3 times (default iterations=3)
        assert mock_adapter._execute_all_queries.call_count == 3

        # Assert 3 results
        assert len(results) == 3

    def test_multiple_queries_per_iteration(self, mock_adapter, mock_benchmark, mock_connection):
        """Test behavior with multiple queries per iteration."""
        run_config = {
            "iterations": 2,
            "warm_up_iterations": 1,
            "scale_factor": 1.0,
        }

        # Mock _execute_all_queries to return NEW dicts each time
        def return_new_queries():
            return [
                {"query_id": "Q1", "status": "SUCCESS", "execution_time": 1.0},
                {"query_id": "Q2", "status": "SUCCESS", "execution_time": 2.0},
                {"query_id": "Q3", "status": "SUCCESS", "execution_time": 3.0},
            ]

        mock_adapter._execute_all_queries.side_effect = [
            return_new_queries(),  # Warmup
            return_new_queries(),  # Iteration 1
            return_new_queries(),  # Iteration 2
        ]

        results = mock_adapter._execute_generic_power_test(mock_benchmark, mock_connection, run_config)

        # Assert 6 total results (3 queries × 2 iterations)
        assert len(results) == 6

        # Assert each query appears in each iteration
        q1_results = [r for r in results if r["query_id"] == "Q1"]
        assert len(q1_results) == 2
        assert q1_results[0]["iteration"] == 1
        assert q1_results[1]["iteration"] == 2

    @patch("rich.console.Console")
    def test_console_output_displays_warmup_and_measurement(
        self, mock_console_cls, mock_adapter, mock_benchmark, mock_connection
    ):
        """Test that console prints warmup and measurement run labels."""
        mock_console = MagicMock()
        mock_console_cls.return_value = mock_console

        run_config = {
            "iterations": 2,
            "warm_up_iterations": 1,
            "scale_factor": 1.0,
        }

        # Return new dicts each time
        def return_new_result():
            return [{"query_id": "Q1", "status": "SUCCESS", "execution_time": 1.0}]

        mock_adapter._execute_all_queries.side_effect = [
            return_new_result(),  # Warmup
            return_new_result(),  # Iteration 1
            return_new_result(),  # Iteration 2
        ]

        mock_adapter._execute_generic_power_test(mock_benchmark, mock_connection, run_config)

        # Check console.print was called with warmup/measurement labels
        console_calls = [str(call) for call in mock_console.print.call_args_list]

        # Should contain warmup label
        assert any("Warm-up Run" in str(c) for c in console_calls)

        # Should contain measurement label
        assert any("Measurement Run" in str(c) for c in console_calls)

        # Should display warmup + measurement counts
        assert any("Warm-up runs: 1" in str(c) for c in console_calls)
        assert any("Measurement runs: 2" in str(c) for c in console_calls)

    def test_failed_queries_included_in_results(self, mock_adapter, mock_benchmark, mock_connection):
        """Test that failed queries are still included with iteration tags."""
        run_config = {
            "iterations": 2,
            "warm_up_iterations": 0,
            "scale_factor": 1.0,
        }

        # Mock _execute_all_queries to return NEW dicts each time (mix of success/failure)
        def return_new_mixed_results():
            return [
                {"query_id": "Q1", "status": "SUCCESS", "execution_time": 1.0},
                {"query_id": "Q2", "status": "FAILED", "execution_time": 0.0, "error": "timeout"},
            ]

        mock_adapter._execute_all_queries.side_effect = [
            return_new_mixed_results(),  # Iteration 1
            return_new_mixed_results(),  # Iteration 2
        ]

        results = mock_adapter._execute_generic_power_test(mock_benchmark, mock_connection, run_config)

        # Assert 4 total results (2 queries × 2 iterations)
        assert len(results) == 4

        # Assert failed queries have iteration tags
        failed_results = [r for r in results if r["status"] == "FAILED"]
        assert len(failed_results) == 2
        assert all("iteration" in r for r in failed_results)
        assert all(r["run_type"] == "measurement" for r in failed_results)
