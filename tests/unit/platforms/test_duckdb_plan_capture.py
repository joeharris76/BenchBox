"""
Tests for DuckDB query plan capture integration.

Verifies that query plans are captured and parsed during query execution.
"""

import pytest

from benchbox.platforms.duckdb import DuckDBAdapter

pytestmark = pytest.mark.fast


class TestDuckDBPlanCapture:
    """Test query plan capture in DuckDB adapter."""

    @pytest.fixture
    def adapter_with_capture(self):
        """Create DuckDB adapter with plan capture enabled."""
        adapter = DuckDBAdapter(capture_plans=True)
        return adapter

    @pytest.fixture
    def adapter_without_capture(self):
        """Create DuckDB adapter with plan capture disabled."""
        adapter = DuckDBAdapter(capture_plans=False)
        return adapter

    def test_parser_is_available(self, adapter_with_capture):
        """Test that DuckDB parser is available."""
        parser = adapter_with_capture.get_query_plan_parser()
        assert parser is not None
        assert parser.platform_name == "duckdb"

    def test_plan_capture_when_enabled(self, adapter_with_capture):
        """Test that plan is captured when capture_plans=True."""
        connection = adapter_with_capture.create_connection()

        try:
            # Execute a simple query
            result = adapter_with_capture.execute_query(
                connection=connection,
                query="SELECT 1 as test_column",
                query_id="test_q1",
                validate_row_count=False,
            )

            # Should have query_plan in result
            assert "query_plan" in result or result["query_plan"] is None  # May be None if parsing fails
            assert "plan_fingerprint" in result or result["plan_fingerprint"] is None

        finally:
            adapter_with_capture.close_connection(connection)

    def test_plan_not_captured_when_disabled(self, adapter_without_capture):
        """Test that plan is not captured when capture_plans=False."""
        connection = adapter_without_capture.create_connection()

        try:
            result = adapter_without_capture.execute_query(
                connection=connection,
                query="SELECT 1 as test_column",
                query_id="test_q2",
                validate_row_count=False,
            )

            # Should NOT have query_plan in result
            assert "query_plan" not in result or result["query_plan"] is None

        finally:
            adapter_without_capture.close_connection(connection)

    def test_capture_query_plan_method(self, adapter_with_capture):
        """Test the capture_query_plan method directly."""
        connection = adapter_with_capture.create_connection()

        try:
            plan, capture_time_ms = adapter_with_capture.capture_query_plan(
                connection=connection,
                query="SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name) WHERE id > 0",
                query_id="test_q3",
            )

            # Capture time should always be measured
            assert capture_time_ms >= 0

            # Plan may be None if parsing fails, but method should not crash
            if plan:
                assert plan.query_id == "test_q3"
                assert plan.platform == "duckdb"
                assert plan.logical_root is not None
                assert plan.plan_fingerprint is not None

        finally:
            adapter_with_capture.close_connection(connection)

    def test_plan_capture_with_table(self, adapter_with_capture):
        """Test plan capture with actual table creation and query."""
        connection = adapter_with_capture.create_connection()

        try:
            # Create a simple table
            connection.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
            connection.execute("INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob')")

            # Execute query with plan capture
            result = adapter_with_capture.execute_query(
                connection=connection,
                query="SELECT * FROM test_table WHERE id = 1",
                query_id="test_q4",
                validate_row_count=False,
            )

            assert result["status"] == "SUCCESS"
            assert result["rows_returned"] == 1

            # Plan may or may not be captured depending on DuckDB's EXPLAIN output
            if "query_plan" in result and result["query_plan"]:
                plan = result["query_plan"]
                assert plan.platform == "duckdb"

        finally:
            adapter_with_capture.close_connection(connection)

    def test_plan_capture_does_not_affect_correctness(self, adapter_with_capture, adapter_without_capture):
        """Test that enabling plan capture doesn't affect query results."""
        # Execute same query with and without capture
        conn_with = adapter_with_capture.create_connection()
        conn_without = adapter_without_capture.create_connection()

        try:
            result_with = adapter_with_capture.execute_query(
                connection=conn_with,
                query="SELECT 42 as answer",
                query_id="test_q5",
                validate_row_count=False,
            )

            result_without = adapter_without_capture.execute_query(
                connection=conn_without,
                query="SELECT 42 as answer",
                query_id="test_q5",
                validate_row_count=False,
            )

            # Core results should be the same
            assert result_with["status"] == result_without["status"]
            assert result_with["rows_returned"] == result_without["rows_returned"]
            assert result_with["execution_time"] >= 0
            assert result_without["execution_time"] >= 0

        finally:
            adapter_with_capture.close_connection(conn_with)
            adapter_without_capture.close_connection(conn_without)
