"""Tests for ClickHouse platform adapter.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from benchbox.platforms.clickhouse import ClickHouseAdapter

pytestmark = pytest.mark.fast

# Check for optional dependencies
try:
    import importlib.util

    CHDB_AVAILABLE = importlib.util.find_spec("chdb") is not None
except ImportError:
    CHDB_AVAILABLE = False


@pytest.fixture(autouse=True)
def clickhouse_dependencies():
    """Mock ClickHouse dependency check to simulate installed extras."""

    with patch("benchbox.platforms.clickhouse.adapter.check_platform_dependencies", return_value=(True, [])):
        yield


class TestClickHouseAdapter:
    """Test ClickHouse platform adapter functionality."""

    def test_initialization_success(self):
        """Test successful adapter initialization in server mode."""
        with patch("benchbox.platforms.clickhouse.setup.ClickHouseClient"):
            adapter = ClickHouseAdapter(
                deployment_mode="server",  # Explicitly request server mode
                host="localhost",
                port=9000,
                database="test",
                username="test",
                password="test",
            )
            assert adapter.platform_name == "ClickHouse (Server)"
            assert adapter.dialect == "clickhouse"
            assert adapter.host == "localhost"
            assert adapter.port == 9000
            assert adapter.deployment_mode == "server"

    def test_initialization_missing_driver(self):
        """Test initialization when ClickHouse driver dependencies are missing (server mode)."""
        with (
            patch(
                "benchbox.platforms.clickhouse.adapter.check_platform_dependencies",
                return_value=(False, ["clickhouse-driver"]),
            ),
            pytest.raises(ImportError) as excinfo,
        ):
            ClickHouseAdapter(deployment_mode="server")  # Server mode requires driver

        assert "Missing dependencies for clickhouse platform" in str(excinfo.value)

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_create_connection_success(self, mock_client_class):
        """Test successful connection creation."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        # Mock database check to return empty list (database doesn't exist)
        mock_client.execute.side_effect = [
            [],
            None,
        ]  # SHOW DATABASES returns [], SELECT 1 returns None

        adapter = ClickHouseAdapter(deployment_mode="server", host="localhost", port=9000)
        connection = adapter.create_connection()

        assert connection == mock_client
        # Should be called twice: once for admin client (db check), once for main client
        assert mock_client_class.call_count == 2
        # Should execute SHOW DATABASES and SELECT 1
        assert mock_client.execute.call_count == 2

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_create_connection_failure(self, mock_client_class):
        """Test connection creation failure."""
        mock_client_class.side_effect = Exception("Connection failed")

        adapter = ClickHouseAdapter(deployment_mode="server")

        with pytest.raises(Exception, match="Connection failed"):
            adapter.create_connection()

    def test_sql_translation(self):
        """Test SQL dialect translation."""
        with patch("benchbox.platforms.clickhouse.setup.ClickHouseClient"):
            adapter = ClickHouseAdapter(deployment_mode="server")

            # Test with sqlglot available
            with patch("sqlglot.transpile") as mock_transpile:
                mock_transpile.return_value = ['SELECT * FROM "table"']

                result = adapter.translate_sql("SELECT * FROM table", "duckdb")
                # translate_sql adds semicolon at the end
                assert result == 'SELECT * FROM "table";'
                mock_transpile.assert_called_once_with("SELECT * FROM table", read="duckdb", write="clickhouse")

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_create_schema(self, mock_client_class):
        """Test schema creation."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_benchmark = Mock()
        mock_benchmark.get_create_tables_sql.return_value = (
            "CREATE TABLE test (id INT); CREATE TABLE test2 (name STRING);"
        )

        adapter = ClickHouseAdapter(deployment_mode="server")
        connection = adapter.create_connection()

        schema_time = adapter.create_schema(mock_benchmark, connection)

        assert isinstance(schema_time, float)
        assert schema_time >= 0
        # Should execute multiple statements
        assert mock_client.execute.call_count >= 2

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_load_data_with_tables(self, mock_client_class):
        """Test data loading with table files."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        # Mock database check to return empty list (database doesn't exist)
        # Mock SELECT 1 to return None for connection test
        # Mock COUNT(*) query to return the row count
        # Mock: database check (connection setup), connection test (connection setup), INSERT statement, COUNT(*) query
        mock_client.execute.side_effect = [[], None, None, [[100]]]

        # Create temporary test file first
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("1,test\n2,test2\n")
            temp_path = Path(f.name)

        try:
            # Create mock benchmark with proper dict attribute (not a Mock)
            mock_benchmark = Mock(spec=["tables", "get_schema"])  # Only allow specific attributes
            tables_dict = {"test_table": str(temp_path)}
            mock_benchmark.tables = tables_dict
            mock_benchmark.get_schema.return_value = {}  # Return empty schema

            adapter = ClickHouseAdapter(deployment_mode="server")
            connection = adapter.create_connection()

            table_stats, load_time, _ = adapter.load_data(mock_benchmark, connection, Path("/tmp"))

            assert isinstance(table_stats, dict)
            assert isinstance(load_time, float)
            assert load_time >= 0
            assert "test_table" in table_stats  # Table names are lowercase per TPC spec
            assert table_stats["test_table"] == 100

        finally:
            temp_path.unlink()

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_execute_query_success(self, mock_client_class):
        """Test successful query execution."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        mock_client.execute.return_value = [[1, "test"], [2, "test2"]]

        adapter = ClickHouseAdapter(deployment_mode="server")
        connection = adapter.create_connection()

        result = adapter.execute_query(connection, "SELECT * FROM test", "q1")

        assert result["query_id"] == "q1"
        assert result["status"] == "SUCCESS"
        assert result["rows_returned"] == 2
        assert result["first_row"] == [1, "test"]
        assert isinstance(result["execution_time"], float)

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_execute_query_failure(self, mock_client_class):
        """Test query execution failure."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        # Mock successful connection setup, then query failure
        mock_client.execute.side_effect = [
            [],
            None,
            Exception("Query failed"),
        ]  # db check, connection test, then query failure

        adapter = ClickHouseAdapter(deployment_mode="server")
        connection = adapter.create_connection()

        result = adapter.execute_query(connection, "INVALID SQL", "q1")

        assert result["query_id"] == "q1"
        assert result["status"] == "FAILED"
        assert result["rows_returned"] == 0
        assert result["error"] == "Query failed"
        assert isinstance(result["execution_time"], float)

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_configure_for_benchmark_tuning_disabled(self, mock_client_class):
        """Test benchmark optimization when tuning is disabled."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Use server mode to avoid chdb initialization in unit tests
        adapter = ClickHouseAdapter(deployment_mode="server", strict_validation=False)
        adapter.tuning_enabled = False  # Explicitly disable tuning
        connection = adapter.create_connection()

        # Should apply OLAP optimizations for OLAP benchmark types
        adapter.configure_for_benchmark(connection, "olap")

        # Should execute multiple optimization statements (basic + OLAP)
        assert mock_client.execute.call_count > 5  # basic settings + OLAP settings

        # Reset mock for next test
        mock_client.reset_mock()

        # Should only apply basic optimizations for non-OLAP benchmark types
        adapter.configure_for_benchmark(connection, "read_primitives")

        # Should execute fewer statements (basic settings + cache control + validation + server memory ratio = 11)
        assert mock_client.execute.call_count == 11  # basic (6) + cache (3) + validation (1) + server memory ratio (1)

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_configure_for_benchmark_tuning_enabled(self, mock_client_class):
        """Test benchmark optimization when tuning is enabled."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        # Mock database check to return empty list
        mock_client.execute.side_effect = [[], None]

        # Use server mode to avoid chdb initialization in unit tests
        adapter = ClickHouseAdapter(deployment_mode="server", strict_validation=False)
        adapter.tuning_enabled = True  # Enable tuning
        connection = adapter.create_connection()

        # Reset mock to clear the connection setup calls
        mock_client.reset_mock()

        # Should only apply basic settings when tuning is enabled
        adapter.configure_for_benchmark(connection, "olap")

        # Should execute only basic optimization statements (no OLAP-specific ones)
        # Basic settings (6) + cache control settings (3) + validation query (1) + server memory ratio (1) = 11
        assert mock_client.execute.call_count == 11  # basic + cache + validation + server memory ratio

    @pytest.mark.skipif(not CHDB_AVAILABLE, reason="chDB not installed (required for embedded mode test)")
    def test_configure_for_benchmark_embedded_mode(self):
        """Test benchmark optimization in embedded mode skips problematic settings."""
        # Skip if chdb is not available - this test specifically tests embedded mode behavior
        # which requires actual chdb to be installed
        mock_connection = Mock()

        # Create adapter in local mode (embedded is now an alias for local)
        # Use a unique database path to avoid conflicts with other tests
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter = ClickHouseAdapter(
                deployment_mode="local",
                strict_validation=False,
                data_path=tmpdir,
            )
            adapter.tuning_enabled = False  # Disable tuning to test OLAP optimizations

            # Should apply OLAP optimizations but skip embedded-incompatible settings
            adapter.configure_for_benchmark(mock_connection, "olap")

            # Check that problematic settings were not applied
            executed_statements = [call[0][0] for call in mock_connection.execute.call_args_list]
            executed_sql = " ".join(executed_statements)

            # These settings should not be present in local/embedded mode
            assert "join_algorithm" not in executed_sql
            assert "enable_multiple_joins_emulation" not in executed_sql

            # But other settings should still be applied
            assert "max_memory_usage" in executed_sql
            assert "max_threads" in executed_sql

    def test_get_database_path_server_mode(self):
        """Test database path generation in server mode returns None."""
        with patch("benchbox.platforms.clickhouse.setup.ClickHouseClient"):
            adapter = ClickHouseAdapter(mode="server")

            result = adapter.get_database_path(database_path="some/path.duckdb")
            assert result is None

    @pytest.mark.skipif(not CHDB_AVAILABLE, reason="chDB not installed (required for embedded mode)")
    def test_apply_setting_with_validation_embedded_mode(self):
        """Test setting validation in embedded mode."""
        with patch("benchbox.platforms.clickhouse.setup.ClickHouseClient"):
            mock_connection = Mock()

            adapter = ClickHouseAdapter(mode="embedded")

            # Test that known problematic settings are skipped in embedded mode
            result = adapter._apply_setting_with_validation(mock_connection, "join_algorithm", "hash")
            assert result is False
            mock_connection.execute.assert_not_called()

            mock_connection.reset_mock()

            result = adapter._apply_setting_with_validation(mock_connection, "enable_multiple_joins_emulation", 1)
            assert result is False
            mock_connection.execute.assert_not_called()

            # Test that safe settings are still applied
            mock_connection.reset_mock()
            result = adapter._apply_setting_with_validation(mock_connection, "max_threads", 4)
            assert result is True
            mock_connection.execute.assert_called_once_with("SET max_threads = 4")

    def test_apply_setting_with_validation_server_mode(self):
        """Test setting validation in server mode."""
        with patch("benchbox.platforms.clickhouse.setup.ClickHouseClient"):
            mock_connection = Mock()

            adapter = ClickHouseAdapter(mode="server")

            # Test that problematic settings are attempted in server mode
            result = adapter._apply_setting_with_validation(mock_connection, "join_algorithm", "hash")
            assert result is True
            mock_connection.execute.assert_called_once_with("SET join_algorithm = hash")

            # Test error handling
            mock_connection.reset_mock()
            mock_connection.execute.side_effect = Exception("Setting not supported")

            result = adapter._apply_setting_with_validation(mock_connection, "some_setting", "value")
            assert result is False
            mock_connection.execute.assert_called_once_with("SET some_setting = value")

    def test_memory_setting_parsing(self):
        """Test memory setting parsing."""
        with patch("benchbox.platforms.clickhouse.setup.ClickHouseClient"):
            adapter = ClickHouseAdapter(deployment_mode="server")

            assert adapter._parse_memory_setting("8GB") == 8 * 1024 * 1024 * 1024
            assert adapter._parse_memory_setting("512MB") == 512 * 1024 * 1024
            assert adapter._parse_memory_setting("1024KB") == 1024 * 1024
            assert adapter._parse_memory_setting(1024) == 1024

    def test_table_optimization(self):
        """Test table definition optimization."""
        with patch("benchbox.platforms.clickhouse.setup.ClickHouseClient"):
            adapter = ClickHouseAdapter(deployment_mode="server")

            # Test adding MergeTree engine
            original = "CREATE TABLE test (id INT, name STRING)"
            optimized = adapter._optimize_table_definition(original)
            expected = "CREATE TABLE test (id INT, name STRING) ENGINE = MergeTree() ORDER BY tuple()"
            assert optimized == expected

            # Test with existing engine
            with_engine = "CREATE TABLE test (id INT) ENGINE = ReplacingMergeTree()"
            optimized_with_engine = adapter._optimize_table_definition(with_engine)
            expected_with_engine = "CREATE TABLE test (id INT) ENGINE = ReplacingMergeTree() ORDER BY tuple()"
            assert optimized_with_engine == expected_with_engine

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_get_platform_metadata(self, mock_client_class):
        """Test platform metadata collection."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock version query
        mock_client.execute.return_value = [["21.8.0"]]

        adapter = ClickHouseAdapter(deployment_mode="server", host="test", port=9000, database="test")
        connection = adapter.create_connection()

        metadata = adapter._get_platform_metadata(connection)

        assert metadata["platform"] == "ClickHouse (Server)"
        assert metadata["host"] == "test"
        assert metadata["port"] == 9000
        assert metadata["database"] == "test"
        assert "clickhouse_version" in metadata

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_get_table_info(self, mock_client_class):
        """Test table information retrieval."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock connection setup, then schema and stats queries
        mock_client.execute.side_effect = [
            [],  # database check
            None,  # connection test
            [("id", "Int32"), ("name", "String")],  # schema query
            [(1000, 1024000, 512000)],  # stats query
        ]

        adapter = ClickHouseAdapter(deployment_mode="server")
        connection = adapter.create_connection()

        info = adapter.get_table_info(connection, "test_table")

        assert info["columns"] == [("id", "Int32"), ("name", "String")]
        assert info["row_count"] == 1000
        assert info["bytes_on_disk"] == 1024000
        assert info["compressed_size"] == 512000

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_optimize_table(self, mock_client_class):
        """Test table optimization."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        adapter = ClickHouseAdapter(deployment_mode="server")
        connection = adapter.create_connection()

        adapter.optimize_table(connection, "test_table")

        mock_client.execute.assert_called_with("OPTIMIZE TABLE test_table FINAL")

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_close_connection(self, mock_client_class):
        """Test connection closing."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        adapter = ClickHouseAdapter(deployment_mode="server")
        connection = adapter.create_connection()

        adapter.close_connection(connection)

        mock_client.disconnect.assert_called_once()

    def test_test_connection(self):
        """Test connection testing."""
        with patch("benchbox.platforms.clickhouse.setup.ClickHouseClient") as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client

            adapter = ClickHouseAdapter(deployment_mode="server")

            # Test successful connection
            assert adapter.test_connection() is True

            # Test failed connection
            mock_client_class.side_effect = Exception("Connection failed")
            assert adapter.test_connection() is False

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_apply_table_tunings_with_sorting(self, mock_client_class):
        """Test applying table tunings with sorting configuration."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock table tuning object
        mock_tuning = Mock()
        mock_tuning.table_name = "test_table"

        # Mock sorting column
        mock_sort_col = Mock()
        mock_sort_col.name = "id"
        mock_sort_col.order = 1

        # Mock the get_columns_by_type method
        def mock_get_columns_by_type(tuning_type):
            # Import here to avoid circular imports in test
            from benchbox.core.tuning.interface import TuningType

            if str(tuning_type) == str(TuningType.SORTING):
                return [mock_sort_col]
            return []

        mock_tuning.get_columns_by_type.side_effect = mock_get_columns_by_type

        mock_tuning.sorting = [mock_sort_col]  # Has sorting configuration
        mock_tuning.clustering = None
        mock_tuning.partitioning = None
        mock_tuning.distribution = None

        adapter = ClickHouseAdapter(deployment_mode="server")
        connection = adapter.create_connection()

        # Should not raise exception
        adapter.apply_table_tunings(mock_tuning, connection)

        # Should call optimize_table (which calls OPTIMIZE TABLE)
        expected_calls = [call for call in mock_client.execute.call_args_list if "OPTIMIZE" in str(call)]
        assert len(expected_calls) > 0

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_apply_table_tunings_with_clustering(self, mock_client_class):
        """Test applying table tunings with clustering configuration."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock table tuning object
        mock_tuning = Mock()
        mock_tuning.table_name = "test_table"

        # Mock clustering column
        mock_cluster_col = Mock()
        mock_cluster_col.name = "cluster_key"
        mock_cluster_col.order = 1

        # Mock the get_columns_by_type method
        def mock_get_columns_by_type(tuning_type):
            # Import here to avoid circular imports in test
            from benchbox.core.tuning.interface import TuningType

            if str(tuning_type) == str(TuningType.CLUSTERING):
                return [mock_cluster_col]
            return []

        mock_tuning.get_columns_by_type.side_effect = mock_get_columns_by_type

        mock_tuning.sorting = None
        mock_tuning.clustering = [mock_cluster_col]  # Has clustering configuration
        mock_tuning.partitioning = None
        mock_tuning.distribution = None

        adapter = ClickHouseAdapter(deployment_mode="server")
        connection = adapter.create_connection()

        # Should not raise exception
        adapter.apply_table_tunings(mock_tuning, connection)

        # Should call OPTIMIZE TABLE FINAL for clustering
        optimize_final_calls = [
            call for call in mock_client.execute.call_args_list if "OPTIMIZE TABLE test_table FINAL" in str(call)
        ]
        assert len(optimize_final_calls) > 0

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_apply_table_tunings_none(self, mock_client_class):
        """Test applying table tunings with None input."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        adapter = ClickHouseAdapter(deployment_mode="server")
        connection = adapter.create_connection()

        # Should not raise exception with None tuning
        adapter.apply_table_tunings(None, connection)

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_generate_tuning_clause_with_partitioning(self, mock_client_class):
        """Test generating tuning clause with partitioning."""
        mock_client_class.return_value = Mock()

        # Mock TuningType and columns
        with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
            mock_tuning_type.PARTITIONING = "partitioning"

            # Mock table tuning object
            mock_tuning = Mock()
            mock_tuning.table_name = "test_table"
            mock_tuning.partitioning = [Mock()]  # Has partitioning configuration
            mock_tuning.clustering = None
            mock_tuning.sorting = None
            mock_tuning.distribution = None

            # Mock column object
            mock_column = Mock()
            mock_column.name = "date_col"
            mock_column.order = 1

            mock_tuning.get_columns_by_type.return_value = [mock_column]

            adapter = ClickHouseAdapter(deployment_mode="server")

            clause = adapter.generate_tuning_clause(mock_tuning)

            assert "PARTITION BY" in clause
            assert "date_col" in clause

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_generate_tuning_clause_with_sorting(self, mock_client_class):
        """Test generating tuning clause with sorting."""
        mock_client_class.return_value = Mock()

        # Mock TuningType and columns
        with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
            mock_tuning_type.SORTING = "sorting"
            mock_tuning_type.CLUSTERING = "clustering"
            mock_tuning_type.PARTITIONING = "partitioning"

            # Mock table tuning object
            mock_tuning = Mock()
            mock_tuning.table_name = "test_table"
            mock_tuning.partitioning = None
            mock_tuning.clustering = None
            mock_tuning.sorting = [Mock()]  # Has sorting configuration
            mock_tuning.distribution = None

            # Mock column object
            mock_column = Mock()
            mock_column.name = "sort_col"
            mock_column.order = 1

            mock_tuning.get_columns_by_type.return_value = [mock_column]

            adapter = ClickHouseAdapter(deployment_mode="server")

            clause = adapter.generate_tuning_clause(mock_tuning)

            assert "ORDER BY" in clause
            assert "sort_col" in clause

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_generate_tuning_clause_none(self, mock_client_class):
        """Test generating tuning clause with None input."""
        mock_client_class.return_value = Mock()

        adapter = ClickHouseAdapter(deployment_mode="server")

        clause = adapter.generate_tuning_clause(None)
        assert clause == ""

    @patch("benchbox.platforms.clickhouse.setup.ClickHouseClient")
    def test_generate_tuning_clause_empty(self, mock_client_class):
        """Test generating tuning clause with empty configuration."""
        mock_client_class.return_value = Mock()

        # Mock table tuning object with no tuning configurations
        mock_tuning = Mock()
        mock_tuning.table_name = "test_table"
        mock_tuning.partitioning = None
        mock_tuning.clustering = None
        mock_tuning.sorting = None
        mock_tuning.distribution = None

        adapter = ClickHouseAdapter(deployment_mode="server")

        clause = adapter.generate_tuning_clause(mock_tuning)
        assert clause == ""
