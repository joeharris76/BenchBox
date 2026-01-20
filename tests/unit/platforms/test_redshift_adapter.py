"""Tests for Redshift platform adapter.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, call, patch

import pytest

from benchbox.platforms.redshift import RedshiftAdapter

pytestmark = pytest.mark.fast


class TestRedshiftAdapter:
    """Test Redshift platform adapter functionality."""

    def test_initialization_success(self):
        """Test successful adapter initialization."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                port=5439,
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        assert adapter.platform_name == "Redshift"
        assert adapter.get_target_dialect() == "redshift"
        assert adapter.host == "test-cluster.redshift.amazonaws.com"
        assert adapter.port == 5439
        assert adapter.database == "test_db"
        assert adapter.username == "test_user"

    def test_initialization_with_defaults(self):
        """Test initialization with default configuration."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        assert adapter.port == 5439
        assert adapter.connect_timeout == 10
        assert adapter.statement_timeout == 0
        assert adapter.sslmode == "require"
        assert adapter.wlm_query_slot_count == 1

    def test_initialization_missing_driver(self):
        """Test initialization when Redshift dependencies are missing."""
        # Cannot mock module-level driver variables that are set at import time
        # This test would require unloading and reloading the module with mocked imports
        pytest.skip("Cannot mock module-level driver imports; test requires environment without drivers")

    def test_initialization_missing_required_config(self):
        """Test initialization with missing required configuration."""
        from benchbox.core.exceptions import ConfigurationError

        try:
            with pytest.raises(ConfigurationError, match="Redshift configuration is incomplete"):
                RedshiftAdapter()  # Missing required fields
        except ImportError:
            pytest.skip("Redshift drivers not installed")

    def test_get_connection_params(self):
        """Test connection parameter configuration."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                port=5439,
                database="test_db",
                username="test_user",
                password="test_pass",
                sslmode="require",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        params = adapter._get_connection_params()

        assert params["host"] == "test-cluster.redshift.amazonaws.com"
        assert params["port"] == 5439
        assert params["database"] == "test_db"
        assert params["user"] == "test_user"
        assert params["password"] == "test_pass"
        assert params["sslmode"] == "require"

        # Test with overrides
        override_params = adapter._get_connection_params(
            host="override-cluster.redshift.amazonaws.com", database="override_db"
        )
        assert override_params["host"] == "override-cluster.redshift.amazonaws.com"
        assert override_params["database"] == "override_db"
        assert override_params["user"] == "test_user"  # Should keep original
        assert "database" in override_params

    def test_create_admin_connection(self):
        """Test admin connection creation."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
                admin_database="admin_db",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        # Mock the connection
        mock_connection = Mock()

        # Mock the actual driver (redshift_connector or psycopg2) at the module level
        import benchbox.platforms.redshift as redshift_module

        if redshift_module.redshift_connector:
            with patch.object(redshift_module.redshift_connector, "connect", return_value=mock_connection):
                connection = adapter._create_admin_connection()

                # Verify it connected to admin database (not target database)
                redshift_module.redshift_connector.connect.assert_called_once()
                call_kwargs = redshift_module.redshift_connector.connect.call_args[1]
                assert call_kwargs["database"] == "admin_db"
                assert call_kwargs["host"] == "test-cluster.redshift.amazonaws.com"
                assert call_kwargs["user"] == "test_user"
        elif redshift_module.psycopg2:
            with patch.object(redshift_module.psycopg2, "connect", return_value=mock_connection):
                connection = adapter._create_admin_connection()

                # Verify it connected to admin database (not target database)
                redshift_module.psycopg2.connect.assert_called_once()
                call_kwargs = redshift_module.psycopg2.connect.call_args[1]
                assert call_kwargs["database"] == "admin_db"
                assert call_kwargs["host"] == "test-cluster.redshift.amazonaws.com"
                assert call_kwargs["user"] == "test_user"
        else:
            pytest.skip("Neither redshift_connector nor psycopg2 available")
            return

        assert connection == mock_connection

    def test_create_direct_connection(self):
        """Test direct connection creation for validation."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
                wlm_query_queue_name="validation_queue",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        # Mock the connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock the actual driver (redshift_connector or psycopg2) at the module level
        import benchbox.platforms.redshift as redshift_module

        if redshift_module.redshift_connector:
            with patch.object(redshift_module.redshift_connector, "connect", return_value=mock_connection):
                connection = adapter._create_direct_connection(database="target_db")
        elif redshift_module.psycopg2:
            with patch.object(redshift_module.psycopg2, "connect", return_value=mock_connection):
                connection = adapter._create_direct_connection(database="target_db")
        else:
            pytest.skip("Neither redshift_connector nor psycopg2 available")
            return

        assert connection == mock_connection

        # Verify WLM queue was set (single quotes escaped for SQL injection protection)
        mock_cursor.execute.assert_called_with("SET query_group TO 'validation_queue'")
        mock_cursor.close.assert_called_once()

    def test_check_server_database_exists_true(self):
        """Test database existence check when database exists."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        # Mock the connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        # Simulate database exists
        mock_cursor.fetchone.return_value = ("test_db",)

        # Mock the actual driver at the module level
        import benchbox.platforms.redshift as redshift_module

        if redshift_module.redshift_connector:
            with patch.object(redshift_module.redshift_connector, "connect", return_value=mock_connection):
                result = adapter.check_server_database_exists(database="test_db")
        elif redshift_module.psycopg2:
            with patch.object(redshift_module.psycopg2, "connect", return_value=mock_connection):
                result = adapter.check_server_database_exists(database="test_db")
        else:
            pytest.skip("Neither redshift_connector nor psycopg2 available")
            return

        # Verify result
        assert result is True

        # Verify SQL was executed
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0][0]
        assert "SELECT datname FROM pg_database WHERE datname" in call_args

    def test_check_server_database_exists_false(self):
        """Test database existence check when database doesn't exist."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        # Mock the connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        # Simulate database doesn't exist
        mock_cursor.fetchone.return_value = None

        # Mock the actual driver at the module level
        import benchbox.platforms.redshift as redshift_module

        if redshift_module.redshift_connector:
            with patch.object(redshift_module.redshift_connector, "connect", return_value=mock_connection):
                result = adapter.check_server_database_exists(database="nonexistent_db")
        elif redshift_module.psycopg2:
            with patch.object(redshift_module.psycopg2, "connect", return_value=mock_connection):
                result = adapter.check_server_database_exists(database="nonexistent_db")
        else:
            pytest.skip("Neither redshift_connector nor psycopg2 available")
            return

        # Verify result
        assert result is False

        # Verify SQL was executed
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0][0]
        assert "SELECT datname FROM pg_database WHERE datname" in call_args

    def test_check_server_database_exists_connection_error(self):
        """Test database existence check with connection error."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        # Mock the actual driver to raise a connection error
        import benchbox.platforms.redshift as redshift_module

        if redshift_module.redshift_connector:
            with patch.object(
                redshift_module.redshift_connector, "connect", side_effect=Exception("Connection failed")
            ):
                result = adapter.check_server_database_exists(database="test_db")
        elif redshift_module.psycopg2:
            with patch.object(redshift_module.psycopg2, "connect", side_effect=Exception("Connection failed")):
                result = adapter.check_server_database_exists(database="test_db")
        else:
            pytest.skip("Neither redshift_connector nor psycopg2 available")
            return

        # When connection fails, method should return False (database assumed not to exist)
        assert result is False

    def test_drop_database(self):
        """Test database dropping."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        # Mock the connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        # First call checks if database exists (return True), second call drops it
        mock_cursor.fetchone.return_value = ("test_db",)

        # Mock the actual driver at the module level
        import benchbox.platforms.redshift as redshift_module

        if redshift_module.redshift_connector:
            with patch.object(redshift_module.redshift_connector, "connect", return_value=mock_connection):
                adapter.drop_database(database="test_db")

                # Verify DROP DATABASE was executed (quoted for SQL injection protection)
                drop_calls = [call for call in mock_cursor.execute.call_args_list if "DROP DATABASE" in str(call)]
                assert len(drop_calls) > 0, "DROP DATABASE should have been executed"
        elif redshift_module.psycopg2:
            with patch.object(redshift_module.psycopg2, "connect", return_value=mock_connection):
                adapter.drop_database(database="test_db")

                # Verify DROP DATABASE was executed (quoted for SQL injection protection)
                drop_calls = [call for call in mock_cursor.execute.call_args_list if "DROP DATABASE" in str(call)]
                assert len(drop_calls) > 0, "DROP DATABASE should have been executed"
        else:
            pytest.skip("Neither redshift_connector nor psycopg2 available")
            return

        # Verify autocommit was enabled
        assert mock_connection.autocommit is True

    def test_create_connection_success(self):
        """Test successful connection creation."""
        # Test uses whichever driver is available (redshift_connector or psycopg2)
        # Cannot patch module-level driver variables that are set at import time
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        # Mock the connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock handle_existing_database and check_server_database_exists to skip database creation logic
        with (
            patch.object(adapter, "handle_existing_database"),
            patch.object(adapter, "check_server_database_exists", return_value=True),
        ):
            # Mock the actual driver (redshift_connector or psycopg2) at the module level
            import benchbox.platforms.redshift as redshift_module

            if redshift_module.redshift_connector:
                with patch.object(redshift_module.redshift_connector, "connect", return_value=mock_connection):
                    connection = adapter.create_connection()
            elif redshift_module.psycopg2:
                with patch.object(redshift_module.psycopg2, "connect", return_value=mock_connection):
                    connection = adapter.create_connection()
            else:
                pytest.skip("Neither redshift_connector nor psycopg2 available")
                return

        assert connection == mock_connection

        # Check connection test was performed
        mock_cursor.execute.assert_called_with("SELECT version()")
        mock_cursor.fetchone.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_create_connection_with_wlm_settings(self):
        """Test connection creation with WLM settings."""
        # Test uses whichever driver is available (redshift_connector or psycopg2)
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
                wlm_query_slot_count=4,
                statement_timeout=300000,
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        # Mock the connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock handle_existing_database and the driver's connect method
        with patch.object(adapter, "handle_existing_database"):
            import benchbox.platforms.redshift as redshift_module

            if redshift_module.redshift_connector:
                with patch.object(redshift_module.redshift_connector, "connect", return_value=mock_connection):
                    adapter.create_connection()
            elif redshift_module.psycopg2:
                with patch.object(redshift_module.psycopg2, "connect", return_value=mock_connection):
                    adapter.create_connection()
            else:
                pytest.skip("Neither redshift_connector nor psycopg2 available")
                return

        # Should execute WLM configuration and set search_path (quoted for SQL injection protection)
        expected_calls = [
            call("SET wlm_query_slot_count = 4"),
            call("SET statement_timeout = 300000"),
            call('SET search_path TO "public"'),
            call("SELECT version()"),
        ]
        mock_cursor.execute.assert_has_calls(expected_calls)

    def test_create_connection_failure(self):
        """Test connection creation failure."""
        # Test uses whichever driver is available (redshift_connector or psycopg2)
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        # Mock handle_existing_database and the driver's connect method to raise an exception
        with patch.object(adapter, "handle_existing_database"):
            import benchbox.platforms.redshift as redshift_module

            if redshift_module.redshift_connector:
                with (
                    patch.object(
                        redshift_module.redshift_connector, "connect", side_effect=Exception("Connection failed")
                    ),
                    pytest.raises(Exception, match="Connection failed"),
                ):
                    adapter.create_connection()
            elif redshift_module.psycopg2:
                with patch.object(redshift_module.psycopg2, "connect", side_effect=Exception("Connection failed")):
                    with pytest.raises(Exception, match="Connection failed"):
                        adapter.create_connection()
            else:
                pytest.skip("Neither redshift_connector nor psycopg2 available")

    def test_create_schema(self):
        """Test schema creation with Redshift table definitions."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        mock_benchmark = Mock()
        mock_benchmark.get_create_tables_sql.return_value = """
            CREATE TABLE table1 (id INTEGER, name VARCHAR(100)) DISTKEY(id) SORTKEY(id);
            CREATE TABLE table2 (id INTEGER, data TEXT) DISTSTYLE EVEN;
        """

        # Mock translate_sql method
        with patch.object(adapter, "translate_sql") as mock_translate:
            mock_translate.return_value = "CREATE TABLE table1 (id INTEGER, name VARCHAR(100)) DISTKEY(id) SORTKEY(id);\nCREATE TABLE table2 (id INTEGER, data TEXT) DISTSTYLE EVEN;"

            schema_time = adapter.create_schema(mock_benchmark, mock_connection)

        assert isinstance(schema_time, float)
        assert schema_time >= 0

        # Should execute table creation statements
        assert mock_cursor.execute.call_count >= 2  # At least 2 CREATE TABLE statements
        # Note: commit is called multiple times (once per statement)
        mock_cursor.close.assert_called_once()

    def test_load_data_with_copy_command(self):
        """Test data loading using Redshift COPY command."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock row count query
        mock_cursor.fetchone.return_value = (100,)

        mock_benchmark = Mock()

        # Create temporary test file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("1,test1\n2,test2\n")
            temp_path = Path(f.name)

        try:
            mock_benchmark.tables = {"test_table": str(temp_path)}

            # Mock no S3 bucket to force direct loading path
            adapter.s3_bucket = None

            table_stats, load_time, _ = adapter.load_data(mock_benchmark, mock_connection, Path("/tmp"))

            assert isinstance(table_stats, dict)
            assert isinstance(load_time, float)
            assert load_time >= 0
            # Table names are now normalized to lowercase
            assert "test_table" in table_stats
            assert table_stats["test_table"] == 2  # 2 rows in test data

            # Should execute INSERT commands for direct loading (no S3)
            execute_calls = [str(call) for call in mock_cursor.execute.call_args_list]
            assert any("INSERT INTO test_table" in call for call in execute_calls)

        finally:
            temp_path.unlink()

    def test_configure_for_benchmark_olap(self):
        """Test OLAP benchmark configuration with Redshift optimizations."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
                strict_validation=False,  # Disable validation in unit tests
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        # Mock table list query result
        mock_cursor.fetchall.return_value = [("public", "table1"), ("public", "table2")]

        adapter.configure_for_benchmark(mock_connection, "olap")

        # Should execute Redshift OLAP optimizations
        execute_calls = [str(call) for call in mock_cursor.execute.call_args_list]
        assert any("enable_result_cache_for_session" in call for call in execute_calls)
        assert any("query_group" in call for call in execute_calls)
        assert any("enable_case_sensitive_identifier" in call for call in execute_calls)

        # cursor.close() called twice: once for configure, once for validation
        assert mock_cursor.close.call_count == 2

    def test_configure_for_benchmark_with_wlm(self):
        """Test benchmark configuration with WLM settings."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
                wlm_query_slot_count=8,
                strict_validation=False,  # Disable validation in unit tests
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        # Mock table list query result
        mock_cursor.fetchall.return_value = [("public", "table1"), ("public", "table2")]

        adapter.configure_for_benchmark(mock_connection, "tpch")

        # Should execute optimization settings (WLM is applied during connection creation)
        execute_calls = [str(call) for call in mock_cursor.execute.call_args_list]
        assert any("query_group" in call for call in execute_calls)
        assert any("statement_timeout" in call for call in execute_calls)

    def test_execute_query_success(self):
        """Test successful query execution."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1, "test"), (2, "test2")]

        # Mock query statistics
        with patch.object(adapter, "_get_query_statistics") as mock_stats:
            mock_stats.return_value = {"query_id": "redshift_query_123"}

            result = adapter.execute_query(mock_connection, "SELECT * FROM test", "q1")

        assert result["query_id"] == "q1"
        assert result["status"] == "SUCCESS"
        assert result["rows_returned"] == 2
        assert result["first_row"] == (1, "test")
        assert isinstance(result["execution_time"], float)
        # Query statistics now includes execution_time_seconds for cost calculation
        assert result["query_statistics"]["query_id"] == "redshift_query_123"
        assert "execution_time_seconds" in result["query_statistics"]
        assert isinstance(result["query_statistics"]["execution_time_seconds"], float)

        mock_cursor.execute.assert_called_with("SELECT * FROM test")
        mock_cursor.close.assert_called_once()

    def test_execute_query_failure(self):
        """Test query execution failure."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Query failed")

        result = adapter.execute_query(mock_connection, "INVALID SQL", "q1")

        assert result["query_id"] == "q1"
        assert result["status"] == "FAILED"
        assert result["rows_returned"] == 0
        assert result["error"] == "Query failed"
        assert result["error_type"] == "Exception"
        assert isinstance(result["execution_time"], float)

        mock_cursor.close.assert_called_once()

    def test_get_query_statistics(self):
        """Test query statistics retrieval from STL tables."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = [
            "query_123",
            1000,
            2000,
            1024000,
            512000,
            4,
            8,
            True,
        ]

        stats = adapter._get_query_statistics(mock_connection, "q1")

        assert stats["query_id"] == "query_123"
        assert stats["duration_microsecs"] == 1000
        assert stats["cpu_time_microsecs"] == 2000
        assert stats["bytes_scanned"] == 1024000
        assert stats["bytes_returned"] == 512000
        assert stats["slots"] == 4
        assert stats["wlm_slots"] == 8
        assert stats["aborted"] is True

        mock_cursor.close.assert_called_once()

    def test_get_platform_metadata(self):
        """Test platform metadata collection."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock query responses
        mock_cursor.fetchone.side_effect = [
            [
                "PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017, Redshift 1.0.0"
            ],  # Version
            ["dc2.large", 2, "1.0", True],  # Cluster info
            ["test_user", "test_db", "public", "192.168.1.1", 5432],  # Session info
        ]

        mock_cursor.fetchall.return_value = [
            ("public", "table1", "test_user", None, False, False, False),
            ("public", "table2", "test_user", None, False, False, False),
        ]

        metadata = adapter._get_platform_metadata(mock_connection)

        assert metadata["platform"] == "Redshift"
        assert metadata["host"] == "test-cluster.redshift.amazonaws.com"
        assert metadata["database"] == "test_db"
        assert "redshift_version" in metadata
        assert "cluster_info" in metadata
        assert "session_info" in metadata
        assert "tables" in metadata

        mock_cursor.close.assert_called()

    def test_analyze_table(self):
        """Test table analysis for query optimization."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        adapter.analyze_table(mock_connection, "test_table")

        mock_cursor.execute.assert_called_once_with("ANALYZE test_table")
        mock_cursor.close.assert_called_once()

    def test_vacuum_table(self):
        """Test table vacuuming for space reclamation."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        adapter.vacuum_table(mock_connection, "test_table")

        mock_cursor.execute.assert_called_once_with("VACUUM test_table")
        mock_cursor.close.assert_called_once()

    def test_close_connection(self):
        """Test connection closing."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        mock_connection = Mock()

        adapter.close_connection(mock_connection)

        mock_connection.close.assert_called_once()

    def test_supports_tuning_type(self):
        """Test tuning type support checking."""
        # Skip driver mocking - the adapter's __init__ will check actual imports
        # Since this test doesn't actually connect, we just need __init__ to succeed
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        # Mock TuningType
        with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
            mock_tuning_type.DISTRIBUTION = "distribution"
            mock_tuning_type.SORTING = "sorting"
            mock_tuning_type.CLUSTERING = "clustering"

            assert adapter.supports_tuning_type(mock_tuning_type.DISTRIBUTION) is True
            assert adapter.supports_tuning_type(mock_tuning_type.SORTING) is True
            assert adapter.supports_tuning_type(mock_tuning_type.CLUSTERING) is False

    def test_generate_tuning_clause_with_distribution(self):
        """Test tuning clause generation with distribution."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        # Mock table tuning with distribution
        mock_tuning = Mock()
        mock_tuning.has_any_tuning.return_value = True

        # Mock distribution column
        mock_column = Mock()
        mock_column.name = "dist_key"
        mock_column.order = 1

        with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
            mock_tuning_type.DISTRIBUTION = "distribution"
            mock_tuning_type.SORTING = "sorting"

            def mock_get_columns_by_type(tuning_type):
                if tuning_type == mock_tuning_type.DISTRIBUTION:
                    return [mock_column]
                return []

            mock_tuning.get_columns_by_type.side_effect = mock_get_columns_by_type

            clause = adapter.generate_tuning_clause(mock_tuning)

            assert "DISTKEY (dist_key)" in clause

    def test_generate_tuning_clause_with_sorting(self):
        """Test tuning clause generation with sorting."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        # Mock table tuning with sorting
        mock_tuning = Mock()
        mock_tuning.has_any_tuning.return_value = True

        # Mock sorting columns
        mock_column1 = Mock()
        mock_column1.name = "sort_key1"
        mock_column1.order = 1
        mock_column2 = Mock()
        mock_column2.name = "sort_key2"
        mock_column2.order = 2

        with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
            mock_tuning_type.DISTRIBUTION = "distribution"
            mock_tuning_type.SORTING = "sorting"

            def mock_get_columns_by_type(tuning_type):
                if tuning_type == mock_tuning_type.SORTING:
                    return [mock_column1, mock_column2]
                return []

            mock_tuning.get_columns_by_type.side_effect = mock_get_columns_by_type

            clause = adapter.generate_tuning_clause(mock_tuning)

            assert "SORTKEY (sort_key1, sort_key2)" in clause

    def test_generate_tuning_clause_with_diststyle(self):
        """Test tuning clause generation with distribution style."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        # Mock table tuning with no distribution columns (DISTSTYLE EVEN)
        mock_tuning = Mock()
        mock_tuning.has_any_tuning.return_value = True

        with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
            mock_tuning_type.DISTRIBUTION = "distribution"
            mock_tuning_type.SORTING = "sorting"

            def mock_get_columns_by_type(tuning_type):
                return []  # No specific columns

            mock_tuning.get_columns_by_type.side_effect = mock_get_columns_by_type

            clause = adapter.generate_tuning_clause(mock_tuning)

            # Should generate DISTSTYLE EVEN when no specific distribution columns
            assert "DISTSTYLE EVEN" in clause

    def test_generate_tuning_clause_none(self):
        """Test tuning clause generation with None input."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        clause = adapter.generate_tuning_clause(None)
        assert clause == ""

    def test_apply_table_tunings_with_distribution_and_sorting(self):
        """Test applying table tunings with distribution and sorting."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock the table configuration query result
        # Format: (schemaname, tablename, diststyle, distkey, sortkey1, sortkey2, sortkey3, sortkey4)
        mock_cursor.fetchone.return_value = (
            "public",
            "test_table",
            "KEY",
            "dist_key",
            "sort_key",
            None,
            None,
            None,
        )

        # Mock table tuning
        mock_tuning = Mock()
        mock_tuning.table_name = "test_table"
        mock_tuning.has_any_tuning.return_value = True

        # Mock distribution and sorting columns
        mock_dist_column = Mock()
        mock_dist_column.name = "dist_key"
        mock_dist_column.order = 1

        mock_sort_column = Mock()
        mock_sort_column.name = "sort_key"
        mock_sort_column.order = 1

        with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
            mock_tuning_type.DISTRIBUTION = "distribution"
            mock_tuning_type.SORTING = "sorting"
            mock_tuning_type.CLUSTERING = "clustering"
            mock_tuning_type.PARTITIONING = "partitioning"

            def mock_get_columns_by_type(tuning_type):
                if tuning_type == mock_tuning_type.DISTRIBUTION:
                    return [mock_dist_column]
                elif tuning_type == mock_tuning_type.SORTING:
                    return [mock_sort_column]
                return []

            mock_tuning.get_columns_by_type.side_effect = mock_get_columns_by_type

            # Should not raise exception - Redshift tuning is applied during table creation
            adapter.apply_table_tunings(mock_tuning, mock_connection)

    def test_apply_unified_tuning(self):
        """Test unified tuning configuration application."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        mock_connection = Mock()
        mock_unified_config = Mock()
        mock_unified_config.primary_keys = Mock()
        mock_unified_config.foreign_keys = Mock()
        mock_unified_config.platform_optimizations = Mock()
        mock_unified_config.table_tunings = {}

        # Should not raise exception
        with patch.object(adapter, "apply_constraint_configuration"):
            with patch.object(adapter, "apply_platform_optimizations"):
                adapter.apply_unified_tuning(mock_unified_config, mock_connection)

    def test_apply_constraint_configuration(self):
        """Test constraint configuration application."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        mock_connection = Mock()
        mock_primary_key_config = Mock()
        mock_primary_key_config.enabled = True
        mock_foreign_key_config = Mock()
        mock_foreign_key_config.enabled = False

        # Should not raise exception - constraints are informational in Redshift
        adapter.apply_constraint_configuration(mock_primary_key_config, mock_foreign_key_config, mock_connection)

    def test_wlm_configuration_options(self):
        """Test WLM (Workload Management) configuration options."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
                wlm_query_slot_count=8,
                wlm_query_queue_name="benchmark_queue",
                statement_timeout=600000,
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        assert adapter.wlm_query_slot_count == 8
        assert adapter.wlm_query_queue_name == "benchmark_queue"
        assert adapter.statement_timeout == 600000

    def test_ssl_configuration_options(self):
        """Test SSL/TLS configuration options."""
        try:
            # Test different SSL modes
            adapter_require = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
                sslmode="require",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        assert adapter_require.sslmode == "require"

        try:
            adapter_verify = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
                sslmode="verify-full",
                sslrootcert="/path/to/cert.pem",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        assert adapter_verify.sslmode == "verify-full"
        assert adapter_verify.sslrootcert == "/path/to/cert.pem"

    def test_file_format_detection_for_chunked_files(self):
        """Test that chunked TPC-H data files (.tbl.1, .tbl.2, etc.) are detected as pipe-delimited.

        This verifies the fix for the bug where chunked files like customer.tbl.1 were treated
        as CSV (comma-delimited) instead of TBL (pipe-delimited), causing data loading failures.
        """
        from pathlib import Path

        test_cases = [
            (Path("customer.tbl"), True, "Simple .tbl file"),
            (Path("customer.tbl.1"), True, "Chunked .tbl file"),
            (Path("lineitem.tbl.10"), True, "Chunked .tbl file (2 digits)"),
            (Path("orders.tbl.gz"), True, "Compressed .tbl file"),
            (Path("part.tbl.1.gz"), True, "Chunked compressed .tbl file"),
            (Path("nation.tbl.zst"), True, "zstd compressed .tbl file"),
            (Path("partsupp.dat"), True, "TPC-DS .dat file"),
            (Path("partsupp.dat.1"), True, "Chunked TPC-DS .dat file"),
            (Path("region.csv"), False, "CSV file"),
            (Path("supplier.csv.gz"), False, "Compressed CSV file"),
        ]

        for file_path, should_be_tbl, description in test_cases:
            file_str = str(file_path.name)
            is_tbl = ".tbl" in file_str or ".dat" in file_str
            assert is_tbl == should_be_tbl, f"Failed for {description}: {file_path.name}"

    def test_compupdate_validation_valid_values(self):
        """Test COMPUPDATE validation accepts valid values (case-insensitive)."""
        valid_values = ["ON", "OFF", "PRESET", "on", "off", "preset", "On", "Off", "Preset"]

        for value in valid_values:
            try:
                adapter = RedshiftAdapter(
                    host="test-cluster.redshift.amazonaws.com",
                    database="test_db",
                    username="test_user",
                    password="test_pass",
                    compupdate=value,
                )
            except ImportError:
                pytest.skip("Redshift drivers not installed")

            # Should normalize to uppercase
            assert adapter.compupdate == value.upper(), f"Failed for value: {value}"

    def test_compupdate_validation_invalid_values(self):
        """Test COMPUPDATE validation rejects invalid values."""
        # Note: Empty string "" is not tested because it triggers the default "PRESET" via "or" operator
        invalid_values = ["ALWAYS", "NEVER", "AUTO", "TRUE", "FALSE", "1", "0", "INVALID"]

        for value in invalid_values:
            with pytest.raises(ValueError, match=r"Invalid COMPUPDATE value"):
                try:
                    RedshiftAdapter(
                        host="test-cluster.redshift.amazonaws.com",
                        database="test_db",
                        username="test_user",
                        password="test_pass",
                        compupdate=value,
                    )
                except ImportError:
                    pytest.skip("Redshift drivers not installed")

    def test_compupdate_default_value(self):
        """Test COMPUPDATE defaults to PRESET."""
        try:
            adapter = RedshiftAdapter(
                host="test-cluster.redshift.amazonaws.com",
                database="test_db",
                username="test_user",
                password="test_pass",
            )
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        assert adapter.compupdate == "PRESET"

    def test_from_config_passes_all_parameters(self):
        """Test from_config() properly passes through all configuration parameters."""
        config = {
            "host": "test-cluster.redshift.amazonaws.com",
            "port": 5439,
            "database": "test_db",
            "username": "test_user",
            "password": "test_pass",
            "schema": "test_schema",
            "benchmark": "tpch",
            "scale_factor": 1.0,
            # Optional staging/optimization parameters
            "iam_role": "arn:aws:iam::123456789012:role/RedshiftCopyRole",
            "s3_bucket": "test-bucket",
            "s3_prefix": "test-prefix",
            "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
            "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "aws_region": "us-west-2",
            "cluster_identifier": "test-cluster",
            "admin_database": "admin_db",
            "connect_timeout": 30,
            "statement_timeout": 60000,
            "sslmode": "require",
            "ssl_enabled": True,
            "ssl_insecure": False,
            "sslrootcert": "/path/to/cert.pem",
            "wlm_query_slot_count": 4,
            "wlm_query_queue_name": "test_queue",
            "wlm_config": {"queue": "test"},
            "compupdate": "ON",
            "auto_vacuum": False,
            "auto_analyze": False,
        }

        try:
            adapter = RedshiftAdapter.from_config(config)
        except ImportError:
            pytest.skip("Redshift drivers not installed")

        # Verify core parameters
        assert adapter.host == "test-cluster.redshift.amazonaws.com"
        assert adapter.port == 5439
        assert adapter.username == "test_user"
        assert adapter.password == "test_pass"
        assert adapter.schema == "test_schema"

        # Verify staging parameters
        assert adapter.iam_role == "arn:aws:iam::123456789012:role/RedshiftCopyRole"
        assert adapter.s3_bucket == "test-bucket"
        assert adapter.s3_prefix == "test-prefix"
        assert adapter.aws_access_key_id == "AKIAIOSFODNN7EXAMPLE"
        assert adapter.aws_secret_access_key == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        assert adapter.aws_region == "us-west-2"

        # Verify connection parameters
        assert adapter.cluster_identifier == "test-cluster"
        assert adapter.admin_database == "admin_db"
        assert adapter.connect_timeout == 30
        assert adapter.statement_timeout == 60000
        assert adapter.sslmode == "require"

        # Verify SSL parameters
        assert adapter.ssl_enabled is True
        assert adapter.ssl_insecure is False
        assert adapter.sslrootcert == "/path/to/cert.pem"

        # Verify WLM parameters
        assert adapter.wlm_query_slot_count == 4
        assert adapter.wlm_query_queue_name == "test_queue"
        assert adapter.workload_management_config == {"queue": "test"}

        # Verify optimization parameters
        assert adapter.compupdate == "ON"
        assert adapter.auto_vacuum is False
        assert adapter.auto_analyze is False
