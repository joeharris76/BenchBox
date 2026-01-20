"""Tests for Trino platform adapter.

Tests the TrinoAdapter for Trino distributed SQL query engine support.

Note: This adapter supports Trino only, NOT PrestoDB (Meta's Presto fork).
For AWS managed Presto/Trino workloads, use the AthenaAdapter.
For Starburst Enterprise, this adapter is fully compatible.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from benchbox.platforms.trino import TrinoAdapter

pytestmark = pytest.mark.fast


class TestTrinoAdapter:
    """Test Trino platform adapter functionality."""

    def test_initialization_success(self):
        """Test successful adapter initialization."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                port=8080,
                catalog="hive",
                schema="default",
                username="trino",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        assert adapter.platform_name == "Trino"
        assert adapter.get_target_dialect() == "trino"
        assert adapter.host == "trino-coordinator.example.com"
        assert adapter.port == 8080
        assert adapter.catalog == "hive"
        assert adapter.schema == "default"
        assert adapter.username == "trino"

    def test_initialization_with_defaults(self):
        """Test initialization with default configuration."""
        try:
            adapter = TrinoAdapter(catalog="hive")  # catalog is required
        except ImportError:
            pytest.skip("Trino drivers not installed")

        assert adapter.host == "localhost"
        assert adapter.port == 8080
        assert adapter.catalog == "hive"
        assert adapter.schema == "default"
        assert adapter.username == "trino"
        assert adapter.http_scheme == "http"
        assert adapter.table_format == "memory"

    def test_initialization_without_catalog(self):
        """Test initialization without catalog (validation happens on connection)."""
        try:
            adapter = TrinoAdapter()
        except ImportError:
            pytest.skip("Trino drivers not installed")

        assert adapter.catalog is None
        assert adapter.host == "localhost"
        assert adapter.port == 8080

    def test_initialization_with_password_enables_https(self):
        """Test that providing a password auto-enables HTTPS."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                username="user",
                password="secret",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        assert adapter.http_scheme == "https"

    def test_initialization_explicit_http_scheme(self):
        """Test explicit HTTP scheme configuration."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                username="user",
                password="secret",
                http_scheme="http",  # Override auto-detection
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        assert adapter.http_scheme == "http"

    def test_get_connection_params(self):
        """Test connection parameter configuration."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                port=8443,
                catalog="iceberg",
                schema="benchmark",
                username="test_user",
                password="test_pass",
                http_scheme="https",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        params = adapter._get_connection_params()

        assert params["host"] == "trino-coordinator.example.com"
        assert params["port"] == 8443
        assert params["catalog"] == "iceberg"
        assert params["schema"] == "benchmark"
        assert params["user"] == "test_user"
        assert params["http_scheme"] == "https"
        # Password should be used for auth object, not passed directly
        assert "auth" in params

    def test_get_connection_params_no_auth(self):
        """Test connection parameters without authentication."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                username="user",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        params = adapter._get_connection_params()

        assert params["user"] == "user"
        assert "auth" not in params

    def test_check_server_database_exists_true(self):
        """Test schema existence check when schema exists."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                catalog="memory",
                schema="default",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        # Mock the connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ("default",)

        import benchbox.platforms.trino as trino_module

        with patch.object(trino_module.trino.dbapi, "connect", return_value=mock_connection):
            result = adapter.check_server_database_exists(schema="default", catalog="memory")

        assert result is True

    def test_check_server_database_exists_false(self):
        """Test schema existence check when schema doesn't exist."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                catalog="memory",
                schema="default",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        # Mock the connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None

        import benchbox.platforms.trino as trino_module

        with patch.object(trino_module.trino.dbapi, "connect", return_value=mock_connection):
            result = adapter.check_server_database_exists(schema="nonexistent", catalog="memory")

        assert result is False

    def test_check_server_database_exists_connection_error(self):
        """Test schema existence check with connection error."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                catalog="hive",  # catalog is required
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        import benchbox.platforms.trino as trino_module

        with patch.object(trino_module.trino.dbapi, "connect", side_effect=Exception("Connection failed")):
            result = adapter.check_server_database_exists(schema="default", catalog="hive")

        # When connection fails, method should return False
        assert result is False

    def test_validate_catalog_raises_when_server_unreachable(self):
        """Catalog validation should raise ConfigurationError when server unreachable and no catalog."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
            )  # No catalog provided
        except ImportError:
            pytest.skip("Trino drivers not installed")

        from benchbox.core.exceptions import ConfigurationError

        # Mock _get_available_catalogs to return empty list (server query fails)
        with patch.object(adapter, "_get_available_catalogs", return_value=[]):
            with pytest.raises(ConfigurationError) as excinfo:
                adapter._validate_catalog_exists(None)

        assert "server is unreachable" in str(excinfo.value)

    def test_auto_select_catalog_prefers_hive(self):
        """Auto-selection should prefer hive over other catalogs."""
        try:
            adapter = TrinoAdapter(host="trino-coordinator.example.com")
        except ImportError:
            pytest.skip("Trino drivers not installed")

        with patch.object(adapter, "_get_available_catalogs", return_value=["memory", "hive", "system"]):
            selected = adapter._auto_select_catalog()

        assert selected == "hive"

    def test_auto_select_catalog_fallback_to_memory(self):
        """Auto-selection should fall back to memory when hive not available."""
        try:
            adapter = TrinoAdapter(host="trino-coordinator.example.com")
        except ImportError:
            pytest.skip("Trino drivers not installed")

        with patch.object(adapter, "_get_available_catalogs", return_value=["memory", "system"]):
            selected = adapter._auto_select_catalog()

        assert selected == "memory"

    def test_auto_select_catalog_uses_first_available(self):
        """Auto-selection should use first usable catalog when no preferred catalogs."""
        try:
            adapter = TrinoAdapter(host="trino-coordinator.example.com")
        except ImportError:
            pytest.skip("Trino drivers not installed")

        # system is filtered out, custom_catalog is used
        with patch.object(adapter, "_get_available_catalogs", return_value=["custom_catalog", "system"]):
            selected = adapter._auto_select_catalog()

        assert selected == "custom_catalog"

    def test_auto_select_catalog_only_system_catalogs(self):
        """Auto-selection should return None when only system catalogs exist."""
        try:
            adapter = TrinoAdapter(host="trino-coordinator.example.com")
        except ImportError:
            pytest.skip("Trino drivers not installed")

        # Only jmx and system - both are system-only and unusable
        with patch.object(adapter, "_get_available_catalogs", return_value=["jmx", "system"]):
            selected = adapter._auto_select_catalog()

        assert selected is None

    def test_auto_select_catalog_server_unreachable(self):
        """Auto-selection should return None when server unreachable."""
        try:
            adapter = TrinoAdapter(host="trino-coordinator.example.com")
        except ImportError:
            pytest.skip("Trino drivers not installed")

        with patch.object(adapter, "_get_available_catalogs", return_value=[]):
            selected = adapter._auto_select_catalog()

        assert selected is None

    def test_validation_auto_selects_when_none(self):
        """Validation should auto-select catalog when None provided."""
        try:
            adapter = TrinoAdapter(host="trino-coordinator.example.com")
        except ImportError:
            pytest.skip("Trino drivers not installed")

        with patch.object(adapter, "_get_available_catalogs", return_value=["hive", "memory", "system"]):
            validated = adapter._validate_catalog_exists(None)

        assert validated == "hive"
        assert adapter._catalog_was_auto_selected is True

    def test_validation_raises_when_server_unreachable(self):
        """Validation should raise ConfigurationError when server unreachable and no catalog."""
        from benchbox.core.exceptions import ConfigurationError

        try:
            adapter = TrinoAdapter(host="trino-coordinator.example.com")
        except ImportError:
            pytest.skip("Trino drivers not installed")

        with patch.object(adapter, "_get_available_catalogs", return_value=[]):
            with pytest.raises(ConfigurationError) as exc_info:
                adapter._validate_catalog_exists(None)

        assert "server is unreachable" in str(exc_info.value)

    def test_validation_raises_when_only_system_catalogs(self):
        """Validation should raise ConfigurationError when only system catalogs exist."""
        from benchbox.core.exceptions import ConfigurationError

        try:
            adapter = TrinoAdapter(host="trino-coordinator.example.com")
        except ImportError:
            pytest.skip("Trino drivers not installed")

        # Only jmx and system catalogs exist
        with patch.object(adapter, "_get_available_catalogs", return_value=["jmx", "system"]):
            with pytest.raises(ConfigurationError) as exc_info:
                adapter._validate_catalog_exists(None)

        assert "No usable data catalogs found" in str(exc_info.value)
        assert "jmx, system" in str(exc_info.value)

    def test_drop_database(self):
        """Test schema dropping."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                catalog="memory",
                schema="test_schema",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        # Mock the connection and cursor
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        # First call checks if schema exists
        mock_cursor.fetchone.return_value = ("test_schema",)

        import benchbox.platforms.trino as trino_module

        with patch.object(trino_module.trino.dbapi, "connect", return_value=mock_connection):
            adapter.drop_database(schema="test_schema", catalog="memory")

        # Verify DROP SCHEMA was executed
        drop_calls = [call for call in mock_cursor.execute.call_args_list if "DROP SCHEMA" in str(call)]
        assert len(drop_calls) > 0, "DROP SCHEMA should have been executed"

    def test_create_connection_success(self):
        """Test successful connection creation."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                catalog="memory",
                schema="default",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (1,)

        with (
            patch.object(adapter, "handle_existing_database"),
            patch.object(adapter, "check_server_database_exists", return_value=True),
        ):
            import benchbox.platforms.trino as trino_module

            with patch.object(trino_module.trino.dbapi, "connect", return_value=mock_connection):
                connection = adapter.create_connection()

        assert connection == mock_connection
        mock_cursor.execute.assert_called_with("SELECT 1")
        mock_cursor.fetchone.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_create_connection_local_refused_raises_friendly_error(self):
        """A local connection failure should surface a helpful error message."""
        try:
            adapter = TrinoAdapter(host="localhost", port=8080)
        except ImportError:
            pytest.skip("Trino drivers not installed")

        import benchbox.platforms.trino as trino_module

        connection_error = Exception(
            "HTTPConnectionPool(host='localhost', port=8080): Max retries exceeded with url: /v1/statement "
            "(Caused by NewConnectionError: Failed to establish a new connection: [Errno 61] Connection refused)"
        )

        with (
            patch.object(adapter, "handle_existing_database"),
            patch.object(adapter, "check_server_database_exists", return_value=True),
            patch.object(trino_module.trino.dbapi, "connect", side_effect=connection_error),
        ):
            with pytest.raises(RuntimeError) as excinfo:
                adapter.create_connection()

        assert "Trino is not running on localhost:8080" in str(excinfo.value)

    def test_create_connection_remote_refused_re_raises_original_error(self):
        """Remote host failures should not be replaced with local-only guidance."""
        try:
            adapter = TrinoAdapter(host="trino-coordinator.example.com", port=8080)
        except ImportError:
            pytest.skip("Trino drivers not installed")

        import benchbox.platforms.trino as trino_module

        connection_error = Exception(
            "HTTPConnectionPool(host='trino-coordinator.example.com', port=8080): Max retries exceeded with url: /v1/statement "
            "(Caused by NewConnectionError: Failed to establish a new connection: [Errno 61] Connection refused)"
        )

        with (
            patch.object(adapter, "handle_existing_database"),
            patch.object(adapter, "check_server_database_exists", return_value=True),
            patch.object(trino_module.trino.dbapi, "connect", side_effect=connection_error),
        ):
            with pytest.raises(Exception) as excinfo:
                adapter.create_connection()

        assert "trino-coordinator.example.com" in str(excinfo.value)

    def test_create_connection_creates_schema(self):
        """Test connection creation with schema creation."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                catalog="memory",
                schema="new_schema",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (1,)

        with (
            patch.object(adapter, "handle_existing_database"),
            # First call returns False (schema doesn't exist), second returns True (just created)
            patch.object(adapter, "check_server_database_exists", side_effect=[False, True]),
        ):
            import benchbox.platforms.trino as trino_module

            with patch.object(trino_module.trino.dbapi, "connect", return_value=mock_connection):
                adapter.create_connection()

        # Verify CREATE SCHEMA was executed
        execute_calls = [str(call) for call in mock_cursor.execute.call_args_list]
        assert any("CREATE SCHEMA" in call for call in execute_calls)

    def test_create_schema(self):
        """Test schema creation with Trino table definitions."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                catalog="memory",
                schema="default",
                table_format="memory",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        mock_benchmark = Mock()
        mock_benchmark.get_create_tables_sql.return_value = """
            CREATE TABLE table1 (id INTEGER, name VARCHAR(100));
            CREATE TABLE table2 (id INTEGER, data VARCHAR(255));
        """

        with patch.object(adapter, "translate_sql") as mock_translate:
            mock_translate.return_value = "CREATE TABLE table1 (id INTEGER, name VARCHAR(100));\nCREATE TABLE table2 (id INTEGER, data VARCHAR(255));"

            schema_time = adapter.create_schema(mock_benchmark, mock_connection)

        assert isinstance(schema_time, float)
        assert schema_time >= 0
        assert mock_cursor.execute.call_count >= 2
        mock_cursor.close.assert_called_once()

    def test_load_data_with_insert(self):
        """Test data loading using INSERT statements."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                catalog="memory",
                schema="default",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        mock_benchmark = Mock()

        # Create temporary test file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("1,test1\n2,test2\n")
            temp_path = Path(f.name)

        try:
            mock_benchmark.tables = {"test_table": str(temp_path)}

            table_stats, load_time, _ = adapter.load_data(mock_benchmark, mock_connection, Path("/tmp"))

            assert isinstance(table_stats, dict)
            assert isinstance(load_time, float)
            assert load_time >= 0
            assert "test_table" in table_stats
            assert table_stats["test_table"] == 2  # 2 rows in test data

            # Should execute INSERT commands
            execute_calls = [str(call) for call in mock_cursor.execute.call_args_list]
            assert any("INSERT INTO test_table" in call for call in execute_calls)

        finally:
            temp_path.unlink()

    def test_load_data_with_tbl_files(self):
        """Test data loading with pipe-delimited .tbl files."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                catalog="memory",
                schema="default",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        mock_benchmark = Mock()

        # Create temporary test file with pipe delimiter
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tbl", delete=False) as f:
            f.write("1|test1|\n2|test2|\n")
            temp_path = Path(f.name)

        try:
            mock_benchmark.tables = {"customer": str(temp_path)}

            table_stats, load_time, _ = adapter.load_data(mock_benchmark, mock_connection, Path("/tmp"))

            assert "customer" in table_stats
            assert table_stats["customer"] == 2

        finally:
            temp_path.unlink()

    def test_configure_for_benchmark_olap(self):
        """Test OLAP benchmark configuration with Trino optimizations."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        adapter.configure_for_benchmark(mock_connection, "olap")

        # Should execute Trino OLAP optimizations
        execute_calls = [str(call) for call in mock_cursor.execute.call_args_list]
        assert any("SET SESSION" in call for call in execute_calls)
        mock_cursor.close.assert_called_once()

    def test_execute_query_success(self):
        """Test successful query execution."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1, "test"), (2, "test2")]

        result = adapter.execute_query(mock_connection, "SELECT * FROM test", "q1")

        assert result["query_id"] == "q1"
        assert result["status"] == "SUCCESS"
        assert result["rows_returned"] == 2
        assert result["first_row"] == (1, "test")
        assert isinstance(result["execution_time"], float)

        mock_cursor.execute.assert_called_with("SELECT * FROM test")
        mock_cursor.close.assert_called_once()

    def test_execute_query_failure(self):
        """Test query execution failure."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

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

        mock_cursor.close.assert_called_once()

    def test_get_query_plan(self):
        """Test query plan retrieval."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ("Fragment 0 [SINGLE]",),
            ("    Output partitioning: SINGLE []",),
            ("    TableScan[memory:test_table]",),
        ]

        plan = adapter.get_query_plan(mock_connection, "SELECT * FROM test_table")

        assert "Fragment 0 [SINGLE]" in plan
        assert "TableScan" in plan
        mock_cursor.close.assert_called_once()

    def test_close_connection(self):
        """Test connection closing."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_connection = Mock()

        adapter.close_connection(mock_connection)

        mock_connection.close.assert_called_once()

    def test_get_platform_info(self):
        """Test platform information retrieval."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                port=8080,
                catalog="memory",
                schema="default",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock query responses
        mock_cursor.fetchone.side_effect = [
            ("478",),  # Trino version
            (3,),  # Node count
        ]
        mock_cursor.fetchall.return_value = [
            ("memory",),
            ("system",),
        ]

        platform_info = adapter.get_platform_info(mock_connection)

        assert platform_info["platform_type"] == "trino"
        assert platform_info["platform_name"] == "Trino"
        assert platform_info["host"] == "trino-coordinator.example.com"
        assert platform_info["port"] == 8080
        assert platform_info["configuration"]["catalog"] == "memory"

    def test_supports_tuning_type(self):
        """Test tuning type support checking."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
            mock_tuning_type.PARTITIONING = "partitioning"
            mock_tuning_type.SORTING = "sorting"
            mock_tuning_type.DISTRIBUTION = "distribution"

            assert adapter.supports_tuning_type(mock_tuning_type.PARTITIONING) is True
            assert adapter.supports_tuning_type(mock_tuning_type.SORTING) is True
            # Trino doesn't support distribution keys like Redshift
            assert adapter.supports_tuning_type(mock_tuning_type.DISTRIBUTION) is False

    def test_generate_tuning_clause_iceberg(self):
        """Test tuning clause generation for Iceberg tables."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                table_format="iceberg",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_tuning = Mock()
        mock_tuning.has_any_tuning.return_value = True

        # Mock partition column
        mock_partition_col = Mock()
        mock_partition_col.name = "event_date"
        mock_partition_col.order = 1

        # Mock sort column
        mock_sort_col = Mock()
        mock_sort_col.name = "event_time"
        mock_sort_col.order = 1

        with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
            mock_tuning_type.PARTITIONING = "partitioning"
            mock_tuning_type.SORTING = "sorting"

            def mock_get_columns_by_type(tuning_type):
                if tuning_type == mock_tuning_type.PARTITIONING:
                    return [mock_partition_col]
                elif tuning_type == mock_tuning_type.SORTING:
                    return [mock_sort_col]
                return []

            mock_tuning.get_columns_by_type.side_effect = mock_get_columns_by_type

            clause = adapter.generate_tuning_clause(mock_tuning)

            assert "WITH" in clause
            assert "partitioning" in clause
            assert "event_date" in clause
            assert "sorted_by" in clause
            assert "event_time" in clause

    def test_generate_tuning_clause_none(self):
        """Test tuning clause generation with None input."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        clause = adapter.generate_tuning_clause(None)
        assert clause == ""

    def test_apply_constraint_configuration(self):
        """Test constraint configuration application."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_connection = Mock()
        mock_primary_key_config = Mock()
        mock_primary_key_config.enabled = True
        mock_foreign_key_config = Mock()
        mock_foreign_key_config.enabled = True

        # Should not raise exception - constraints are informational in Trino
        adapter.apply_constraint_configuration(mock_primary_key_config, mock_foreign_key_config, mock_connection)

    def test_from_config(self):
        """Test from_config() properly passes through all configuration parameters."""
        config = {
            "host": "trino-coordinator.example.com",
            "port": 8443,
            "catalog": "iceberg",
            "username": "test_user",
            "password": "test_pass",
            "http_scheme": "https",
            "table_format": "iceberg",
            "benchmark": "tpch",
            "scale_factor": 1.0,
            "verify_ssl": True,
            "session_properties": {"query_max_memory": "1GB"},
        }

        try:
            adapter = TrinoAdapter.from_config(config)
        except ImportError:
            pytest.skip("Trino drivers not installed")

        assert adapter.host == "trino-coordinator.example.com"
        assert adapter.port == 8443
        assert adapter.catalog == "iceberg"
        assert adapter.username == "test_user"
        assert adapter.password == "test_pass"
        assert adapter.http_scheme == "https"
        assert adapter.table_format == "iceberg"
        assert adapter.verify_ssl is True
        assert adapter.session_properties == {"query_max_memory": "1GB"}

    def test_from_config_generates_schema_name(self):
        """Test from_config() generates schema name from benchmark config."""
        config = {
            "host": "trino-coordinator.example.com",
            "catalog": "memory",
            "benchmark": "tpch",
            "scale_factor": 10.0,
        }

        try:
            adapter = TrinoAdapter.from_config(config)
        except ImportError:
            pytest.skip("Trino drivers not installed")

        # Schema should be generated from benchmark config
        assert adapter.schema is not None
        assert "tpch" in adapter.schema.lower() or "sf10" in adapter.schema.lower()

    def test_normalize_table_name_in_sql(self):
        """Test table name normalization in SQL."""
        try:
            adapter = TrinoAdapter()
        except ImportError:
            pytest.skip("Trino drivers not installed")

        sql = 'CREATE TABLE "CUSTOMER" (id INTEGER, name VARCHAR(100))'
        normalized = adapter._normalize_table_name_in_sql(sql)

        assert "CREATE TABLE customer" in normalized

    def test_optimize_table_definition_memory(self):
        """Test table definition optimization for memory catalog."""
        try:
            adapter = TrinoAdapter(table_format="memory")
        except ImportError:
            pytest.skip("Trino drivers not installed")

        sql = "CREATE TABLE test (id INTEGER) WITH (format='PARQUET')"
        optimized = adapter._optimize_table_definition(sql)

        # Memory catalog should strip WITH clause
        assert "WITH" not in optimized

    def test_optimize_table_definition_iceberg(self):
        """Test table definition optimization for Iceberg tables."""
        try:
            adapter = TrinoAdapter(table_format="iceberg")
        except ImportError:
            pytest.skip("Trino drivers not installed")

        sql = "CREATE TABLE test (id INTEGER, name VARCHAR(100))"
        optimized = adapter._optimize_table_definition(sql)

        # Iceberg should add format specification
        assert "WITH" in optimized
        assert "PARQUET" in optimized

    def test_get_existing_tables(self):
        """Test getting list of existing tables."""
        try:
            adapter = TrinoAdapter()
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [("table1",), ("table2",), ("TABLE3",)]

        tables = adapter._get_existing_tables(mock_connection)

        assert tables == ["table1", "table2", "table3"]  # All lowercase
        mock_cursor.execute.assert_called_with("SHOW TABLES")

    def test_analyze_table_memory_skipped(self):
        """Test that ANALYZE is skipped for memory catalog."""
        try:
            adapter = TrinoAdapter(table_format="memory")
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        adapter.analyze_table(mock_connection, "test_table")

        # ANALYZE should not be called for memory catalog
        mock_cursor.execute.assert_not_called()

    def test_analyze_table_iceberg(self):
        """Test that ANALYZE is called for Iceberg catalog."""
        try:
            adapter = TrinoAdapter(table_format="iceberg")
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        adapter.analyze_table(mock_connection, "test_table")

        mock_cursor.execute.assert_called_once_with("ANALYZE test_table")

    def test_session_properties_configuration(self):
        """Test session properties configuration."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                session_properties={
                    "query_max_memory": "2GB",
                    "join_reordering_strategy": "AUTOMATIC",
                },
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        assert adapter.session_properties["query_max_memory"] == "2GB"
        assert adapter.session_properties["join_reordering_strategy"] == "AUTOMATIC"

    def test_timezone_configuration(self):
        """Test timezone configuration."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                timezone="America/New_York",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        params = adapter._get_connection_params()
        assert params["timezone"] == "America/New_York"

    def test_encoding_configuration(self):
        """Test spooling protocol encoding configuration."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                encoding="json+zstd",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        params = adapter._get_connection_params()
        assert params["encoding"] == "json+zstd"

    def test_validate_identifier_valid(self):
        """Test valid SQL identifier validation."""
        try:
            adapter = TrinoAdapter()
        except ImportError:
            pytest.skip("Trino drivers not installed")

        # Valid identifiers
        assert adapter._validate_identifier("my_schema") is True
        assert adapter._validate_identifier("catalog123") is True
        assert adapter._validate_identifier("_private") is True
        assert adapter._validate_identifier("test-schema") is True
        assert adapter._validate_identifier("MySchema") is True

    def test_validate_identifier_invalid(self):
        """Test invalid SQL identifier validation (prevents SQL injection)."""
        try:
            adapter = TrinoAdapter()
        except ImportError:
            pytest.skip("Trino drivers not installed")

        # Invalid identifiers - potential SQL injection attempts
        assert adapter._validate_identifier("") is False
        assert adapter._validate_identifier(None) is False
        assert adapter._validate_identifier("schema; DROP TABLE users") is False
        assert adapter._validate_identifier("schema'--") is False
        assert adapter._validate_identifier("123schema") is False  # Can't start with number
        assert adapter._validate_identifier("a" * 129) is False  # Too long
        assert adapter._validate_identifier("schema.table") is False  # Dots not allowed
        assert adapter._validate_identifier('schema"quote') is False  # Quotes not allowed

    def test_drop_database_rejects_invalid_identifier(self):
        """Test that drop_database rejects SQL injection attempts."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
                catalog="memory",
                schema="default",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        # Attempting SQL injection should raise ValueError
        with pytest.raises(ValueError, match="Invalid catalog or schema identifier"):
            adapter.drop_database(schema="test; DROP TABLE users", catalog="memory")

        with pytest.raises(ValueError, match="Invalid catalog or schema identifier"):
            adapter.drop_database(schema="test", catalog="memory'--")

    def test_test_connection_success(self):
        """Test successful connection test."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (1,)

        import benchbox.platforms.trino as trino_module

        with patch.object(trino_module.trino.dbapi, "connect", return_value=mock_connection):
            result = adapter.test_connection()

        assert result is True
        mock_cursor.execute.assert_called_with("SELECT 1")
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

    def test_test_connection_failure(self):
        """Test failed connection test."""
        try:
            adapter = TrinoAdapter(
                host="trino-coordinator.example.com",
            )
        except ImportError:
            pytest.skip("Trino drivers not installed")

        import benchbox.platforms.trino as trino_module

        with patch.object(trino_module.trino.dbapi, "connect", side_effect=Exception("Connection refused")):
            result = adapter.test_connection()

        assert result is False

    def test_trino_only_not_presto(self):
        """Verify this adapter is explicitly for Trino, NOT PrestoDB.

        The TrinoAdapter uses the 'trino' Python package and 'trino' SQL dialect,
        which are incompatible with PrestoDB (Meta's Presto fork).

        Key differences that prevent PrestoDB compatibility:
        - Driver: Uses 'trino' package, PrestoDB needs 'presto-python-client'
        - Dialect: Uses SQLGlot 'trino' dialect, not 'presto'
        - Headers: Trino uses X-Trino-* headers, Presto uses X-Presto-*
        - System tables: Different metadata schemas between forks

        For AWS managed Presto/Trino, use AthenaAdapter instead.
        """
        try:
            adapter = TrinoAdapter()
        except ImportError:
            pytest.skip("Trino drivers not installed")

        # Verify dialect is explicitly 'trino', not 'presto'
        assert adapter.get_target_dialect() == "trino"
        assert adapter._dialect == "trino"

        # Verify platform name is 'Trino', not 'Presto'
        assert adapter.platform_name == "Trino"
        assert "Presto" not in adapter.platform_name
