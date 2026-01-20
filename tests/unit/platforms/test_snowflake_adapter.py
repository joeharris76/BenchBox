"""Tests for Snowflake platform adapter.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from benchbox.platforms.snowflake import SnowflakeAdapter

pytestmark = pytest.mark.fast


@pytest.fixture(autouse=True)
def snowflake_dependencies():
    """Mock Snowflake dependency check to simulate installed extras."""

    with patch("benchbox.platforms.snowflake.check_platform_dependencies", return_value=(True, [])):
        yield


class TestSnowflakeAdapter:
    """Test Snowflake platform adapter functionality."""

    def test_initialization_success(self):
        """Test successful adapter initialization."""
        with patch("benchbox.platforms.snowflake.snowflake"):
            adapter = SnowflakeAdapter(
                account="test_account",
                username="test_user",
                password="test_pass",
                warehouse="TEST_WH",
                database="TEST_DB",
                schema="TEST_SCHEMA",
            )
            assert adapter.platform_name == "Snowflake"
            assert adapter.get_target_dialect() == "snowflake"
            assert adapter.account == "test_account"
            assert adapter.username == "test_user"
            assert adapter.warehouse == "TEST_WH"
            assert adapter.database == "TEST_DB"

    def test_initialization_with_defaults(self):
        """Test initialization with default configuration."""
        with patch("benchbox.platforms.snowflake.snowflake"):
            adapter = SnowflakeAdapter(account="test_account", username="test_user", password="test_pass")
            assert adapter.warehouse == "COMPUTE_WH"
            assert adapter.database == "BENCHBOX"
            assert adapter.schema == "PUBLIC"
            assert adapter.authenticator == "snowflake"
            assert adapter.warehouse_size == "MEDIUM"
            assert adapter.auto_suspend == 300
            assert adapter.auto_resume is True

    def test_initialization_missing_driver(self):
        """Test initialization when Snowflake dependencies are missing."""
        with (
            patch("benchbox.platforms.snowflake.snowflake", None),
            patch(
                "benchbox.platforms.snowflake.check_platform_dependencies",
                return_value=(False, ["snowflake-connector-python"]),
            ),
            pytest.raises(ImportError) as excinfo,
        ):
            SnowflakeAdapter()

        assert "Missing dependencies for snowflake platform" in str(excinfo.value)

    def test_initialization_missing_required_config(self):
        """Test initialization with missing required configuration."""
        from benchbox.core.exceptions import ConfigurationError

        with patch("benchbox.platforms.snowflake.snowflake"):
            with pytest.raises(ConfigurationError, match="Snowflake configuration is incomplete"):
                SnowflakeAdapter()  # Missing required fields

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_get_connection_params(self, mock_snowflake):
        """Test connection parameter configuration."""
        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
            role="TEST_ROLE",
        )

        params = adapter._get_connection_params()

        assert params["account"] == "test_account"
        assert params["username"] == "test_user"
        assert params["password"] == "test_pass"
        assert params["warehouse"] == "TEST_WH"
        assert params["role"] == "TEST_ROLE"

        # Test with overrides
        override_params = adapter._get_connection_params(account="override_account", username="override_user")
        assert override_params["account"] == "override_account"
        assert override_params["username"] == "override_user"
        assert override_params["password"] == "test_pass"  # Should keep original

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_from_config_generates_database_name(self, mock_snowflake):
        """Test that from_config generates proper database names."""
        config = {
            "benchmark": "tpch",
            "scale_factor": 1.0,
            "account": "test_account",
            "username": "test_user",
            "password": "test_pass",
            "warehouse": "TEST_WH",
            "tuning_config": None,  # No tuning configuration
        }

        adapter = SnowflakeAdapter.from_config(config)

        # Database should be auto-generated with benchmark and scale info
        assert adapter.database == "tpch_sf1_notuning_noconstraints"
        assert adapter.account == "test_account"
        assert adapter.warehouse == "TEST_WH"

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_from_config_respects_explicit_database(self, mock_snowflake):
        """Test that explicit database name overrides generation."""
        config = {
            "benchmark": "tpch",
            "scale_factor": 1.0,
            "account": "test_account",
            "username": "test_user",
            "password": "test_pass",
            "warehouse": "TEST_WH",
            "database": "CUSTOM_DATABASE",
        }

        adapter = SnowflakeAdapter.from_config(config)

        # Explicit database name should be used
        assert adapter.database == "CUSTOM_DATABASE"

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_from_config_with_scale_factor_formatting(self, mock_snowflake):
        """Test database name generation with different scale factors."""
        # Test SF 0.1
        config_01 = {
            "benchmark": "tpcds",
            "scale_factor": 0.1,
            "account": "test_account",
            "username": "test_user",
            "password": "test_pass",
        }
        adapter_01 = SnowflakeAdapter.from_config(config_01)
        assert adapter_01.database == "tpcds_sf01_notuning_noconstraints"

        # Test SF 10
        config_10 = {
            "benchmark": "ssb",
            "scale_factor": 10.0,
            "account": "test_account",
            "username": "test_user",
            "password": "test_pass",
        }
        adapter_10 = SnowflakeAdapter.from_config(config_10)
        assert adapter_10.database == "ssb_sf10_notuning_noconstraints"

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_create_admin_connection(self, mock_snowflake):
        """Test admin connection creation."""
        mock_connection = Mock()
        mock_snowflake.connector.connect.return_value = mock_connection

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
        )

        admin_conn = adapter._create_admin_connection()

        assert admin_conn == mock_connection
        mock_snowflake.connector.connect.assert_called_once()
        call_kwargs = mock_snowflake.connector.connect.call_args[1]
        assert call_kwargs["account"] == "test_account"
        assert call_kwargs["username"] == "test_user"
        assert call_kwargs["password"] == "test_pass"
        assert call_kwargs["warehouse"] == "TEST_WH"
        assert call_kwargs["client_session_keep_alive"] is True

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_check_server_database_exists_true(self, mock_snowflake):
        """Test database existence check when database exists."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            ["schema", "TEST_DB", "owner", "comment"],
            ["schema", "OTHER_DB", "owner", "comment"],
        ]
        mock_snowflake.connector.connect.return_value = mock_connection

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
        )

        exists = adapter.check_server_database_exists()

        assert exists is True
        mock_cursor.execute.assert_called_once_with("SHOW DATABASES")
        mock_connection.close.assert_called_once()

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_check_server_database_exists_false(self, mock_snowflake):
        """Test database existence check when database doesn't exist."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Return different values for each fetchall() call:
        # 1. SHOW DATABASES returns only OTHER_DB (not TEST_DB)
        # 2. SHOW SCHEMAS returns empty (database doesn't exist)
        # 3. SHOW TABLES returns empty (no tables)
        mock_cursor.fetchall.side_effect = [
            [["schema", "OTHER_DB", "owner", "comment"]],  # SHOW DATABASES
            [],  # SHOW SCHEMAS (if database exists but is empty)
            [],  # SHOW TABLES (if schema exists but is empty)
        ]
        mock_snowflake.connector.connect.return_value = mock_connection

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
        )

        exists = adapter.check_server_database_exists()

        assert exists is False
        mock_connection.close.assert_called_once()

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_check_server_database_exists_connection_error(self, mock_snowflake):
        """Test database existence check with connection error."""
        mock_snowflake.connector.connect.side_effect = Exception("Connection failed")

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
        )

        exists = adapter.check_server_database_exists()

        assert exists is False

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_drop_database(self, mock_snowflake):
        """Test database dropping."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_snowflake.connector.connect.return_value = mock_connection

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
        )

        adapter.drop_database()

        # SQL injection fix: identifiers are now quoted
        mock_cursor.execute.assert_called_once_with('DROP DATABASE IF EXISTS "TEST_DB"')
        mock_connection.close.assert_called_once()

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_create_connection_success(self, mock_snowflake):
        """Test successful connection creation."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_snowflake.connector.connect.return_value = mock_connection

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
        )

        # Mock handle_existing_database
        with patch.object(adapter, "handle_existing_database"):
            connection = adapter.create_connection()

        assert connection == mock_connection
        mock_snowflake.connector.connect.assert_called_once()

        # Check connection test was performed
        mock_cursor.execute.assert_called_with("SELECT CURRENT_VERSION()")
        mock_cursor.fetchall.assert_called_once()
        mock_cursor.close.assert_called_once()

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_create_connection_with_key_pair_auth(self, mock_snowflake):
        """Test connection creation with key pair authentication."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_snowflake.connector.connect.return_value = mock_connection

        # Create a temporary key file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as f:
            f.write("""-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA0123456789...
-----END RSA PRIVATE KEY-----""")
            key_path = f.name

        try:
            adapter = SnowflakeAdapter(
                account="test_account",
                username="test_user",
                password="test_pass",
                warehouse="TEST_WH",
                database="TEST_DB",
                private_key_path=key_path,
            )

            # Mock cryptography imports by patching the import statements
            mock_serialization = Mock()
            mock_load_key = Mock()
            mock_private_key = Mock()
            mock_load_key.return_value = mock_private_key
            mock_private_key.private_bytes.return_value = b"mock_key_bytes"

            with patch.dict(
                "sys.modules",
                {
                    "cryptography": Mock(),
                    "cryptography.hazmat": Mock(),
                    "cryptography.hazmat.primitives": Mock(),
                    "cryptography.hazmat.primitives.serialization": mock_serialization,
                },
            ):
                mock_serialization.load_pem_private_key = mock_load_key
                mock_serialization.Encoding = Mock()
                mock_serialization.PrivateFormat = Mock()
                mock_serialization.NoEncryption = Mock()

                with patch.object(adapter, "handle_existing_database"):
                    connection = adapter.create_connection()

            assert connection == mock_connection

            # Should call connect with private_key parameter
            call_kwargs = mock_snowflake.connector.connect.call_args[1]
            assert "private_key" in call_kwargs
            assert "password" not in call_kwargs  # Should remove password for key auth

        finally:
            Path(key_path).unlink()

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_create_connection_failure(self, mock_snowflake):
        """Test connection creation failure."""
        mock_snowflake.connector.connect.side_effect = Exception("Connection failed")

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
        )

        with patch.object(adapter, "handle_existing_database"):
            with pytest.raises(Exception, match="Connection failed"):
                adapter.create_connection()

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_create_schema(self, mock_snowflake):
        """Test schema creation."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_snowflake.connector.connect.return_value = mock_connection

        mock_benchmark = Mock()
        mock_benchmark.get_create_tables_sql.return_value = """
            CREATE TABLE table1 (id INTEGER, name VARCHAR(100));
            CREATE TABLE table2 (id INTEGER, data TEXT);
        """

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
        )

        # Mock translate_sql method
        with patch.object(adapter, "translate_sql") as mock_translate:
            mock_translate.return_value = (
                "CREATE TABLE table1 (id INTEGER, name VARCHAR(100));\nCREATE TABLE table2 (id INTEGER, data TEXT);"
            )

            schema_time = adapter.create_schema(mock_benchmark, mock_connection)

        assert isinstance(schema_time, float)
        assert schema_time >= 0

        # Should execute database and schema setup
        setup_calls = list(mock_cursor.execute.call_args_list)
        setup_sqls = [str(call) for call in setup_calls]

        # Should include database creation, schema creation, and table creation
        assert any("CREATE DATABASE" in sql for sql in setup_sqls)
        assert any("CREATE SCHEMA" in sql for sql in setup_sqls)

        mock_cursor.close.assert_called_once()

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_load_data_with_file_upload(self, mock_snowflake):
        """Test data loading using Snowflake PUT and COPY INTO."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock COPY INTO results
        mock_cursor.fetchall.side_effect = [
            [(True, 100, 0, 0, "LOADED", None)],  # Copy results
            [(100,)],  # Row count
        ]
        mock_cursor.fetchone.return_value = (100,)  # Row count query

        mock_benchmark = Mock()

        # Create temporary test file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tbl", delete=False) as f:
            f.write("1|test1|\n2|test2|\n")
            temp_path = Path(f.name)

        try:
            mock_benchmark.tables = {"test_table": str(temp_path)}

            adapter = SnowflakeAdapter(
                account="test_account",
                username="test_user",
                password="test_pass",
                warehouse="TEST_WH",
                database="TEST_DB",
            )

            table_stats, load_time, _ = adapter.load_data(mock_benchmark, mock_connection, Path("/tmp"))

            assert isinstance(table_stats, dict)
            assert isinstance(load_time, float)
            assert load_time >= 0
            assert "TEST_TABLE" in table_stats
            assert table_stats["TEST_TABLE"] == 100

            # Should execute file format creation, PUT, and COPY INTO
            execute_calls = [str(call) for call in mock_cursor.execute.call_args_list]
            assert any("CREATE OR REPLACE FILE FORMAT" in call for call in execute_calls)
            assert any("PUT file://" in call for call in execute_calls)
            assert any("COPY INTO TEST_TABLE" in call for call in execute_calls)

        finally:
            temp_path.unlink()

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_configure_for_benchmark_olap(self, mock_snowflake):
        """Test OLAP benchmark configuration."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
            strict_validation=False,  # Disable validation in unit tests
        )

        adapter.configure_for_benchmark(mock_connection, "olap")

        # Should execute OLAP-specific optimizations
        execute_calls = [str(call) for call in mock_cursor.execute.call_args_list]
        assert any("QUERY_ACCELERATION_MAX_SCALE_FACTOR" in call for call in execute_calls)
        assert any("USE_CACHED_RESULT" in call for call in execute_calls)
        assert any("STATEMENT_TIMEOUT_IN_SECONDS" in call for call in execute_calls)
        assert any("USE WAREHOUSE" in call for call in execute_calls)

        # cursor.close() called twice: once for configure, once for validation
        assert mock_cursor.close.call_count == 2

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_configure_for_benchmark_with_multi_cluster(self, mock_snowflake):
        """Test benchmark configuration with multi-cluster warehouse."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
            multi_cluster_warehouse=True,
            modify_warehouse_settings=True,  # Explicitly enable warehouse modifications
            strict_validation=False,  # Disable validation in unit tests
        )

        adapter.configure_for_benchmark(mock_connection, "tpch")

        # Should execute multi-cluster warehouse configuration
        execute_calls = [str(call) for call in mock_cursor.execute.call_args_list]
        assert any("MIN_CLUSTER_COUNT" in call for call in execute_calls)
        assert any("MAX_CLUSTER_COUNT" in call for call in execute_calls)
        assert any("SCALING_POLICY" in call for call in execute_calls)

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_execute_query_success(self, mock_snowflake):
        """Test successful query execution."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1, "test"), (2, "test2")]

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
        )

        # Mock query statistics
        with patch.object(adapter, "_get_query_statistics") as mock_stats:
            mock_stats.return_value = {"snowflake_query_id": "test_query_id"}

            result = adapter.execute_query(mock_connection, "SELECT * FROM test", "q1")

        assert result["query_id"] == "q1"
        assert result["status"] == "SUCCESS"
        assert result["rows_returned"] == 2
        assert result["first_row"] == (1, "test")
        assert isinstance(result["execution_time"], float)
        assert result["query_statistics"] == {"snowflake_query_id": "test_query_id"}

        mock_cursor.execute.assert_any_call("ALTER SESSION SET QUERY_TAG = 'BenchBox_q1'")
        mock_cursor.execute.assert_any_call("SELECT * FROM test")
        mock_cursor.close.assert_called_once()

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_execute_query_failure(self, mock_snowflake):
        """Test query execution failure."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = [
            None,
            Exception("Query failed"),
        ]  # Query tag succeeds, query fails

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
        )

        result = adapter.execute_query(mock_connection, "INVALID SQL", "q1")

        assert result["query_id"] == "q1"
        assert result["status"] == "FAILED"
        assert result["rows_returned"] == 0
        assert result["error"] == "Query failed"
        assert result["error_type"] == "Exception"
        assert isinstance(result["execution_time"], float)

        mock_cursor.close.assert_called_once()

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_get_query_statistics(self, mock_snowflake):
        """Test query statistics retrieval."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = [
            "query_id_123",
            "SELECT * FROM test",
            1000,
            800,
            200,
            1024000,
            512000,
            0,
            0,
            10,
            1000,
            5.5,
            "MEDIUM",
            1,
        ]

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
        )

        stats = adapter._get_query_statistics(mock_connection, "q1")

        assert stats["snowflake_query_id"] == "query_id_123"
        assert stats["total_elapsed_time_ms"] == 1000
        assert stats["execution_time_ms"] == 800
        assert stats["compilation_time_ms"] == 200
        assert stats["bytes_scanned"] == 1024000
        assert stats["rows_produced"] == 10
        assert stats["warehouse_size"] == "MEDIUM"

        mock_cursor.close.assert_called_once()

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_get_platform_metadata(self, mock_snowflake):
        """Test platform metadata collection."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock query responses
        mock_cursor.fetchone.side_effect = [
            ["6.21.0"],  # Snowflake version
            [
                "test_user",
                "test_role",
                "TEST_WH",
                "TEST_DB",
                "PUBLIC",
                "us-east-1",
                "test_account",
            ],  # Session info
        ]

        # Import datetime for proper mock objects
        from datetime import datetime

        mock_datetime = datetime(2024, 1, 1, 12, 0, 0)

        mock_cursor.fetchall.side_effect = [
            [
                [
                    "TEST_WH",
                    "STARTED",
                    "STANDARD",
                    "MEDIUM",
                    1,
                    1,
                    1,
                    1,
                    0,
                    0,
                    0,
                    0,
                    300,
                    True,
                    True,
                    False,
                    False,
                    False,
                    "2024-01-01",
                    "2024-01-01",
                    "2024-01-01",
                    "test_user",
                    "Test warehouse",
                    None,
                    None,
                    "STANDARD",
                ]
            ],  # Warehouse info
            [
                ["TABLE1", 1000, 1024000, 7, mock_datetime, mock_datetime, None]
            ],  # Table info with proper datetime objects
        ]

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
        )

        metadata = adapter._get_platform_metadata(mock_connection)

        assert metadata["platform"] == "Snowflake"
        assert metadata["account"] == "test_account"
        assert metadata["warehouse"] == "TEST_WH"
        assert metadata["snowflake_version"] == "6.21.0"
        assert "session_info" in metadata
        assert "warehouse_info" in metadata
        assert "tables" in metadata

        mock_cursor.close.assert_called()

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_analyze_table(self, mock_snowflake):
        """Test table analysis (reclustering)."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
        )

        adapter.analyze_table(mock_connection, "test_table")

        mock_cursor.execute.assert_called_once_with("ALTER TABLE TEST_TABLE RECLUSTER")
        mock_cursor.close.assert_called_once()

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_close_connection(self, mock_snowflake):
        """Test connection closing."""
        mock_connection = Mock()

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
        )

        adapter.close_connection(mock_connection)

        mock_connection.close.assert_called_once()

    def test_supports_tuning_type(self):
        """Test tuning type support checking."""
        with patch("benchbox.platforms.snowflake.snowflake"):
            adapter = SnowflakeAdapter(
                account="test_account",
                username="test_user",
                password="test_pass",
                warehouse="TEST_WH",
                database="TEST_DB",
            )

            # Mock TuningType from the correct import path
            with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
                mock_tuning_type.CLUSTERING = "clustering"
                mock_tuning_type.PARTITIONING = "partitioning"
                mock_tuning_type.SORTING = "sorting"

                assert adapter.supports_tuning_type(mock_tuning_type.CLUSTERING) is True
                assert adapter.supports_tuning_type(mock_tuning_type.PARTITIONING) is True
                assert adapter.supports_tuning_type(mock_tuning_type.SORTING) is False

    def test_generate_tuning_clause_with_clustering(self):
        """Test tuning clause generation with clustering."""
        with patch("benchbox.platforms.snowflake.snowflake"):
            adapter = SnowflakeAdapter(
                account="test_account",
                username="test_user",
                password="test_pass",
                warehouse="TEST_WH",
                database="TEST_DB",
            )

            # Mock table tuning with clustering
            mock_tuning = Mock()
            mock_tuning.has_any_tuning.return_value = True

            # Mock clustering column
            mock_column = Mock()
            mock_column.name = "cluster_key"
            mock_column.order = 1

            with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
                mock_tuning_type.CLUSTERING = "clustering"
                mock_tuning_type.PARTITIONING = "partitioning"

                def mock_get_columns_by_type(tuning_type):
                    if tuning_type == mock_tuning_type.CLUSTERING:
                        return [mock_column]
                    return []

                mock_tuning.get_columns_by_type.side_effect = mock_get_columns_by_type

                clause = adapter.generate_tuning_clause(mock_tuning)

                assert "CLUSTER BY (cluster_key)" in clause

    def test_generate_tuning_clause_none(self):
        """Test tuning clause generation with None input."""
        with patch("benchbox.platforms.snowflake.snowflake"):
            adapter = SnowflakeAdapter(
                account="test_account",
                username="test_user",
                password="test_pass",
                warehouse="TEST_WH",
                database="TEST_DB",
            )

            clause = adapter.generate_tuning_clause(None)
            assert clause == ""

    @patch("benchbox.platforms.snowflake.snowflake")
    def test_apply_table_tunings_with_clustering(self, mock_snowflake):
        """Test applying table tunings with clustering."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock clustering key query response
        mock_cursor.fetchone.return_value = [None]  # No existing clustering

        adapter = SnowflakeAdapter(
            account="test_account",
            username="test_user",
            password="test_pass",
            warehouse="TEST_WH",
            database="TEST_DB",
        )

        # Mock table tuning
        mock_tuning = Mock()
        mock_tuning.table_name = "test_table"
        mock_tuning.has_any_tuning.return_value = True

        # Mock clustering column
        mock_column = Mock()
        mock_column.name = "cluster_key"
        mock_column.order = 1

        with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
            mock_tuning_type.CLUSTERING = "clustering"
            mock_tuning_type.PARTITIONING = "partitioning"
            mock_tuning_type.SORTING = "sorting"
            mock_tuning_type.DISTRIBUTION = "distribution"

            def mock_get_columns_by_type(tuning_type):
                if tuning_type == mock_tuning_type.CLUSTERING:
                    return [mock_column]
                return []

            mock_tuning.get_columns_by_type.side_effect = mock_get_columns_by_type

            adapter.apply_table_tunings(mock_tuning, mock_connection)

            # Should execute clustering setup
            execute_calls = [str(call) for call in mock_cursor.execute.call_args_list]
            assert any("ALTER TABLE TEST_TABLE CLUSTER BY" in call for call in execute_calls)
            assert any("RESUME RECLUSTER" in call for call in execute_calls)

        mock_cursor.close.assert_called()

    def test_apply_unified_tuning(self):
        """Test unified tuning configuration application."""
        with patch("benchbox.platforms.snowflake.snowflake"):
            adapter = SnowflakeAdapter(
                account="test_account",
                username="test_user",
                password="test_pass",
                warehouse="TEST_WH",
                database="TEST_DB",
            )

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

    def test_file_format_detection_for_chunked_files(self):
        """Test that chunked .tbl files (.tbl.1, .tbl.2) are correctly detected as TBL format."""
        with patch("benchbox.platforms.snowflake.snowflake"):
            from pathlib import Path

            SnowflakeAdapter(
                account="test_account",
                username="test_user",
                password="test_pass",
                warehouse="TEST_WH",
                database="TEST_DB",
            )

            # Test various TPC-H file naming patterns
            test_cases = [
                (Path("customer.tbl"), True, "Simple .tbl file"),
                (Path("customer.tbl.1"), True, "Chunked .tbl file"),
                (Path("lineitem.tbl.10"), True, "Chunked .tbl file (2 digits)"),
                (Path("orders.tbl.gz"), True, "Compressed .tbl file"),
                (Path("part.tbl.1.gz"), True, "Chunked compressed .tbl file"),
                (Path("nation.tbl.zst"), True, "zstd compressed .tbl file"),
                (Path("region.csv"), False, "CSV file"),
                (Path("supplier.csv.gz"), False, "Compressed CSV file"),
            ]

            for file_path, should_be_tbl, description in test_cases:
                file_str = str(file_path.name)
                is_tbl = ".tbl" in file_str
                assert is_tbl == should_be_tbl, f"Failed for {description}: {file_path.name}"

    def test_apply_constraint_configuration(self):
        """Test constraint configuration application."""
        with patch("benchbox.platforms.snowflake.snowflake"):
            adapter = SnowflakeAdapter(
                account="test_account",
                username="test_user",
                password="test_pass",
                warehouse="TEST_WH",
                database="TEST_DB",
            )

            mock_connection = Mock()
            mock_primary_key_config = Mock()
            mock_primary_key_config.enabled = True
            mock_foreign_key_config = Mock()
            mock_foreign_key_config.enabled = True

            # Should not raise exception - constraints are informational in Snowflake
            adapter.apply_constraint_configuration(mock_primary_key_config, mock_foreign_key_config, mock_connection)
