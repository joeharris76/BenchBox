"""Tests for Azure Synapse Analytics platform adapter.

Tests the AzureSynapseAdapter for Azure Synapse Dedicated SQL Pool support.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from unittest.mock import Mock, patch

import pytest

import benchbox.platforms.azure_synapse as synapse_module
from benchbox.platforms.azure_synapse import SYNAPSE_DIALECT, AzureSynapseAdapter

pytestmark = pytest.mark.fast


@pytest.fixture()
def synapse_stubs(monkeypatch):
    """Patch pyodbc objects so tests don't require the real driver."""
    mock_pyodbc = Mock()
    mock_pyodbc.connect = Mock()

    monkeypatch.setattr(synapse_module, "pyodbc", mock_pyodbc)

    # Mock the dependency check to always pass
    monkeypatch.setattr(
        synapse_module,
        "check_platform_dependencies",
        lambda platform, packages=None: (True, []),
    )

    return mock_pyodbc


class TestAzureSynapseAdapter:
    """Unit tests for Azure Synapse adapter wiring and SQL handling."""

    def test_initialization_defaults(self, synapse_stubs):
        """Adapter should initialize with Azure Synapse defaults when stubs are present."""
        adapter = AzureSynapseAdapter(
            server="myworkspace.sql.azuresynapse.net",
            username="admin",
            password="secret",
        )

        assert adapter.platform_name == "Azure Synapse"
        assert adapter.get_target_dialect() == SYNAPSE_DIALECT
        assert adapter.server == "myworkspace.sql.azuresynapse.net"
        assert adapter.port == 1433
        assert adapter.database == "benchbox"
        assert adapter.username == "admin"
        assert adapter.schema == "dbo"
        assert adapter.auth_method == "sql"

    def test_initialization_with_config(self, synapse_stubs):
        """Adapter should accept custom configuration."""
        adapter = AzureSynapseAdapter(
            server="custom.sql.azuresynapse.net",
            port=1434,
            database="custom_db",
            username="custom_user",
            password="secret",
            schema="analytics",
            auth_method="aad_password",
            resource_class="staticrc30",
        )

        assert adapter.server == "custom.sql.azuresynapse.net"
        assert adapter.port == 1434
        assert adapter.database == "custom_db"
        assert adapter.username == "custom_user"
        assert adapter.password == "secret"
        assert adapter.schema == "analytics"
        assert adapter.auth_method == "aad_password"
        assert adapter.resource_class == "staticrc30"

    def test_get_connection_string_sql_auth(self, synapse_stubs):
        """Connection string should use SQL auth format."""
        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            port=1433,
            database="testdb",
            username="testuser",
            password="testpass",
            auth_method="sql",
        )

        conn_str = adapter._get_connection_string()

        assert "test.sql.azuresynapse.net" in conn_str
        assert "testdb" in conn_str
        assert "UID=testuser" in conn_str
        assert "PWD=testpass" in conn_str
        assert "Encrypt=yes" in conn_str

    def test_get_connection_string_aad_auth(self, synapse_stubs):
        """Connection string should use AAD auth format when specified."""
        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            username="user@domain.com",
            password="secret",
            auth_method="aad_password",
        )

        conn_str = adapter._get_connection_string()

        assert "Authentication=ActiveDirectoryPassword" in conn_str

    def test_get_connection_string_msi_auth(self, synapse_stubs):
        """Connection string should use MSI auth format when specified."""
        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            auth_method="aad_msi",
        )

        conn_str = adapter._get_connection_string()

        assert "Authentication=ActiveDirectoryMsi" in conn_str

    def test_check_server_database_exists_true(self, synapse_stubs):
        """Database existence check returns True when database is found."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ("testdb",)
        mock_conn.cursor.return_value = mock_cursor
        synapse_stubs.connect.return_value = mock_conn

        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            database="testdb",
            username="admin",
            password="secret",
        )

        assert adapter.check_server_database_exists() is True

    def test_check_server_database_exists_false(self, synapse_stubs):
        """Database existence check returns False when database not found."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value = mock_cursor
        synapse_stubs.connect.return_value = mock_conn

        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            database="nonexistent",
            username="admin",
            password="secret",
        )

        assert adapter.check_server_database_exists() is False

    def test_check_server_database_exists_connection_error(self, synapse_stubs):
        """Database existence check returns False on connection error."""
        synapse_stubs.connect.side_effect = Exception("Connection refused")

        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            username="admin",
            password="secret",
        )

        assert adapter.check_server_database_exists() is False

    def test_create_connection_creates_database(self, synapse_stubs):
        """Connection should create database if it doesn't exist."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [None, ("version",)]  # DB doesn't exist, then version
        mock_conn.cursor.return_value = mock_cursor
        synapse_stubs.connect.return_value = mock_conn

        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            database="newdb",
            username="admin",
            password="secret",
        )

        with (
            patch.object(adapter, "handle_existing_database"),
            patch.object(adapter, "check_server_database_exists", side_effect=[False, True]),
            patch.object(adapter, "_create_admin_connection", return_value=mock_conn),
        ):
            adapter.create_connection()

        # Should have called execute to create database
        executed_calls = mock_cursor.execute.call_args_list
        create_db_calls = [c for c in executed_calls if "CREATE DATABASE" in str(c)]
        assert len(create_db_calls) > 0

    def test_get_platform_info(self, synapse_stubs):
        """Platform info should include Azure Synapse version and settings."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.side_effect = [
            ("Microsoft SQL Azure ...",),  # version
            (100.0,),  # database size
        ]
        mock_conn.cursor.return_value = mock_cursor

        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            database="testdb",
            schema="analytics",
            username="admin",
            password="secret",
            resource_class="staticrc30",
        )

        info = adapter.get_platform_info(connection=mock_conn)

        assert info["platform_type"] == "azure_synapse"
        assert info["platform_name"] == "Azure Synapse"
        assert info["cloud_provider"] == "Azure"
        assert info["server"] == "test.sql.azuresynapse.net"
        assert info["dialect"] == SYNAPSE_DIALECT
        assert info["configuration"]["database"] == "testdb"
        assert info["configuration"]["schema"] == "analytics"
        assert info["configuration"]["resource_class"] == "staticrc30"

    def test_execute_query_success(self, synapse_stubs):
        """Query execution should return correct result structure."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [(1, "test"), (2, "test2")]
        mock_conn.cursor.return_value = mock_cursor

        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            username="admin",
            password="secret",
        )

        result = adapter.execute_query(mock_conn, "SELECT * FROM test", "q1")

        assert result["query_id"] == "q1"
        assert result["status"] == "SUCCESS"
        assert result["rows_returned"] == 2
        assert result["first_row"] == (1, "test")
        assert isinstance(result["execution_time"], float)

    def test_execute_query_failure(self, synapse_stubs):
        """Query execution failure should return error info."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.side_effect = Exception("Query failed")
        mock_conn.cursor.return_value = mock_cursor

        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            username="admin",
            password="secret",
        )

        result = adapter.execute_query(mock_conn, "INVALID SQL", "q1")

        assert result["query_id"] == "q1"
        assert result["status"] == "FAILED"
        assert result["rows_returned"] == 0
        assert result["error"] == "Query failed"
        assert result["error_type"] == "Exception"

    def test_configure_for_benchmark_olap(self, synapse_stubs):
        """OLAP configuration should set appropriate settings."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            username="admin",
            password="secret",
            disable_result_cache=True,
        )

        adapter.configure_for_benchmark(mock_conn, "olap")

        executed = " ".join(str(call) for call in mock_cursor.execute.call_args_list)
        assert "RESULT_SET_CACHING OFF" in executed
        assert "ANSI_NULLS ON" in executed

    def test_get_existing_tables(self, synapse_stubs):
        """Should query INFORMATION_SCHEMA for tables."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [("TABLE1",), ("table2",)]
        mock_conn.cursor.return_value = mock_cursor

        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            schema="dbo",
            username="admin",
            password="secret",
        )

        tables = adapter._get_existing_tables(mock_conn)

        assert tables == ["table1", "table2"]

    def test_test_connection_success(self, synapse_stubs):
        """Connection test should return True on success."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value = mock_cursor
        synapse_stubs.connect.return_value = mock_conn

        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            username="admin",
            password="secret",
        )

        assert adapter.test_connection() is True

    def test_test_connection_failure(self, synapse_stubs):
        """Connection test should return False on failure."""
        synapse_stubs.connect.side_effect = Exception("Connection refused")

        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            username="admin",
            password="secret",
        )

        assert adapter.test_connection() is False

    def test_from_config_generates_database_name(self, synapse_stubs):
        """from_config should generate database name from benchmark config."""
        config = {
            "server": "test.sql.azuresynapse.net",
            "username": "admin",
            "password": "secret",
            "benchmark": "tpch",
            "scale_factor": 10.0,
        }

        adapter = AzureSynapseAdapter.from_config(config)

        assert "tpch" in adapter.database.lower()

    def test_from_config_uses_provided_database(self, synapse_stubs):
        """from_config should use explicitly provided database name."""
        config = {
            "server": "test.sql.azuresynapse.net",
            "username": "admin",
            "password": "secret",
            "database": "my_custom_db",
            "benchmark": "tpch",
            "scale_factor": 10.0,
        }

        adapter = AzureSynapseAdapter.from_config(config)

        assert adapter.database == "my_custom_db"

    def test_supports_tuning_type(self, synapse_stubs):
        """Should report correct tuning type support."""
        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            username="admin",
            password="secret",
        )

        with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning:
            mock_tuning.DISTRIBUTION = "distribution"
            mock_tuning.PARTITIONING = "partitioning"
            mock_tuning.INDEXING = "indexing"
            mock_tuning.SORTING = "sorting"

            assert adapter.supports_tuning_type(mock_tuning.DISTRIBUTION) is True
            assert adapter.supports_tuning_type(mock_tuning.PARTITIONING) is True
            assert adapter.supports_tuning_type(mock_tuning.INDEXING) is True
            assert adapter.supports_tuning_type(mock_tuning.SORTING) is False

    def test_close_connection(self, synapse_stubs):
        """Close connection should call close on the connection."""
        mock_conn = Mock()

        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            username="admin",
            password="secret",
        )

        adapter.close_connection(mock_conn)

        mock_conn.close.assert_called_once()

    def test_dialect_is_tsql(self, synapse_stubs):
        """Dialect should be 'tsql' for SQL Server/Synapse compatibility."""
        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            username="admin",
            password="secret",
        )

        assert adapter.get_target_dialect() == "tsql"
        assert adapter._dialect == "tsql"

    def test_extract_table_name(self, synapse_stubs):
        """Should extract table name from CREATE TABLE statement."""
        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            username="admin",
            password="secret",
        )

        assert adapter._extract_table_name("CREATE TABLE test_table (id INT)") == "test_table"
        assert adapter._extract_table_name("CREATE TABLE [my_table] (id INT)") == "my_table"
        assert adapter._extract_table_name("SELECT * FROM test") is None

    def test_optimize_table_definition_adds_distribution(self, synapse_stubs):
        """Should add default DISTRIBUTION clause if not present."""
        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            username="admin",
            password="secret",
            distribution_default="ROUND_ROBIN",
        )

        statement = "CREATE TABLE test_table (id INT, name VARCHAR(100))"
        optimized = adapter._optimize_table_definition(statement)

        assert "DISTRIBUTION = ROUND_ROBIN" in optimized

    def test_missing_server_raises_error(self, synapse_stubs):
        """Should raise error when server is missing for SQL auth."""
        from benchbox.core.exceptions import ConfigurationError

        with pytest.raises(ConfigurationError, match="Azure Synapse SQL authentication is incomplete"):
            AzureSynapseAdapter(
                username="admin",
                password="secret",
            )

    def test_missing_password_raises_error(self, synapse_stubs):
        """Should raise error when password is missing for SQL auth."""
        from benchbox.core.exceptions import ConfigurationError

        with pytest.raises(ConfigurationError, match="Azure Synapse SQL authentication is incomplete"):
            AzureSynapseAdapter(
                server="test.sql.azuresynapse.net",
                username="admin",
            )


class TestAzureSynapseDataLoading:
    """Tests for Azure Synapse data loading methods."""

    def test_load_data_direct_fallback(self, synapse_stubs, tmp_path):
        """Should use direct INSERT when no storage configured."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (3,)  # Row count
        mock_conn.cursor.return_value = mock_cursor

        # Create test CSV file
        csv_file = tmp_path / "test_table.csv"
        csv_file.write_text("1,alice\n2,bob\n3,charlie\n")

        class Benchmark:
            tables = {"test_table": csv_file}

        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            schema="dbo",
            username="admin",
            password="secret",
            # No storage_account configured
        )

        stats, load_time, _ = adapter.load_data(Benchmark(), mock_conn, tmp_path)

        # Should have loaded some data
        assert "test_table" in stats
        assert load_time >= 0

    def test_load_data_skips_empty_files(self, synapse_stubs, tmp_path):
        """Should skip empty data files."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        # Create empty file
        empty_file = tmp_path / "empty.csv"
        empty_file.write_text("")

        class Benchmark:
            tables = {"empty_table": empty_file}

        adapter = AzureSynapseAdapter(
            server="test.sql.azuresynapse.net",
            schema="dbo",
            username="admin",
            password="secret",
        )

        stats, _, _ = adapter.load_data(Benchmark(), mock_conn, tmp_path)

        assert stats.get("empty_table", 0) == 0
