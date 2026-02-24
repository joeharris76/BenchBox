"""Tests for QuestDB platform adapter.

Tests the QuestDBAdapter for QuestDB time-series database support.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from unittest.mock import MagicMock, Mock, patch

import pytest

import benchbox.platforms.questdb as questdb_module
from benchbox.core.platform_registry import PlatformRegistry
from benchbox.platforms.questdb import QUESTDB_DIALECT, QuestDBAdapter

pytestmark = pytest.mark.fast


@pytest.fixture()
def questdb_stubs(monkeypatch):
    """Patch psycopg2 objects so tests don't require the real driver."""
    mock_psycopg2 = Mock()
    mock_psycopg2.__version__ = "2.9.9"
    mock_psycopg2.extras = Mock()

    monkeypatch.setattr(questdb_module, "psycopg2", mock_psycopg2)

    return mock_psycopg2


class TestQuestDBAdapter:
    """Unit tests for QuestDB adapter wiring and SQL handling."""

    def test_initialization_defaults(self, questdb_stubs):
        """Adapter should initialize with QuestDB defaults when stubs are present."""
        adapter = QuestDBAdapter()

        assert adapter.platform_name == "QuestDB"
        assert adapter.get_target_dialect() == QUESTDB_DIALECT
        assert adapter.host == "localhost"
        assert adapter.pg_port == 8812
        assert adapter.http_port == 9000
        assert adapter.database == "qdb"
        assert adapter.username == "admin"
        assert adapter.password == "quest"
        assert adapter.skip_database_management is True

    def test_initialization_with_config(self, questdb_stubs):
        """Adapter should accept custom configuration."""
        adapter = QuestDBAdapter(
            host="questdb.example.com",
            pg_port=8813,
            http_port=9001,
            database="custom_db",
            username="custom_user",
            password="secret",
        )

        assert adapter.host == "questdb.example.com"
        assert adapter.pg_port == 8813
        assert adapter.http_port == 9001
        assert adapter.database == "custom_db"
        assert adapter.username == "custom_user"
        assert adapter.password == "secret"

    def test_get_connection_params(self, questdb_stubs):
        """Connection parameters should include all required fields."""
        adapter = QuestDBAdapter(
            host="questdb.example.com",
            pg_port=8813,
            database="testdb",
            username="testuser",
            password="testpass",
            connect_timeout=15,
        )

        params = adapter._get_connection_params()

        assert params["host"] == "questdb.example.com"
        assert params["port"] == 8813
        assert params["dbname"] == "testdb"
        assert params["user"] == "testuser"
        assert params["password"] == "testpass"
        assert params["connect_timeout"] == 15

    def test_get_connection_params_no_password(self, questdb_stubs):
        """Connection parameters should omit password when not provided."""
        adapter = QuestDBAdapter(password=None)

        params = adapter._get_connection_params()

        assert "password" not in params

    def test_create_connection_sets_autocommit(self, questdb_stubs):
        """Connection should enable autocommit mode (required by QuestDB)."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value = mock_cursor
        questdb_stubs.connect.return_value = mock_conn

        adapter = QuestDBAdapter()

        with patch.object(adapter, "handle_existing_database"):
            connection = adapter.create_connection()

        assert connection is mock_conn
        assert mock_conn.autocommit is True

    def test_create_connection_verifies_connectivity(self, questdb_stubs):
        """Connection should verify connectivity with SELECT 1."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value = mock_cursor
        questdb_stubs.connect.return_value = mock_conn

        adapter = QuestDBAdapter()

        with patch.object(adapter, "handle_existing_database"):
            adapter.create_connection()

        mock_cursor.execute.assert_called_with("SELECT 1")

    def test_get_platform_info_basic(self, questdb_stubs):
        """Platform info should include QuestDB details."""
        adapter = QuestDBAdapter(
            host="questdb.example.com",
            pg_port=8812,
            http_port=9000,
        )

        info = adapter.get_platform_info()

        assert info["platform_type"] == "questdb"
        assert info["platform_name"] == "QuestDB"
        assert info["host"] == "questdb.example.com"
        assert info["pg_port"] == 8812
        assert info["http_port"] == 9000
        assert info["dialect"] == QUESTDB_DIALECT

    def test_get_platform_info_with_connection(self, questdb_stubs):
        """Platform info should include version when connection is available."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ("7.3.10",)
        mock_conn.cursor.return_value = mock_cursor

        adapter = QuestDBAdapter()

        info = adapter.get_platform_info(connection=mock_conn)

        assert info["version"] == "7.3.10"

    def test_execute_query_success(self, questdb_stubs):
        """Query execution should return correct result structure."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [(1, "test"), (2, "test2")]
        mock_conn.cursor.return_value = mock_cursor

        adapter = QuestDBAdapter()

        result = adapter.execute_query(mock_conn, "SELECT * FROM test", "q1")

        assert result["query_id"] == "q1"
        assert result["status"] == "SUCCESS"
        assert result["rows_returned"] == 2
        assert result["first_row"] == (1, "test")
        assert isinstance(result["execution_time_seconds"], float)

    def test_execute_query_failure(self, questdb_stubs):
        """Query execution failure should return error info."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.side_effect = Exception("Query failed")
        mock_conn.cursor.return_value = mock_cursor

        adapter = QuestDBAdapter()

        result = adapter.execute_query(mock_conn, "INVALID SQL", "q1")

        assert result["query_id"] == "q1"
        assert result["status"] == "FAILED"
        assert result["rows_returned"] == 0
        assert result["error"] == "Query failed"
        assert result["error_type"] == "Exception"

    def test_get_query_plan(self, questdb_stubs):
        """Query plan should use EXPLAIN."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            ("Scan on test",),
            ("  Filter: (id > 5)",),
        ]
        mock_conn.cursor.return_value = mock_cursor

        adapter = QuestDBAdapter()

        plan = adapter.get_query_plan(mock_conn, "SELECT * FROM test WHERE id > 5")

        assert "Scan" in plan
        assert "Filter" in plan
        mock_cursor.execute.assert_called_with("EXPLAIN SELECT * FROM test WHERE id > 5")

    def test_get_query_plan_failure(self, questdb_stubs):
        """Query plan failure should return error message."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.side_effect = Exception("EXPLAIN not supported")
        mock_conn.cursor.return_value = mock_cursor

        adapter = QuestDBAdapter()

        plan = adapter.get_query_plan(mock_conn, "SELECT 1")

        assert "Failed to get query plan" in plan

    def test_configure_for_benchmark(self, questdb_stubs):
        """Configure should not raise errors (QuestDB has no session-level settings)."""
        mock_conn = Mock()

        adapter = QuestDBAdapter()

        # Should not raise
        adapter.configure_for_benchmark(mock_conn, "olap")
        adapter.configure_for_benchmark(mock_conn, "oltp")

    def test_validate_identifier_valid(self, questdb_stubs):
        """Valid identifiers should pass validation."""
        adapter = QuestDBAdapter()

        assert adapter._validate_identifier("my_table") is True
        assert adapter._validate_identifier("TestTable") is True
        assert adapter._validate_identifier("_private") is True
        assert adapter._validate_identifier("tbl123") is True

    def test_validate_identifier_invalid(self, questdb_stubs):
        """Invalid identifiers should fail validation."""
        adapter = QuestDBAdapter()

        assert adapter._validate_identifier("") is False
        assert adapter._validate_identifier(None) is False
        assert adapter._validate_identifier("123abc") is False
        assert adapter._validate_identifier("tbl-name") is False
        assert adapter._validate_identifier("a" * 128) is False
        assert adapter._validate_identifier("tbl.schema") is False

    def test_adapt_drop_table_adds_if_exists(self, questdb_stubs):
        """DROP TABLE without IF EXISTS should be adapted."""
        adapter = QuestDBAdapter()

        result = adapter._adapt_drop_table("DROP TABLE lineitem")
        assert "IF EXISTS" in result
        assert "lineitem" in result

    def test_adapt_drop_table_preserves_if_exists(self, questdb_stubs):
        """DROP TABLE with IF EXISTS should remain unchanged."""
        adapter = QuestDBAdapter()

        original = "DROP TABLE IF EXISTS lineitem"
        result = adapter._adapt_drop_table(original)
        assert result == original

    def test_close_connection(self, questdb_stubs):
        """Close should close the connection without errors."""
        mock_conn = Mock()

        adapter = QuestDBAdapter()
        adapter.close_connection(mock_conn)

        mock_conn.close.assert_called_once()

    def test_close_connection_handles_error(self, questdb_stubs):
        """Close should handle connection errors gracefully."""
        mock_conn = Mock()
        mock_conn.close.side_effect = Exception("Connection already closed")

        adapter = QuestDBAdapter()
        # Should not raise
        adapter.close_connection(mock_conn)

    def test_close_connection_none(self, questdb_stubs):
        """Close should handle None connection gracefully."""
        adapter = QuestDBAdapter()
        # Should not raise
        adapter.close_connection(None)

    def test_check_database_exists_true(self, questdb_stubs):
        """Database exists check should return True when QuestDB is reachable."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value = mock_cursor
        questdb_stubs.connect.return_value = mock_conn

        adapter = QuestDBAdapter()

        assert adapter.check_database_exists() is True

    def test_check_database_exists_false(self, questdb_stubs):
        """Database exists check should return False when QuestDB is unreachable."""
        questdb_stubs.connect.side_effect = Exception("Connection refused")

        adapter = QuestDBAdapter()

        assert adapter.check_database_exists() is False

    def test_get_database_path_returns_none(self, questdb_stubs):
        """Database path should be None (server-based)."""
        adapter = QuestDBAdapter()

        assert adapter.get_database_path() is None

    def test_table_exists_true(self, questdb_stubs):
        """Table exists should return True when table is found."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = ("lineitem",)
        mock_conn.cursor.return_value = mock_cursor

        adapter = QuestDBAdapter()

        assert adapter.table_exists(mock_conn, "lineitem") is True

    def test_table_exists_false(self, questdb_stubs):
        """Table exists should return False when table is not found."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value = mock_cursor

        adapter = QuestDBAdapter()

        assert adapter.table_exists(mock_conn, "nonexistent") is False

    def test_table_exists_invalid_identifier(self, questdb_stubs):
        """Table exists should return False for invalid identifiers."""
        mock_conn = Mock()

        adapter = QuestDBAdapter()

        assert adapter.table_exists(mock_conn, "invalid;name") is False

    def test_drop_table(self, questdb_stubs):
        """Drop table should execute DROP TABLE IF EXISTS."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        adapter = QuestDBAdapter()
        adapter.drop_table(mock_conn, "test_table")

        mock_cursor.execute.assert_called_with('DROP TABLE IF EXISTS "test_table"')

    def test_drop_table_invalid_identifier(self, questdb_stubs):
        """Drop table should reject invalid identifiers."""
        mock_conn = Mock()

        adapter = QuestDBAdapter()
        # Should not raise, but should warn and skip
        adapter.drop_table(mock_conn, "invalid;name")

    def test_from_config_defaults(self, questdb_stubs):
        """from_config should use QuestDB defaults."""
        config = {}

        adapter = QuestDBAdapter.from_config(config)

        assert adapter.host == "localhost"
        assert adapter.pg_port == 8812
        assert adapter.http_port == 9000
        assert adapter.username == "admin"
        assert adapter.database == "qdb"

    def test_from_config_custom_values(self, questdb_stubs):
        """from_config should accept custom configuration."""
        config = {
            "host": "questdb.example.com",
            "pg_port": 8813,
            "http_port": 9001,
            "username": "custom_user",
            "password": "secret",
            "database": "custom_db",
        }

        adapter = QuestDBAdapter.from_config(config)

        assert adapter.host == "questdb.example.com"
        assert adapter.pg_port == 8813
        assert adapter.http_port == 9001
        assert adapter.username == "custom_user"
        assert adapter.password == "secret"
        assert adapter.database == "custom_db"

    def test_from_config_port_fallback(self, questdb_stubs):
        """from_config should use 'port' as fallback for 'pg_port'."""
        config = {"port": 8813}

        adapter = QuestDBAdapter.from_config(config)

        assert adapter.pg_port == 8813

    def test_from_config_passes_tuning_config(self, questdb_stubs):
        """from_config should pass through tuning configuration."""
        config = {
            "tuning_enabled": True,
            "verbose_enabled": True,
            "very_verbose": True,
        }

        adapter = QuestDBAdapter.from_config(config)

        assert adapter.config.get("tuning_enabled") is True

    def test_create_schema_skips_foreign_keys(self, questdb_stubs):
        """Schema creation should skip foreign key constraints."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        adapter = QuestDBAdapter()

        mock_benchmark = Mock()

        with patch.object(adapter, "_create_schema_with_tuning") as mock_schema:
            mock_schema.return_value = (
                "CREATE TABLE t1 (id INT);"
                "ALTER TABLE t1 ADD FOREIGN KEY (cid) REFERENCES t2(id);"
                "CREATE TABLE t2 (id INT)"
            )
            adapter.create_schema(mock_benchmark, mock_conn)

        # Should have executed CREATE statements but not the FOREIGN KEY one
        executed_calls = [str(call) for call in mock_cursor.execute.call_args_list]
        executed_text = " ".join(executed_calls)
        assert "CREATE TABLE t1" in executed_text
        assert "CREATE TABLE t2" in executed_text
        assert "FOREIGN KEY" not in executed_text

    def test_open_normalized_csv_stream_tpc_binary(self, questdb_stubs, tmp_path):
        """TPC files should be normalized without trailing delimiters in binary mode."""
        adapter = QuestDBAdapter()
        data_file = tmp_path / "lineitem.tbl"
        data_file.write_bytes(b"1|2|\n3|4|\n")

        with adapter._open_normalized_csv_stream(data_file) as stream:
            assert stream.read() == b"1|2\n3|4\n"

    def test_open_normalized_csv_stream_tpc_text(self, questdb_stubs, tmp_path):
        """TPC files should be normalized without trailing delimiters in text mode."""
        adapter = QuestDBAdapter()
        data_file = tmp_path / "orders.tbl"
        data_file.write_text("1|2|\n3|4|\n", encoding="utf-8")

        with adapter._open_normalized_csv_stream(data_file, text_mode=True) as stream:
            assert stream.read() == "1|2\n3|4\n"

    def test_cursor_closed_on_execute_query_failure(self, questdb_stubs):
        """Cursor should be closed even when query execution raises."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Query failed")
        mock_conn.cursor.return_value = mock_cursor

        adapter = QuestDBAdapter()
        adapter.execute_query(mock_conn, "SELECT 1", "q1")

        mock_cursor.close.assert_called_once()

    def test_cursor_closed_on_execute_query_success(self, questdb_stubs):
        """Cursor should be closed after successful query execution."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [(1,)]
        mock_conn.cursor.return_value = mock_cursor

        adapter = QuestDBAdapter()
        adapter.execute_query(mock_conn, "SELECT 1", "q1")

        mock_cursor.close.assert_called_once()

    def test_cursor_closed_on_create_schema_failure(self, questdb_stubs):
        """Cursor should be closed when schema creation fails mid-statement."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = [None, KeyboardInterrupt("interrupted")]
        mock_conn.cursor.return_value = mock_cursor

        adapter = QuestDBAdapter()

        with patch.object(adapter, "_create_schema_with_tuning") as mock_schema:
            mock_schema.return_value = "CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT)"
            with pytest.raises(KeyboardInterrupt):
                adapter.create_schema(Mock(), mock_conn)

        mock_cursor.close.assert_called_once()

    def test_create_schema_raises_on_create_table_failure(self, questdb_stubs):
        """create_schema should raise RuntimeError when CREATE TABLE statements fail."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Syntax error")
        mock_conn.cursor.return_value = mock_cursor

        adapter = QuestDBAdapter()

        with (
            patch.object(adapter, "_create_schema_with_tuning") as mock_schema,
            pytest.raises(RuntimeError, match="critical CREATE TABLE"),
        ):
            mock_schema.return_value = "CREATE TABLE lineitem (id INT)"
            adapter.create_schema(Mock(), mock_conn)

    def test_create_schema_strips_fk_constraints(self, questdb_stubs):
        """create_schema should strip FK constraints but keep the CREATE TABLE statement."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        adapter = QuestDBAdapter()

        with patch.object(adapter, "_create_schema_with_tuning") as mock_schema:
            mock_schema.return_value = (
                "CREATE TABLE orders (id INT, cust_id INT, FOREIGN KEY (cust_id) REFERENCES customer(id))"
            )
            adapter.create_schema(Mock(), mock_conn)

        # The CREATE TABLE should still be executed (FK stripped, not dropped)
        mock_cursor.execute.assert_called_once()
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "CREATE TABLE" in executed_sql
        assert "FOREIGN KEY" not in executed_sql

    def test_platform_registry_requirements_include_requests(self):
        """QuestDB platform metadata should include REST dependency guidance."""
        info = PlatformRegistry.get_platform_info("questdb")
        assert info is not None
        assert "requests>=2.28.0" in info.requirements
        assert info.installation_command == "uv add benchbox --extra questdb"


class TestQuestDBTlsUrls:
    """Tests for TLS/HTTPS URL construction in QuestDB adapter."""

    def test_use_tls_defaults_to_false(self, questdb_stubs):
        """use_tls defaults to False (HTTP)."""
        adapter = QuestDBAdapter(host="myhost", http_port=9000)
        assert adapter.use_tls is False

    def test_use_tls_can_be_enabled(self, questdb_stubs):
        """use_tls=True is stored on the adapter."""
        adapter = QuestDBAdapter(host="myhost", http_port=9000, use_tls=True)
        assert adapter.use_tls is True

    def test_imp_url_uses_http_by_default(self, questdb_stubs):
        """Import endpoint URL uses HTTP scheme by default."""
        adapter = QuestDBAdapter(host="myhost", http_port=9000)
        mock_requests = Mock()
        mock_response = Mock(status_code=200, text='{"status":"OK"}')
        mock_requests.post.return_value = mock_response

        with (
            patch.dict("sys.modules", {"requests": mock_requests}),
            patch.object(adapter, "_open_normalized_csv_stream", return_value=MagicMock()),
        ):
            adapter._load_table_via_rest_api("test_table", "/tmp/data.csv")
            url = mock_requests.post.call_args[0][0]
            assert url == "http://myhost:9000/imp"

    def test_imp_url_uses_https_when_tls_enabled(self, questdb_stubs):
        """Import endpoint URL uses HTTPS when use_tls=True."""
        adapter = QuestDBAdapter(host="myhost", http_port=9000, use_tls=True)
        mock_requests = Mock()
        mock_response = Mock(status_code=200, text='{"status":"OK"}')
        mock_requests.post.return_value = mock_response

        with (
            patch.dict("sys.modules", {"requests": mock_requests}),
            patch.object(adapter, "_open_normalized_csv_stream", return_value=MagicMock()),
        ):
            adapter._load_table_via_rest_api("test_table", "/tmp/data.csv")
            url = mock_requests.post.call_args[0][0]
            assert url == "https://myhost:9000/imp"

    def test_exec_url_uses_https_when_tls_enabled(self, questdb_stubs):
        """Exec endpoint URL uses HTTPS when use_tls=True."""
        adapter = QuestDBAdapter(host="myhost", http_port=9000, use_tls=True)
        mock_requests = Mock()
        mock_response = Mock(status_code=200)
        mock_response.json.return_value = {"dataset": [[42]]}
        mock_requests.get.return_value = mock_response

        with patch.dict("sys.modules", {"requests": mock_requests}):
            adapter._count_table_rows_via_http("test_table")
            url = mock_requests.get.call_args[0][0]
            assert url == "https://myhost:9000/exec"

    def test_from_config_passes_use_tls(self, questdb_stubs):
        """from_config propagates use_tls to adapter instance."""
        config = {
            "host": "localhost",
            "use_tls": True,
            "benchmark": "tpch",
            "scale_factor": 0.01,
        }
        adapter = QuestDBAdapter.from_config(config)
        assert adapter.use_tls is True
