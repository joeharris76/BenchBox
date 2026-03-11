"""Tests for pg_duckdb platform adapter.

Tests the PgDuckDBAdapter for DuckDB-accelerated PostgreSQL support.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from unittest.mock import Mock, patch

import pytest

import benchbox.platforms.pg_duckdb as pg_duckdb_module
import benchbox.platforms.postgresql as postgresql_module
from benchbox.platforms.pg_duckdb import PgDuckDBAdapter
from benchbox.platforms.postgresql import POSTGRES_DIALECT

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


@pytest.fixture()
def pg_duckdb_stubs(monkeypatch):
    """Patch psycopg2 objects so tests don't require the real driver.

    Must patch both pg_duckdb and postgresql modules since PgDuckDBAdapter
    inherits from PostgreSQLAdapter which checks for psycopg2 in its __init__.
    """
    mock_psycopg2 = Mock()
    mock_psycopg2.__version__ = "2.9.9"
    mock_psycopg2.extras = Mock()

    # Patch both modules - parent checks in postgresql module
    monkeypatch.setattr(pg_duckdb_module, "psycopg2", mock_psycopg2)
    monkeypatch.setattr(postgresql_module, "psycopg2", mock_psycopg2)

    return mock_psycopg2


class TestPgDuckDBAdapter:
    """Unit tests for pg_duckdb adapter wiring and SQL handling."""

    def test_initialization_defaults(self, pg_duckdb_stubs):
        """Adapter should initialize with pg_duckdb defaults when stubs are present."""
        adapter = PgDuckDBAdapter()

        assert adapter.platform_name == "pg_duckdb"
        assert adapter.get_target_dialect() == POSTGRES_DIALECT
        assert adapter.host == "localhost"
        assert adapter.port == 5432
        assert adapter.database == "benchbox"
        assert adapter.username == "postgres"
        assert adapter.schema == "public"
        # pg_duckdb-specific defaults
        assert adapter.force_execution is True
        assert adapter.postgres_scan_threads == 0
        assert adapter.deployment_mode == "self-hosted"

    def test_initialization_with_config(self, pg_duckdb_stubs):
        """Adapter should accept custom pg_duckdb configuration."""
        adapter = PgDuckDBAdapter(
            host="pgduckdb.example.com",
            port=5433,
            database="analytics_db",
            username="custom_user",
            password="secret",
            schema="analytics",
            force_execution=False,
            postgres_scan_threads=4,
        )

        assert adapter.host == "pgduckdb.example.com"
        assert adapter.port == 5433
        assert adapter.database == "analytics_db"
        assert adapter.username == "custom_user"
        assert adapter.password == "secret"
        assert adapter.schema == "analytics"
        assert adapter.force_execution is False
        assert adapter.postgres_scan_threads == 4

    def test_dialect_is_postgres(self, pg_duckdb_stubs):
        """pg_duckdb should use PostgreSQL dialect (compatible)."""
        adapter = PgDuckDBAdapter()

        assert adapter.get_target_dialect() == POSTGRES_DIALECT
        assert adapter.get_target_dialect() == "postgres"

    def test_from_config_basic(self, pg_duckdb_stubs):
        """from_config should create adapter with correct settings."""
        config = {
            "host": "pgduckdb.local",
            "port": 5433,
            "database": "test_analytics",
            "force_execution": True,
            "postgres_scan_threads": 8,
        }

        adapter = PgDuckDBAdapter.from_config(config)

        assert adapter.host == "pgduckdb.local"
        assert adapter.port == 5433
        assert adapter.database == "test_analytics"
        assert adapter.force_execution is True
        assert adapter.postgres_scan_threads == 8

    def test_from_config_generates_database_name(self, pg_duckdb_stubs):
        """from_config should generate database name from benchmark config."""
        config = {
            "benchmark": "tpch",
            "scale_factor": 1.0,
        }

        adapter = PgDuckDBAdapter.from_config(config)

        assert "benchbox" in adapter.database

    def test_from_config_uses_provided_database(self, pg_duckdb_stubs):
        """from_config should prefer explicit database name over generated one."""
        config = {
            "database": "explicit_db",
            "benchmark": "tpch",
            "scale_factor": 1.0,
        }

        adapter = PgDuckDBAdapter.from_config(config)

        assert adapter.database == "explicit_db"

    def test_inherits_postgresql_connection_params(self, pg_duckdb_stubs):
        """pg_duckdb adapter should inherit PostgreSQL connection parameter handling."""
        adapter = PgDuckDBAdapter(
            host="pgduckdb.example.com",
            port=5433,
            database="testdb",
            username="testuser",
            password="testpass",
            sslmode="require",
            connect_timeout=15,
        )

        params = adapter._get_connection_params()

        assert params["host"] == "pgduckdb.example.com"
        assert params["port"] == 5433
        assert params["dbname"] == "testdb"
        assert params["user"] == "testuser"
        assert params["password"] == "testpass"
        assert params["sslmode"] == "require"
        assert params["connect_timeout"] == 15

    def test_supports_tuning_type(self, pg_duckdb_stubs):
        """pg_duckdb should support same tuning types as PostgreSQL."""
        adapter = PgDuckDBAdapter()

        from benchbox.core.tuning.interface import TuningType

        assert adapter.supports_tuning_type(TuningType.PARTITIONING) is True
        assert adapter.supports_tuning_type(TuningType.CLUSTERING) is True
        assert adapter.supports_tuning_type(TuningType.PRIMARY_KEYS) is True
        assert adapter.supports_tuning_type(TuningType.FOREIGN_KEYS) is True
        # pg_duckdb does not support distribution/sorting
        assert adapter.supports_tuning_type(TuningType.DISTRIBUTION) is False
        assert adapter.supports_tuning_type(TuningType.SORTING) is False

    def test_get_platform_info_basic(self, pg_duckdb_stubs):
        """Platform info should show pg_duckdb details."""
        adapter = PgDuckDBAdapter(
            force_execution=True,
            postgres_scan_threads=4,
        )

        info = adapter.get_platform_info(connection=None)

        assert info["platform_type"] == "pg_duckdb"
        assert info["platform_name"] == "pg_duckdb"
        assert info["configuration"]["force_execution"] is True
        assert info["configuration"]["postgres_scan_threads"] == 4
        assert info["configuration"]["deployment_mode"] == "self-hosted"


class TestPgDuckDBExtensionVerification:
    """Tests for pg_duckdb extension verification in create_connection."""

    def test_create_connection_verifies_extension(self, pg_duckdb_stubs):
        """create_connection should verify pg_duckdb extension is available."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        # Extension exists
        mock_cursor.fetchone.side_effect = [
            None,  # check_server_database_exists
            None,  # check again in _create_database
            ("1.1.0",),  # extension version check
            (1,),  # verify connection
        ]

        pg_duckdb_stubs.connect.return_value = mock_conn

        adapter = PgDuckDBAdapter()

        with (
            patch.object(adapter, "check_server_database_exists", return_value=True),
            patch.object(adapter, "handle_existing_database"),
        ):
            adapter.create_connection()

        # Should have checked for extension
        calls = [str(call) for call in mock_cursor.execute.call_args_list]
        extension_check = any("pg_duckdb" in call.lower() for call in calls)
        assert extension_check

    def test_create_connection_sets_force_execution(self, pg_duckdb_stubs):
        """create_connection should set duckdb.force_execution GUC."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchone.side_effect = [
            None,  # check_server_database_exists
            None,  # check again
            ("1.0.0",),  # extension exists
            (1,),  # verify connection
        ]

        pg_duckdb_stubs.connect.return_value = mock_conn

        adapter = PgDuckDBAdapter(force_execution=True)

        with (
            patch.object(adapter, "check_server_database_exists", return_value=True),
            patch.object(adapter, "handle_existing_database"),
        ):
            adapter.create_connection()

        calls = [str(call) for call in mock_cursor.execute.call_args_list]
        force_exec_set = any("force_execution" in call for call in calls)
        assert force_exec_set

    def test_create_connection_sets_thread_count(self, pg_duckdb_stubs):
        """create_connection should set thread count when configured."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchone.side_effect = [
            None,
            None,
            ("1.0.0",),
            (1,),
        ]

        pg_duckdb_stubs.connect.return_value = mock_conn

        adapter = PgDuckDBAdapter(postgres_scan_threads=8)

        with (
            patch.object(adapter, "check_server_database_exists", return_value=True),
            patch.object(adapter, "handle_existing_database"),
        ):
            adapter.create_connection()

        calls = [str(call) for call in mock_cursor.execute.call_args_list]
        threads_set = any("threads_for_postgres_scan" in call for call in calls)
        assert threads_set

    def test_create_connection_raises_when_extension_missing(self, pg_duckdb_stubs):
        """create_connection should raise when pg_duckdb extension is not available."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        # Extension not found, CREATE EXTENSION fails, still not found
        mock_cursor.fetchone.side_effect = [
            None,  # check_server_database_exists
            None,  # check again
            None,  # extension not found
            None,  # still not found after CREATE
            (1,),  # verify connection
        ]

        pg_duckdb_stubs.connect.return_value = mock_conn

        adapter = PgDuckDBAdapter()

        with (
            patch.object(adapter, "check_server_database_exists", return_value=True),
            patch.object(adapter, "handle_existing_database"),
            pytest.raises(RuntimeError, match="pg_duckdb extension is not available"),
        ):
            adapter.create_connection()


class TestPgDuckDBMotherDuckMode:
    """Tests for MotherDuck deployment mode."""

    def test_motherduck_mode_requires_token(self, pg_duckdb_stubs, monkeypatch):
        """MotherDuck mode should require MOTHERDUCK_TOKEN."""
        monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)

        with pytest.raises(ValueError, match="MotherDuck deployment mode requires authentication token"):
            PgDuckDBAdapter(deployment_mode="motherduck")

    def test_motherduck_mode_accepts_config_token(self, pg_duckdb_stubs, monkeypatch):
        """MotherDuck mode should accept token from config."""
        monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)

        adapter = PgDuckDBAdapter(
            deployment_mode="motherduck",
            motherduck_token="test_token_123",
        )

        assert adapter.deployment_mode == "motherduck"
        assert adapter.motherduck_token == "test_token_123"

    def test_motherduck_mode_accepts_env_token(self, pg_duckdb_stubs, monkeypatch):
        """MotherDuck mode should accept token from environment variable."""
        monkeypatch.setenv("MOTHERDUCK_TOKEN", "env_token_456")

        adapter = PgDuckDBAdapter(deployment_mode="motherduck")

        assert adapter.deployment_mode == "motherduck"
        assert adapter.motherduck_token == "env_token_456"

    def test_invalid_deployment_mode_raises(self, pg_duckdb_stubs):
        """Invalid deployment mode should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid pg_duckdb deployment mode"):
            PgDuckDBAdapter(deployment_mode="invalid")

    def test_create_connection_sets_motherduck_token(self, pg_duckdb_stubs, monkeypatch):
        """create_connection should set duckdb.motherduck_token in motherduck mode."""
        monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchone.side_effect = [
            None,
            None,
            ("1.0.0",),  # extension exists
            (1,),
        ]

        pg_duckdb_stubs.connect.return_value = mock_conn

        adapter = PgDuckDBAdapter(
            deployment_mode="motherduck",
            motherduck_token="md_token_789",
        )

        with (
            patch.object(adapter, "check_server_database_exists", return_value=True),
            patch.object(adapter, "handle_existing_database"),
        ):
            adapter.create_connection()

        calls = [str(call) for call in mock_cursor.execute.call_args_list]
        token_set = any("motherduck_token" in call for call in calls)
        assert token_set

    def test_create_connection_mogrify_uses_existing_cursor(self, pg_duckdb_stubs, monkeypatch):
        """mogrify() should use existing cursor, not create a new orphaned one."""
        monkeypatch.delenv("MOTHERDUCK_TOKEN", raising=False)

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.mogrify.return_value = b"'md_token_789'"
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchone.side_effect = [
            None,
            None,
            ("1.0.0",),
            (1,),
        ]

        pg_duckdb_stubs.connect.return_value = mock_conn

        adapter = PgDuckDBAdapter(
            deployment_mode="motherduck",
            motherduck_token="md_token_789",
        )

        with (
            patch.object(adapter, "check_server_database_exists", return_value=True),
            patch.object(adapter, "handle_existing_database"),
        ):
            adapter.create_connection()

        # mogrify should be called on the same cursor, not via conn.cursor().mogrify()
        mock_cursor.mogrify.assert_called_once()


class TestPgDuckDBRegistration:
    """Tests for pg_duckdb platform registration."""

    def test_pg_duckdb_in_platform_registry(self, pg_duckdb_stubs):
        """pg_duckdb should be registered in platform registry."""
        from benchbox.core.platform_registry import PlatformRegistry, auto_register_platforms

        auto_register_platforms()

        assert "pg-duckdb" in PlatformRegistry._adapters
        assert PlatformRegistry._adapters["pg-duckdb"] == PgDuckDBAdapter

    def test_pg_duckdb_metadata(self, pg_duckdb_stubs):
        """pg_duckdb should have correct metadata in registry."""
        from benchbox.core.platform_registry import PlatformRegistry

        metadata = PlatformRegistry._build_platform_metadata()

        assert "pg-duckdb" in metadata
        assert metadata["pg-duckdb"]["display_name"] == "pg_duckdb"
        assert metadata["pg-duckdb"]["category"] == "olap"
        assert "olap" in metadata["pg-duckdb"]["supports"]
        assert "analytics" in metadata["pg-duckdb"]["supports"]


class TestPgDuckDBConfigBuilder:
    """Tests for pg_duckdb configuration builder function."""

    def test_config_builder_basic(self, pg_duckdb_stubs):
        """Config builder should produce correct configuration."""
        from benchbox.platforms.pg_duckdb import _build_pg_duckdb_config

        benchmark_config = {"scale_factor": 1.0}
        platform_options = {
            "host": "localhost",
            "port": 5432,
            "force_execution": True,
            "postgres_scan_threads": 4,
        }

        config = _build_pg_duckdb_config(benchmark_config, platform_options)

        assert config["host"] == "localhost"
        assert config["port"] == 5432
        assert config["force_execution"] is True
        assert config["postgres_scan_threads"] == 4
        assert config["scale_factor"] == 1.0

    def test_config_builder_defaults(self, pg_duckdb_stubs):
        """Config builder should apply defaults for missing options."""
        from benchbox.platforms.pg_duckdb import _build_pg_duckdb_config

        config = _build_pg_duckdb_config({}, {})

        assert config["host"] == "localhost"
        assert config["port"] == 5432
        assert config["username"] == "postgres"
        assert config["force_execution"] is True
        assert config["postgres_scan_threads"] == 0


class TestPgDuckDBFromConfigPassthrough:
    """Test from_config passes through deployment_mode and motherduck_token."""

    def test_from_config_passes_deployment_mode(self, pg_duckdb_stubs):
        """Test that from_config passes deployment_mode to adapter."""
        from benchbox.platforms.pg_duckdb import PgDuckDBAdapter

        config = {"deployment_mode": "motherduck", "motherduck_token": "test_token_123"}
        adapter = PgDuckDBAdapter.from_config(config)

        assert adapter.deployment_mode == "motherduck"
        assert adapter.motherduck_token == "test_token_123"

    def test_from_config_default_deployment_mode(self, pg_duckdb_stubs):
        """Test that from_config defaults to self-hosted deployment mode."""
        from benchbox.platforms.pg_duckdb import PgDuckDBAdapter

        adapter = PgDuckDBAdapter.from_config({})

        assert adapter.deployment_mode == "self-hosted"


class TestPgDuckDBCreateConnectionReraise:
    """Test that create_connection re-raises on extension configuration failure."""

    def test_create_connection_reraises_on_guc_failure(self, pg_duckdb_stubs):
        """Test that GUC configuration failures are re-raised, not swallowed."""
        from benchbox.platforms.pg_duckdb import PgDuckDBAdapter

        adapter = PgDuckDBAdapter(host="localhost", database="test")

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        # Extension check succeeds, but GUC SET fails
        mock_cursor.fetchone.return_value = ("0.1.0",)
        mock_cursor.execute.side_effect = [
            None,  # SELECT extversion
            Exception("GUC not available"),  # SET duckdb.force_execution
        ]

        # Mock the parent create_connection to return our mock connection
        with patch.object(type(adapter).__bases__[0], "create_connection", return_value=mock_conn):
            with pytest.raises(RuntimeError, match="pg_duckdb configuration failed"):
                adapter.create_connection()
