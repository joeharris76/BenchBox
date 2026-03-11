"""Tests for pg_mooncake platform adapter.

Tests the PgMooncakeAdapter for columnstore PostgreSQL support.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from unittest.mock import Mock, patch

import pytest

import benchbox.platforms.pg_mooncake as pg_mooncake_module
import benchbox.platforms.postgresql as postgresql_module
from benchbox.platforms.pg_mooncake import PgMooncakeAdapter
from benchbox.platforms.postgresql import POSTGRES_DIALECT

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


@pytest.fixture()
def pg_mooncake_stubs(monkeypatch):
    """Patch psycopg2 objects so tests don't require the real driver.

    Must patch both pg_mooncake and postgresql modules since PgMooncakeAdapter
    inherits from PostgreSQLAdapter which checks for psycopg2 in its __init__.
    """
    mock_psycopg2 = Mock()
    mock_psycopg2.__version__ = "2.9.9"
    mock_psycopg2.extras = Mock()

    # Patch both modules - parent checks in postgresql module
    monkeypatch.setattr(pg_mooncake_module, "psycopg2", mock_psycopg2)
    monkeypatch.setattr(postgresql_module, "psycopg2", mock_psycopg2)

    return mock_psycopg2


class TestPgMooncakeAdapter:
    """Unit tests for pg_mooncake adapter wiring and SQL handling."""

    def test_initialization_defaults(self, pg_mooncake_stubs):
        """Adapter should initialize with pg_mooncake defaults when stubs are present."""
        adapter = PgMooncakeAdapter()

        assert adapter.platform_name == "pg_mooncake"
        assert adapter.get_target_dialect() == POSTGRES_DIALECT
        assert adapter.host == "localhost"
        assert adapter.port == 5432
        assert adapter.database == "benchbox"
        assert adapter.username == "postgres"
        assert adapter.schema == "public"
        # pg_mooncake-specific defaults
        assert adapter.storage_mode == "local"
        assert adapter.mooncake_bucket is None

    def test_initialization_with_config(self, pg_mooncake_stubs):
        """Adapter should accept custom pg_mooncake configuration."""
        adapter = PgMooncakeAdapter(
            host="mooncake.example.com",
            port=5433,
            database="analytics_db",
            username="custom_user",
            password="secret",
            schema="analytics",
            storage_mode="local",
        )

        assert adapter.host == "mooncake.example.com"
        assert adapter.port == 5433
        assert adapter.database == "analytics_db"
        assert adapter.username == "custom_user"
        assert adapter.password == "secret"
        assert adapter.schema == "analytics"
        assert adapter.storage_mode == "local"

    def test_dialect_is_postgres(self, pg_mooncake_stubs):
        """pg_mooncake should use PostgreSQL dialect (compatible)."""
        adapter = PgMooncakeAdapter()

        assert adapter.get_target_dialect() == POSTGRES_DIALECT
        assert adapter.get_target_dialect() == "postgres"

    def test_from_config_basic(self, pg_mooncake_stubs):
        """from_config should create adapter with correct settings."""
        config = {
            "host": "mooncake.local",
            "port": 5433,
            "database": "test_analytics",
            "storage_mode": "local",
        }

        adapter = PgMooncakeAdapter.from_config(config)

        assert adapter.host == "mooncake.local"
        assert adapter.port == 5433
        assert adapter.database == "test_analytics"
        assert adapter.storage_mode == "local"

    def test_from_config_generates_database_name(self, pg_mooncake_stubs):
        """from_config should generate database name from benchmark config."""
        config = {
            "benchmark": "tpch",
            "scale_factor": 1.0,
        }

        adapter = PgMooncakeAdapter.from_config(config)

        assert "benchbox" in adapter.database

    def test_from_config_uses_provided_database(self, pg_mooncake_stubs):
        """from_config should prefer explicit database name over generated one."""
        config = {
            "database": "explicit_db",
            "benchmark": "tpch",
            "scale_factor": 1.0,
        }

        adapter = PgMooncakeAdapter.from_config(config)

        assert adapter.database == "explicit_db"

    def test_inherits_postgresql_connection_params(self, pg_mooncake_stubs):
        """pg_mooncake adapter should inherit PostgreSQL connection parameter handling."""
        adapter = PgMooncakeAdapter(
            host="mooncake.example.com",
            port=5433,
            database="testdb",
            username="testuser",
            password="testpass",
            sslmode="require",
            connect_timeout=15,
        )

        params = adapter._get_connection_params()

        assert params["host"] == "mooncake.example.com"
        assert params["port"] == 5433
        assert params["dbname"] == "testdb"
        assert params["user"] == "testuser"
        assert params["password"] == "testpass"
        assert params["sslmode"] == "require"
        assert params["connect_timeout"] == 15

    def test_supports_tuning_type(self, pg_mooncake_stubs):
        """pg_mooncake columnstore tables should not support most PostgreSQL tuning."""
        adapter = PgMooncakeAdapter()

        from benchbox.core.tuning.interface import TuningType

        # Columnstore tables don't support PostgreSQL tuning
        assert adapter.supports_tuning_type(TuningType.PARTITIONING) is False
        assert adapter.supports_tuning_type(TuningType.SORTING) is False
        assert adapter.supports_tuning_type(TuningType.DISTRIBUTION) is False
        assert adapter.supports_tuning_type(TuningType.CLUSTERING) is False
        assert adapter.supports_tuning_type(TuningType.PRIMARY_KEYS) is False
        assert adapter.supports_tuning_type(TuningType.FOREIGN_KEYS) is False

    def test_get_platform_info_basic(self, pg_mooncake_stubs):
        """Platform info should show pg_mooncake details."""
        adapter = PgMooncakeAdapter(storage_mode="local")

        info = adapter.get_platform_info(connection=None)

        assert info["platform_type"] == "pg_mooncake"
        assert info["platform_name"] == "pg_mooncake"
        assert info["configuration"]["storage_mode"] == "local"


class TestPgMooncakeColumnstoreDDL:
    """Tests for columnstore DDL generation."""

    def test_add_columnstore_basic(self, pg_mooncake_stubs):
        """_add_columnstore_access_method should add USING columnstore."""
        adapter = PgMooncakeAdapter()

        result = adapter._add_columnstore_access_method("CREATE TABLE foo (id INT, name TEXT);")

        assert "USING columnstore" in result
        assert result.endswith(";")

    def test_add_columnstore_no_semicolon(self, pg_mooncake_stubs):
        """Should handle DDL without trailing semicolon."""
        adapter = PgMooncakeAdapter()

        result = adapter._add_columnstore_access_method("CREATE TABLE foo (id INT)")

        assert "USING columnstore" in result

    def test_add_columnstore_already_present(self, pg_mooncake_stubs):
        """Should not double-add USING columnstore."""
        adapter = PgMooncakeAdapter()

        ddl = "CREATE TABLE foo (id INT) USING columnstore;"
        result = adapter._add_columnstore_access_method(ddl)

        assert result.count("USING columnstore") == 1

    def test_add_columnstore_skips_non_create_table(self, pg_mooncake_stubs):
        """Should not modify non-CREATE TABLE statements."""
        adapter = PgMooncakeAdapter()

        # ALTER TABLE should be unchanged
        alter = "ALTER TABLE foo ADD COLUMN bar TEXT;"
        assert adapter._add_columnstore_access_method(alter) == alter

        # CREATE INDEX should be unchanged
        index = "CREATE INDEX idx ON foo (id);"
        assert adapter._add_columnstore_access_method(index) == index

    def test_create_schema_adds_columnstore(self, pg_mooncake_stubs):
        """create_schema should add USING columnstore to CREATE TABLE DDL."""
        adapter = PgMooncakeAdapter()

        ddl_list = [
            "CREATE TABLE foo (id INT, name TEXT);",
            "CREATE INDEX idx_foo ON foo (id);",
            "CREATE TABLE bar (x BIGINT, y DECIMAL(10,2));",
        ]

        # We can't call create_schema directly without a real connection,
        # but we can verify the DDL modification logic
        modified = []
        for stmt in ddl_list:
            modified.append(adapter._add_columnstore_access_method(stmt))

        # CREATE TABLE statements should have USING columnstore
        assert "USING columnstore" in modified[0]
        assert "USING columnstore" in modified[2]
        # CREATE INDEX should be unchanged
        assert "USING columnstore" not in modified[1]


class TestPgMooncakeExtensionVerification:
    """Tests for pg_mooncake extension verification in create_connection."""

    def test_create_connection_verifies_extension(self, pg_mooncake_stubs):
        """create_connection should verify pg_mooncake extension is available."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchone.side_effect = [
            None,
            None,
            ("0.5.0",),  # extension exists
            (1,),
        ]

        pg_mooncake_stubs.connect.return_value = mock_conn

        adapter = PgMooncakeAdapter()

        with (
            patch.object(adapter, "check_server_database_exists", return_value=True),
            patch.object(adapter, "handle_existing_database"),
        ):
            adapter.create_connection()

        calls = [str(call) for call in mock_cursor.execute.call_args_list]
        extension_check = any("pg_mooncake" in call.lower() for call in calls)
        assert extension_check

    def test_create_connection_raises_when_extension_missing(self, pg_mooncake_stubs):
        """create_connection should raise when pg_mooncake is not available."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchone.side_effect = [
            None,
            None,
            None,  # extension not found
            None,  # still not found after CREATE
            (1,),
        ]

        pg_mooncake_stubs.connect.return_value = mock_conn

        adapter = PgMooncakeAdapter()

        with (
            patch.object(adapter, "check_server_database_exists", return_value=True),
            patch.object(adapter, "handle_existing_database"),
            pytest.raises(RuntimeError, match="pg_mooncake extension is not available"),
        ):
            adapter.create_connection()


class TestPgMooncakeStorageConfig:
    """Tests for storage mode configuration."""

    def test_s3_mode_requires_bucket(self, pg_mooncake_stubs, monkeypatch):
        """S3 mode should require bucket configuration."""
        monkeypatch.delenv("MOONCAKE_S3_BUCKET", raising=False)

        with pytest.raises(ValueError, match="S3 storage mode requires bucket configuration"):
            PgMooncakeAdapter(storage_mode="s3")

    def test_s3_mode_accepts_config_bucket(self, pg_mooncake_stubs, monkeypatch):
        """S3 mode should accept bucket from config."""
        monkeypatch.delenv("MOONCAKE_S3_BUCKET", raising=False)

        adapter = PgMooncakeAdapter(
            storage_mode="s3",
            mooncake_bucket="s3://my-bucket/mooncake-data",
        )

        assert adapter.storage_mode == "s3"
        assert adapter.mooncake_bucket == "s3://my-bucket/mooncake-data"

    def test_s3_mode_accepts_env_bucket(self, pg_mooncake_stubs, monkeypatch):
        """S3 mode should accept bucket from environment variable."""
        monkeypatch.setenv("MOONCAKE_S3_BUCKET", "s3://env-bucket/data")

        adapter = PgMooncakeAdapter(storage_mode="s3")

        assert adapter.storage_mode == "s3"
        assert adapter.mooncake_bucket == "s3://env-bucket/data"

    def test_invalid_storage_mode_raises(self, pg_mooncake_stubs):
        """Invalid storage mode should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid pg_mooncake storage mode"):
            PgMooncakeAdapter(storage_mode="gcs")

    def test_create_connection_sets_bucket(self, pg_mooncake_stubs, monkeypatch):
        """create_connection should set mooncake.default_bucket in S3 mode."""
        monkeypatch.delenv("MOONCAKE_S3_BUCKET", raising=False)

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchone.side_effect = [
            None,
            None,
            ("0.5.0",),  # extension exists
            (1,),
        ]

        pg_mooncake_stubs.connect.return_value = mock_conn

        adapter = PgMooncakeAdapter(
            storage_mode="s3",
            mooncake_bucket="s3://test-bucket/data",
        )

        with (
            patch.object(adapter, "check_server_database_exists", return_value=True),
            patch.object(adapter, "handle_existing_database"),
        ):
            adapter.create_connection()

        calls = [str(call) for call in mock_cursor.execute.call_args_list]
        bucket_set = any("default_bucket" in call for call in calls)
        assert bucket_set

    def test_create_connection_mogrify_uses_existing_cursor(self, pg_mooncake_stubs, monkeypatch):
        """mogrify() should use existing cursor, not create a new orphaned one."""
        monkeypatch.delenv("MOONCAKE_S3_BUCKET", raising=False)

        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.mogrify.return_value = b"'s3://test-bucket/data'"
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchone.side_effect = [
            None,
            None,
            ("0.5.0",),
            (1,),
        ]

        pg_mooncake_stubs.connect.return_value = mock_conn

        adapter = PgMooncakeAdapter(
            storage_mode="s3",
            mooncake_bucket="s3://test-bucket/data",
        )

        with (
            patch.object(adapter, "check_server_database_exists", return_value=True),
            patch.object(adapter, "handle_existing_database"),
        ):
            adapter.create_connection()

        # mogrify should be called on the same cursor, not via conn.cursor().mogrify()
        mock_cursor.mogrify.assert_called_once()


class TestPgMooncakeRegistration:
    """Tests for pg_mooncake platform registration."""

    def test_pg_mooncake_in_platform_registry(self, pg_mooncake_stubs):
        """pg_mooncake should be registered in platform registry."""
        from benchbox.core.platform_registry import PlatformRegistry, auto_register_platforms

        auto_register_platforms()

        assert "pg-mooncake" in PlatformRegistry._adapters
        assert PlatformRegistry._adapters["pg-mooncake"] == PgMooncakeAdapter

    def test_pg_mooncake_metadata(self, pg_mooncake_stubs):
        """pg_mooncake should have correct metadata in registry."""
        from benchbox.core.platform_registry import PlatformRegistry

        metadata = PlatformRegistry._build_platform_metadata()

        assert "pg-mooncake" in metadata
        assert metadata["pg-mooncake"]["display_name"] == "pg_mooncake"
        assert metadata["pg-mooncake"]["category"] == "olap"
        assert "columnstore" in metadata["pg-mooncake"]["supports"]


class TestPgMooncakeConfigBuilder:
    """Tests for pg_mooncake configuration builder function."""

    def test_config_builder_basic(self, pg_mooncake_stubs):
        """Config builder should produce correct configuration."""
        from benchbox.platforms.pg_mooncake import _build_pg_mooncake_config

        benchmark_config = {"scale_factor": 1.0}
        platform_options = {
            "host": "localhost",
            "port": 5432,
            "storage_mode": "local",
        }

        config = _build_pg_mooncake_config(benchmark_config, platform_options)

        assert config["host"] == "localhost"
        assert config["port"] == 5432
        assert config["storage_mode"] == "local"
        assert config["scale_factor"] == 1.0

    def test_config_builder_defaults(self, pg_mooncake_stubs):
        """Config builder should apply defaults for missing options."""
        from benchbox.platforms.pg_mooncake import _build_pg_mooncake_config

        config = _build_pg_mooncake_config({}, {})

        assert config["host"] == "localhost"
        assert config["port"] == 5432
        assert config["username"] == "postgres"
        assert config["storage_mode"] == "local"
        assert config["mooncake_bucket"] is None
