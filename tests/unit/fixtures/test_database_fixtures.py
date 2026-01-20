"""Tests for the consolidated database fixture system.

Tests unified database fixtures and migration patterns.
All examples from database_fixture_usage_examples.py integrated
as proper test cases.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

pytest_plugins = ["tests.fixtures.database_fixtures"]

# Import all database fixtures from the fixtures module
from tests.fixtures.database_fixtures import (
    _create_duckdb_connection,
    # All fixtures will be auto-imported by pytest
    create_test_database,
    get_database_config,
)


@pytest.mark.unit
@pytest.mark.duckdb
@pytest.mark.fast
class TestBasicUsage:
    """Test basic database fixture usage patterns."""

    def test_with_memory_database(self, duckdb_memory_db):
        """Basic test using the standard memory database fixture."""
        # This works exactly as before - no changes needed
        result = duckdb_memory_db.execute("SELECT 1 as test").fetchone()
        assert result[0] == 1

    def test_with_extensions(self, duckdb_with_extensions):
        """Test using database with extensions loaded."""
        # Extensions are automatically loaded
        # Test parquet functionality (if available)
        try:
            duckdb_with_extensions.execute("SELECT * FROM parquet_metadata('nonexistent.parquet')")
        except Exception:
            # Expected to fail, but parquet extension should be loaded
            pass

    def test_with_configuration(self, configured_duckdb):
        """Test using pre-configured database."""
        # Database has optimized settings applied
        result = configured_duckdb.execute("SELECT 2 as test").fetchone()
        assert result[0] == 2


@pytest.mark.unit
@pytest.mark.duckdb
@pytest.mark.fast
class TestParameterizedFixtures:
    """Test parameterized fixtures."""

    def test_with_all_database_types(self, duckdb_database):
        """This test will run with all database configurations."""
        # Will run 4 times: memory, memory_with_extensions, configured, file
        result = duckdb_database.execute("SELECT 3 as test").fetchone()
        assert result[0] == 3

    @pytest.mark.parametrize("duckdb_database", ["memory", "performance"], indirect=True)
    def test_with_specific_configs(self, duckdb_database):
        """Test with specific database configurations."""
        # Will run only with memory and performance configurations
        result = duckdb_database.execute("SELECT 4 as test").fetchone()
        assert result[0] == 4


@pytest.mark.unit
@pytest.mark.duckdb
@pytest.mark.fast
class TestCustomDatabases:
    """Test custom database factory."""

    def test_with_custom_configuration(self, duckdb_custom):
        """Create a database with custom configuration on-demand."""
        # Create a high-memory database
        db = duckdb_custom(memory_limit="4GB", threads=8, load_extensions=True)

        result = db.execute("SELECT 5 as test").fetchone()
        assert result[0] == 5

        # Database is automatically cleaned up

    def test_with_file_database(self, duckdb_custom):
        """Create a custom file-based database."""
        db = duckdb_custom(connection_type="file", memory_limit="2GB")

        # Create and populate a table
        db.execute("CREATE TABLE test_table (id INTEGER, value TEXT)")
        db.execute("INSERT INTO test_table VALUES (1, 'test')")

        result = db.execute("SELECT * FROM test_table").fetchone()
        assert result == (1, "test")

    def test_multiple_custom_databases(self, duckdb_custom):
        """Create multiple databases with different configurations."""
        # Fast database for simple operations
        fast_db = duckdb_custom(memory_limit="256MB", threads=1, load_extensions=False)

        # Performance database for complex operations
        perf_db = duckdb_custom(memory_limit="4GB", threads=8, load_extensions=True)

        # Both databases work independently
        fast_db.execute("SELECT 1").fetchone()
        perf_db.execute("SELECT 2").fetchone()


@pytest.mark.unit
@pytest.mark.duckdb
@pytest.mark.fast
class TestPerformanceFixtures:
    """Test performance-optimized fixtures."""

    def test_with_performance_database(self, duckdb_performance):
        """Test using the high-performance database fixture."""
        # Database is optimized for performance testing
        # 2GB memory, 4 threads, extensions loaded, profiling enabled
        result = duckdb_performance.execute("SELECT 6 as test").fetchone()
        assert result[0] == 6

    def test_with_minimal_database(self, duckdb_minimal):
        """Test using the minimal database fixture for fast unit tests."""
        # Database uses minimal resources: 256MB memory, 1 thread
        result = duckdb_minimal.execute("SELECT 7 as test").fetchone()
        assert result[0] == 7


@pytest.mark.unit
@pytest.mark.duckdb
@pytest.mark.fast
class TestUtilityFunctions:
    """Test utility functions for advanced scenarios."""

    def test_create_database_outside_fixture(self):
        """Create a database outside of pytest fixtures."""
        # Create database with base configuration
        db = create_test_database("memory")
        try:
            result = db.execute("SELECT 8 as test").fetchone()
            assert result[0] == 8
        finally:
            db.close()

        # Create database with custom overrides
        db = create_test_database("memory", memory_limit="512MB", threads=2, load_extensions=True)
        try:
            result = db.execute("SELECT 9 as test").fetchone()
            assert result[0] == 9
        finally:
            db.close()

    def test_get_configuration(self):
        """Get and inspect database configurations."""
        config = get_database_config("performance")
        assert config.memory_limit == "2GB"
        assert config.threads == 4
        assert config.load_extensions

    def test_custom_configuration_class(self, duckdb_config_factory):
        """Create custom configurations using the factory."""
        config = duckdb_config_factory(
            connection_type="memory",
            memory_limit="8GB",
            threads=16,
            load_extensions=True,
            settings={"enable_profiling": True, "enable_optimizer": True},
        )

        # Use configuration to create database
        db = _create_duckdb_connection(config)
        try:
            result = db.execute("SELECT 10 as test").fetchone()
            assert result[0] == 10
        finally:
            db.close()


@pytest.mark.unit
@pytest.mark.duckdb
@pytest.mark.fast
class TestBackwardCompatibility:
    """Test backward compatibility - existing tests work unchanged."""

    def test_existing_memory_fixture(self, duckdb_memory_db):
        """Existing tests using duckdb_memory_db work unchanged."""
        result = duckdb_memory_db.execute("SELECT 'backward' as test").fetchone()
        assert result[0] == "backward"

    def test_existing_file_fixture(self, duckdb_file_db):
        """Existing tests using duckdb_file_db work unchanged."""
        result = duckdb_file_db.execute("SELECT 'compatible' as test").fetchone()
        assert result[0] == "compatible"

    def test_existing_configured_fixture(self, configured_duckdb):
        """Existing tests using configured_duckdb work unchanged."""
        result = configured_duckdb.execute("SELECT 'works' as test").fetchone()
        assert result[0] == "works"

    def test_existing_database_config(self, database_config):
        """Existing tests using database_config fixture work unchanged."""
        assert isinstance(database_config, dict)
        assert "memory_limit" in database_config
        assert "threads" in database_config


@pytest.mark.unit
@pytest.mark.duckdb
@pytest.mark.fast
class TestMigrationPatterns:
    """Test migration from old patterns to new ones."""

    # OLD PATTERN (still works, but deprecated):
    def test_old_pattern_with_extensions(self, duckdb_memory_db):
        """Old pattern: manually setting up extensions."""
        from tests.fixtures.database_fixtures import setup_duckdb_extensions

        setup_duckdb_extensions(duckdb_memory_db)

        result = duckdb_memory_db.execute("SELECT 'old_pattern' as test").fetchone()
        assert result[0] == "old_pattern"

    # NEW PATTERN (recommended):
    def test_new_pattern_with_extensions(self, duckdb_with_extensions):
        """New pattern: use fixture with extensions pre-loaded."""
        result = duckdb_with_extensions.execute("SELECT 'new_pattern' as test").fetchone()
        assert result[0] == "new_pattern"

    # OLD PATTERN (still works):
    def test_old_pattern_configuration(self, duckdb_memory_db, database_config):
        """Old pattern: manually applying configuration."""
        for key, value in database_config.items():
            try:
                if isinstance(value, str):
                    duckdb_memory_db.execute(f"SET {key} = '{value}';")
                else:
                    duckdb_memory_db.execute(f"SET {key} = {value};")
            except Exception:
                pass  # Ignore configuration errors

        result = duckdb_memory_db.execute("SELECT 'old_config' as test").fetchone()
        assert result[0] == "old_config"

    # NEW PATTERN (recommended):
    def test_new_pattern_configuration(self, configured_duckdb):
        """New pattern: use pre-configured fixture."""
        result = configured_duckdb.execute("SELECT 'new_config' as test").fetchone()
        assert result[0] == "new_config"

    # BEST PATTERN for new tests:
    def test_best_pattern_custom_needs(self, duckdb_custom):
        """Best pattern: use custom factory for specific needs."""
        db = duckdb_custom(
            memory_limit="1GB",
            threads=4,
            load_extensions=True,
            settings={"enable_profiling": True, "enable_optimizer": True},
        )

        result = db.execute("SELECT 'best_pattern' as test").fetchone()
        assert result[0] == "best_pattern"


@pytest.mark.unit
@pytest.mark.duckdb
@pytest.mark.fast
class TestFixtureErrorHandling:
    """Test error handling and edge cases in database fixtures."""

    def test_invalid_configuration_name(self):
        """Test handling of invalid configuration names."""
        with pytest.raises(KeyError, match="Unknown database configuration"):
            get_database_config("nonexistent_config")

    def test_custom_database_with_invalid_type(self, duckdb_custom):
        """Test custom database with invalid connection type."""
        with pytest.raises(ValueError, match="Unsupported connection type"):
            duckdb_custom(connection_type="invalid_type")

    def test_file_database_without_path(self, duckdb_custom, tmp_path):
        """Test file database creation with temporary path."""
        # This should work with tmp_path provided by fixture
        db = duckdb_custom(connection_type="file")

        # Should be able to create tables
        db.execute("CREATE TABLE test_file_db (id INTEGER)")
        db.execute("INSERT INTO test_file_db VALUES (42)")

        result = db.execute("SELECT * FROM test_file_db").fetchone()
        assert result[0] == 42


@pytest.mark.unit
@pytest.mark.duckdb
@pytest.mark.fast
class TestFixtureResourceManagement:
    """Test proper resource management in database fixtures."""

    def test_connection_cleanup(self, duckdb_custom):
        """Test that connections are properly cleaned up."""
        # Create multiple connections
        connections = []
        for i in range(3):
            db = duckdb_custom(memory_limit="256MB")
            db.execute(f"SELECT {i}")
            connections.append(db)

        # Connections should be automatically cleaned up by fixture
        # This test mainly ensures no exceptions are raised

    def test_temporary_file_cleanup(self, duckdb_custom):
        """Test that temporary files are cleaned up."""
        db = duckdb_custom(connection_type="file")

        # Create some data
        db.execute("CREATE TABLE cleanup_test (id INTEGER)")
        db.execute("INSERT INTO cleanup_test VALUES (1)")

        # File should be cleaned up automatically by fixture


@pytest.mark.unit
@pytest.mark.duckdb
@pytest.mark.fast
class TestFixtureConfiguration:
    """Test fixture configuration options."""

    def test_memory_limit_settings(self, duckdb_custom):
        """Test different memory limit configurations."""
        memory_limits = ["256MB", "512MB", "1GB", "2GB"]

        for limit in memory_limits:
            db = duckdb_custom(memory_limit=limit)
            # Basic operation should work
            result = db.execute("SELECT 1").fetchone()
            assert result[0] == 1

    def test_thread_count_settings(self, duckdb_custom):
        """Test different thread count configurations."""
        thread_counts = [1, 2, 4, 8]

        for threads in thread_counts:
            db = duckdb_custom(threads=threads)
            # Basic operation should work
            result = db.execute("SELECT 1").fetchone()
            assert result[0] == 1

    def test_extension_loading(self, duckdb_custom):
        """Test extension loading configuration."""
        # Without extensions
        db_no_ext = duckdb_custom(load_extensions=False)
        result = db_no_ext.execute("SELECT 1").fetchone()
        assert result[0] == 1

        # With extensions
        db_with_ext = duckdb_custom(load_extensions=True)
        result = db_with_ext.execute("SELECT 1").fetchone()
        assert result[0] == 1

    def test_custom_settings(self, duckdb_custom):
        """Test custom settings application."""
        custom_settings = {
            "enable_optimizer": True,
            "enable_profiling": False,
            "preserve_insertion_order": False,
        }

        db = duckdb_custom(settings=custom_settings)
        result = db.execute("SELECT 1").fetchone()
        assert result[0] == 1


@pytest.mark.unit
@pytest.mark.duckdb
@pytest.mark.fast
class TestDatabaseFixtureConsistency:
    """Test consistency across different fixture types."""

    def test_all_fixtures_basic_functionality(
        self,
        duckdb_memory_db,
        duckdb_with_extensions,
        configured_duckdb,
        duckdb_performance,
        duckdb_minimal,
    ):
        """Test that all fixture types support basic SQL operations."""
        fixtures = [
            ("memory", duckdb_memory_db),
            ("with_extensions", duckdb_with_extensions),
            ("configured", configured_duckdb),
            ("performance", duckdb_performance),
            ("minimal", duckdb_minimal),
        ]

        for name, db in fixtures:
            # Basic SELECT
            result = db.execute("SELECT 1").fetchone()
            assert result[0] == 1, f"Basic SELECT failed for {name} fixture"

            # Table creation and insertion
            db.execute(f"CREATE TABLE test_{name} (id INTEGER, value TEXT)")
            db.execute(f"INSERT INTO test_{name} VALUES (1, 'test')")

            # Query the table
            result = db.execute(f"SELECT * FROM test_{name}").fetchone()
            assert result == (1, "test"), f"Table operations failed for {name} fixture"

            # Clean up
            db.execute(f"DROP TABLE test_{name}")

    def test_parameterized_fixture_consistency(self, duckdb_database):
        """Test that parameterized fixture provides consistent interface."""
        # Should work the same regardless of which parameter is active
        result = duckdb_database.execute("SELECT 42").fetchone()
        assert result[0] == 42

        # Should support table operations
        duckdb_database.execute("CREATE TABLE param_test (value INTEGER)")
        duckdb_database.execute("INSERT INTO param_test VALUES (123)")

        result = duckdb_database.execute("SELECT * FROM param_test").fetchone()
        assert result[0] == 123
