"""Tests for Databricks platform adapter.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, call, patch

import pytest

from benchbox.platforms.databricks import DatabricksAdapter

pytestmark = pytest.mark.fast


@pytest.fixture(autouse=True)
def databricks_dependencies():
    """Mock Databricks dependency check to simulate installed extras."""

    # Patch where check_platform_dependencies is actually called (in the adapter module)
    with patch("benchbox.platforms.databricks.adapter.check_platform_dependencies", return_value=(True, [])):
        yield


class TestDatabricksAdapter:
    """Test Databricks platform adapter functionality."""

    def test_initialization_success(self):
        """Test successful adapter initialization."""
        with patch("benchbox.platforms.databricks.adapter.databricks_sql"):
            adapter = DatabricksAdapter(
                server_hostname="test.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/test",
                access_token="test_token",
                catalog="test_catalog",
                schema="test_schema",
                staging_root="dbfs:/Volumes/workspace/tmp",
            )
            assert adapter.platform_name == "Databricks"
            assert adapter.get_target_dialect() == "databricks"
            assert adapter.server_hostname == "test.cloud.databricks.com"
            assert adapter.http_path == "/sql/1.0/warehouses/test"
            assert adapter.catalog == "test_catalog"
            assert adapter.schema == "test_schema"

    def test_initialization_with_defaults(self):
        """Test initialization with default configuration."""
        with patch("benchbox.platforms.databricks.adapter.databricks_sql"):
            adapter = DatabricksAdapter(
                server_hostname="test.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/test",
                access_token="test_token",
            )
            assert adapter.catalog == "main"
            assert adapter.schema == "benchbox"
            assert adapter.enable_delta_optimization is True
            assert adapter.delta_auto_optimize is True
            assert adapter.delta_auto_compact is True

    def test_initialization_missing_driver(self):
        """Test initialization when Databricks dependencies are missing."""
        with (
            patch(
                "benchbox.platforms.databricks.adapter.check_platform_dependencies",
                return_value=(False, ["databricks-sql-connector"]),
            ),
            pytest.raises(ImportError) as excinfo,
        ):
            DatabricksAdapter()

        assert "Missing dependencies for databricks platform" in str(excinfo.value)

    def test_initialization_missing_required_config(self):
        """Test initialization with missing required configuration."""
        from benchbox.core.exceptions import ConfigurationError

        with patch("benchbox.platforms.databricks.adapter.databricks_sql"):
            with pytest.raises(ConfigurationError, match="Databricks configuration is incomplete"):
                DatabricksAdapter()  # Missing required fields

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_get_connection_params(self, mock_databricks_sql):
        """Test connection parameter configuration."""
        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
            catalog="test_catalog",
        )

        params = adapter._get_connection_params()

        assert params["server_hostname"] == "test.cloud.databricks.com"
        assert params["http_path"] == "/sql/1.0/warehouses/test"
        assert params["access_token"] == "test_token"

        # Test with overrides
        override_params = adapter._get_connection_params(
            server_hostname="override.databricks.com", access_token="override_token"
        )
        assert override_params["server_hostname"] == "override.databricks.com"
        assert override_params["access_token"] == "override_token"
        assert override_params["http_path"] == "/sql/1.0/warehouses/test"  # Should keep original

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_from_config_with_explicit_schema(self, mock_databricks_sql):
        """Test from_config() with explicitly provided schema name."""
        config = {
            "server_hostname": "test.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/test",
            "access_token": "test_token",
            "catalog": "test_catalog",
            "schema": "my_explicit_schema",
            "benchmark": "tpcds",
            "scale_factor": 1,
        }

        adapter = DatabricksAdapter.from_config(config)

        # Should use the explicitly provided schema name
        assert adapter.schema == "my_explicit_schema"
        assert adapter.catalog == "test_catalog"

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_from_config_with_none_schema_generates_name(self, mock_databricks_sql):
        """Test from_config() with None schema auto-generates schema name.

        This is a regression test for the Databricks harmonization fix.
        When --schema is not provided (default=None), the adapter should
        auto-generate an intelligent schema name based on benchmark, scale,
        and tuning configuration, NOT fall back to 'benchbox'.

        See: databricks_harmonization_summary.md
        """
        config = {
            "server_hostname": "test.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/test",
            "access_token": "test_token",
            "catalog": "test_catalog",
            "schema": None,  # This is what CLI provides when --schema is not specified
            "benchmark": "tpcds",
            "scale_factor": 1,
        }

        adapter = DatabricksAdapter.from_config(config)

        # Should auto-generate schema name, NOT use 'benchbox' fallback
        assert adapter.schema != "benchbox", (
            "Schema should be auto-generated, not fall back to 'benchbox'. "
            "This indicates a regression in the schema name generation logic."
        )
        # Should contain benchmark name
        assert "tpcds" in adapter.schema, (
            f"Generated schema name '{adapter.schema}' should contain benchmark name 'tpcds'"
        )
        # Should contain scale factor
        assert "sf1" in adapter.schema, f"Generated schema name '{adapter.schema}' should contain scale factor 'sf1'"

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_from_config_without_schema_key_generates_name(self, mock_databricks_sql):
        """Test from_config() without schema key auto-generates schema name."""
        config = {
            "server_hostname": "test.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/test",
            "access_token": "test_token",
            "catalog": "test_catalog",
            # schema key not provided at all
            "benchmark": "tpch",
            "scale_factor": 10,
        }

        adapter = DatabricksAdapter.from_config(config)

        # Should auto-generate schema name, NOT use 'benchbox' fallback
        assert adapter.schema != "benchbox"
        assert "tpch" in adapter.schema
        assert "sf10" in adapter.schema

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_from_config_schema_generation_with_tuning(self, mock_databricks_sql):
        """Test schema name generation includes tuning configuration."""
        config = {
            "server_hostname": "test.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/test",
            "access_token": "test_token",
            "schema": None,
            "benchmark": "tpcds",
            "scale_factor": 1,
            "tuning_config": {
                "name": "optimized",
                "indexes": True,
            },
        }

        adapter = DatabricksAdapter.from_config(config)

        # Should include tuning config name in generated schema
        assert adapter.schema != "benchbox"
        assert "tpcds" in adapter.schema
        assert "optimized" in adapter.schema or "tuning" in adapter.schema

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_from_config_with_benchbox_default_generates_name(self, mock_databricks_sql):
        """Test from_config() with 'benchbox' default schema auto-generates.

        This is a regression test for credentials file containing schema: benchbox.
        When schema is 'benchbox' (the default from credentials), and benchmark
        context is available, the adapter should auto-generate a proper schema name
        instead of using the default.

        Previously, the credentials default was incorrectly treated as an explicit
        override, causing all tables to be created in the 'benchbox' schema instead
        of properly named schemas like 'tpcds_sf1_notuning_uniq_check'.
        """
        config = {
            "server_hostname": "test.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/test",
            "access_token": "test_token",
            "catalog": "workspace",
            "schema": "benchbox",  # This is the default from credentials file
            "benchmark": "tpcds",
            "scale_factor": 1,
        }

        adapter = DatabricksAdapter.from_config(config)

        # Should auto-generate schema name, NOT use 'benchbox' default
        assert adapter.schema != "benchbox", (
            "Schema should be auto-generated, not use 'benchbox' default from credentials"
        )
        assert adapter.schema.startswith("tpcds_sf1"), f"Schema should start with benchmark and scale: {adapter.schema}"

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_create_admin_connection(self, mock_databricks_sql):
        """Test admin connection creation."""
        mock_connection = Mock()
        mock_databricks_sql.connect.return_value = mock_connection

        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
        )

        admin_conn = adapter._create_admin_connection()

        assert admin_conn == mock_connection
        mock_databricks_sql.connect.assert_called_once()
        call_kwargs = mock_databricks_sql.connect.call_args[1]
        assert call_kwargs["server_hostname"] == "test.cloud.databricks.com"
        assert call_kwargs["http_path"] == "/sql/1.0/warehouses/test"
        assert call_kwargs["access_token"] == "test_token"

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_check_server_database_exists_true(self, mock_databricks_sql):
        """Test database existence check when catalog/schema exists."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        # Mock SHOW CATALOGS and SHOW SCHEMAS responses
        mock_cursor.fetchall.side_effect = [
            [["test_catalog"], ["other_catalog"]],  # SHOW CATALOGS
            [["benchbox"], ["other_schema"]],  # SHOW SCHEMAS IN test_catalog
        ]
        mock_databricks_sql.connect.return_value = mock_connection

        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
            catalog="test_catalog",
        )

        exists = adapter.check_server_database_exists()

        assert exists is True
        # Should call both SHOW CATALOGS and SHOW SCHEMAS
        mock_cursor.execute.assert_any_call("SHOW CATALOGS")
        mock_cursor.execute.assert_any_call("SHOW SCHEMAS IN test_catalog")
        # Connection is closed in finally block
        mock_connection.close.assert_called_once()

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_check_server_database_exists_false(self, mock_databricks_sql):
        """Test database existence check when catalog doesn't exist."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [["other_catalog"]]
        mock_databricks_sql.connect.return_value = mock_connection

        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
            catalog="test_catalog",
        )

        exists = adapter.check_server_database_exists()

        assert exists is False
        mock_connection.close.assert_called_once()

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_check_server_database_exists_connection_error(self, mock_databricks_sql):
        """Test database existence check with connection error."""
        mock_databricks_sql.connect.side_effect = Exception("Connection failed")

        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
        )

        exists = adapter.check_server_database_exists()

        assert exists is False

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_drop_database(self, mock_databricks_sql):
        """Test catalog/schema dropping."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_databricks_sql.connect.return_value = mock_connection

        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
            catalog="test_catalog",
        )

        adapter.drop_database()

        # Should drop schema (Databricks doesn't drop catalogs, only schemas)
        mock_cursor.execute.assert_called_with("DROP SCHEMA IF EXISTS test_catalog.benchbox CASCADE")
        # Connection is closed in finally block
        mock_connection.close.assert_called_once()

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_create_connection_success(self, mock_databricks_sql):
        """Test successful connection creation."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_databricks_sql.connect.return_value = mock_connection

        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
            catalog="test_catalog",
            schema="test_schema",
        )

        # Mock handle_existing_database
        with patch.object(adapter, "handle_existing_database"):
            connection = adapter.create_connection()

        assert connection == mock_connection
        mock_databricks_sql.connect.assert_called_once()

        # Check catalog was set (schema is set in create_schema(), not here)
        expected_calls = [
            call("USE CATALOG test_catalog"),
            call("SELECT 1"),  # Connection test
        ]
        for expected_call in expected_calls:
            mock_cursor.execute.assert_any_call(expected_call.args[0])

        # Note: cursor is not closed in create_connection - connection stays open

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_create_connection_failure(self, mock_databricks_sql):
        """Test connection creation failure."""
        mock_databricks_sql.connect.side_effect = Exception("Connection failed")

        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
        )

        with patch.object(adapter, "handle_existing_database"):
            with pytest.raises(Exception, match="Connection failed"):
                adapter.create_connection()

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_create_schema(self, mock_databricks_sql):
        """Test schema creation with Unity Catalog."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        mock_benchmark = Mock()
        mock_benchmark.get_create_tables_sql.return_value = """
            CREATE TABLE table1 (id BIGINT, name STRING);
            CREATE TABLE table2 (id BIGINT, data STRING);
        """

        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
            catalog="test_catalog",
            schema="test_schema",
            create_catalog=True,  # Enable catalog creation for this test
        )

        # Mock translate_sql method
        with patch.object(adapter, "translate_sql") as mock_translate:
            mock_translate.return_value = (
                "CREATE TABLE table1 (id BIGINT, name STRING);\nCREATE TABLE table2 (id BIGINT, data STRING);"
            )

            schema_time = adapter.create_schema(mock_benchmark, mock_connection)

        assert isinstance(schema_time, float)
        assert schema_time >= 0

        # Should execute catalog and schema setup
        setup_calls = list(mock_cursor.execute.call_args_list)
        setup_sqls = [str(call) for call in setup_calls]

        # Should include catalog creation, schema creation, and table creation
        assert any("CREATE CATALOG" in sql for sql in setup_sqls)
        assert any("CREATE SCHEMA" in sql for sql in setup_sqls)

        mock_cursor.close.assert_called_once()

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_load_data_with_delta_tables(self, mock_databricks_sql):
        """Test data loading using Delta Lake format."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock SHOW TABLES query (returns test_table)
        mock_cursor.fetchall.return_value = [("test_schema", "test_table", False)]
        # Mock row count query
        mock_cursor.fetchone.return_value = (100,)

        mock_benchmark = Mock()

        # Create temporary test file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name\n1,test1\n2,test2\n")
            temp_path = Path(f.name)

        try:
            mock_benchmark.tables = {"test_table": str(temp_path)}

            adapter = DatabricksAdapter(
                server_hostname="test.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/test",
                access_token="test_token",
                catalog="test_catalog",
                schema="test_schema",
                staging_root="dbfs:/Volumes/workspace/tmp",
            )

            table_stats, load_time, _ = adapter.load_data(mock_benchmark, mock_connection, Path("/tmp"))

            assert isinstance(table_stats, dict)
            assert isinstance(load_time, float)
            assert load_time >= 0
            assert "TEST_TABLE" in table_stats
            assert table_stats["TEST_TABLE"] == 100

            # Should execute COPY INTO statements without temporary views or insert-select
            execute_calls = [str(call) for call in mock_cursor.execute.call_args_list]
            assert any("COPY INTO TEST_TABLE" in call for call in execute_calls)
            assert all("CREATE OR REPLACE TEMPORARY VIEW" not in call for call in execute_calls)
            assert all("INSERT INTO" not in call for call in execute_calls)

        finally:
            temp_path.unlink()

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_configure_for_benchmark_olap(self, mock_databricks_sql):
        """Test OLAP benchmark configuration without default Spark optimizations."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
        )

        adapter.configure_for_benchmark(mock_connection, "olap")

        # Should execute cache control setting by default
        execute_calls = [str(call) for call in mock_cursor.execute.call_args_list]
        assert len(execute_calls) == 1, "Should set cache control"
        assert "use_cached_result = false" in execute_calls[0]

        # Should create cursor to apply cache control
        mock_connection.cursor.assert_called_once()

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_configure_for_benchmark_with_delta_optimization(self, mock_databricks_sql):
        """Test benchmark configuration with user-provided Spark configs."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Create adapter with custom Spark configs
        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
            enable_delta_optimization=True,
            delta_auto_optimize=True,
            delta_auto_compact=True,
        )

        # Manually provide spark_configs to test user-provided config path
        adapter.spark_configs = {
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true",
        }

        adapter.configure_for_benchmark(mock_connection, "tpch")

        # Should execute user-provided Spark configs
        execute_calls = [str(call) for call in mock_cursor.execute.call_args_list]
        assert any("spark.databricks.delta.optimizeWrite.enabled" in call for call in execute_calls)
        assert any("spark.databricks.delta.autoCompact.enabled" in call for call in execute_calls)

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_execute_query_success(self, mock_databricks_sql):
        """Test successful query execution."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1, "test"), (2, "test2")]

        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
        )

        result = adapter.execute_query(mock_connection, "SELECT * FROM test", "q1")

        assert result["query_id"] == "q1"
        assert result["status"] == "SUCCESS"
        assert result["rows_returned"] == 2
        assert result["first_row"] == (1, "test")
        assert isinstance(result["execution_time"], float)
        # Note: query_statistics not returned by actual implementation

        mock_cursor.execute.assert_called_with("SELECT * FROM test")
        mock_cursor.close.assert_called_once()

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_execute_query_failure(self, mock_databricks_sql):
        """Test query execution failure."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Query failed")

        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
        )

        result = adapter.execute_query(mock_connection, "INVALID SQL", "q1")

        assert result["query_id"] == "q1"
        assert result["status"] == "FAILED"
        assert result["rows_returned"] == 0
        assert result["error"] == "Query failed"
        assert result["error_type"] == "Exception"
        assert isinstance(result["execution_time"], float)

        mock_cursor.close.assert_called_once()

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_get_query_statistics(self, mock_databricks_sql):
        """Test query statistics retrieval (method not implemented)."""
        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
        )

        # Method doesn't exist in actual implementation
        assert not hasattr(adapter, "_get_query_statistics")

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_get_platform_metadata(self, mock_databricks_sql):
        """Test platform metadata collection."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock query responses
        mock_cursor.fetchone.side_effect = [
            ["Spark 3.4.1"],  # Spark version
            ["test_catalog", "test_schema"],  # Current catalog and schema
        ]

        mock_cursor.fetchall.side_effect = [
            [["TABLE1", 1000, 1024000, "DELTA", "2024-01-01"]],  # Table info
            [["memory: 8GB", "cores: 4", "cluster_type: sql_warehouse"]],  # Cluster info
        ]

        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
            catalog="test_catalog",
            schema="test_schema",
        )

        metadata = adapter._get_platform_metadata(mock_connection)

        assert metadata["platform"] == "Databricks"
        assert metadata["server_hostname"] == "test.cloud.databricks.com"
        assert metadata["catalog"] == "test_catalog"
        assert metadata["schema"] == "test_schema"
        assert "spark_version" in metadata
        assert "current_catalog" in metadata
        assert "current_schema" in metadata
        assert "available_functions" in metadata

        mock_cursor.close.assert_called()

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_optimize_table_delta(self, mock_databricks_sql):
        """Test Delta table optimization."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
        )

        adapter.optimize_table(mock_connection, "test_table")

        # Should execute OPTIMIZE command for Delta tables
        execute_calls = [str(call) for call in mock_cursor.execute.call_args_list]
        assert any("OPTIMIZE TEST_TABLE" in call for call in execute_calls)
        # Note: VACUUM is handled by separate vacuum_table method

        mock_cursor.close.assert_called_once()

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_close_connection(self, mock_databricks_sql):
        """Test connection closing."""
        mock_connection = Mock()

        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
        )

        adapter.close_connection(mock_connection)

        mock_connection.close.assert_called_once()

    def test_supports_tuning_type(self):
        """Test tuning type support checking."""
        with patch("benchbox.platforms.databricks.adapter.databricks_sql"):
            adapter = DatabricksAdapter(
                server_hostname="test.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/test",
                access_token="test_token",
            )

            # Mock TuningType
            with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
                mock_tuning_type.PARTITIONING = "partitioning"
                mock_tuning_type.CLUSTERING = "clustering"
                mock_tuning_type.DISTRIBUTION = "distribution"
                mock_tuning_type.SORTING = "sorting"

                assert adapter.supports_tuning_type(mock_tuning_type.PARTITIONING) is True
                assert adapter.supports_tuning_type(mock_tuning_type.CLUSTERING) is True
                assert adapter.supports_tuning_type(mock_tuning_type.DISTRIBUTION) is True
                assert adapter.supports_tuning_type(mock_tuning_type.SORTING) is False

    def test_generate_tuning_clause_with_partitioning(self):
        """Test tuning clause generation with partitioning."""
        with patch("benchbox.platforms.databricks.adapter.databricks_sql"):
            adapter = DatabricksAdapter(
                server_hostname="test.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/test",
                access_token="test_token",
            )

            # Mock table tuning with partitioning
            mock_tuning = Mock()
            mock_tuning.has_any_tuning.return_value = True

            # Mock partitioning column
            mock_column = Mock()
            mock_column.name = "partition_key"
            mock_column.order = 1

            with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
                mock_tuning_type.PARTITIONING = "partitioning"
                mock_tuning_type.CLUSTERING = "clustering"
                mock_tuning_type.Z_ORDERING = "z_ordering"

                def mock_get_columns_by_type(tuning_type):
                    if tuning_type == mock_tuning_type.PARTITIONING:
                        return [mock_column]
                    return []

                mock_tuning.get_columns_by_type.side_effect = mock_get_columns_by_type

                clause = adapter.generate_tuning_clause(mock_tuning)

                assert "PARTITIONED BY (partition_key)" in clause

    def test_generate_tuning_clause_with_clustering(self):
        """Test tuning clause generation with clustering."""
        with patch("benchbox.platforms.databricks.adapter.databricks_sql"):
            adapter = DatabricksAdapter(
                server_hostname="test.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/test",
                access_token="test_token",
            )

            # Mock table tuning with clustering
            mock_tuning = Mock()
            mock_tuning.has_any_tuning.return_value = True

            # Mock clustering column
            mock_column = Mock()
            mock_column.name = "cluster_key"
            mock_column.order = 1

            with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
                mock_tuning_type.PARTITIONING = "partitioning"
                mock_tuning_type.CLUSTERING = "clustering"
                mock_tuning_type.DISTRIBUTION = "distribution"

                def mock_get_columns_by_type(tuning_type):
                    if tuning_type == mock_tuning_type.CLUSTERING:
                        return [mock_column]
                    return []

                mock_tuning.get_columns_by_type.side_effect = mock_get_columns_by_type

                clause = adapter.generate_tuning_clause(mock_tuning)

                assert "CLUSTER BY (cluster_key)" in clause

    def test_generate_tuning_clause_none(self):
        """Test tuning clause generation with None input."""
        with patch("benchbox.platforms.databricks.adapter.databricks_sql"):
            adapter = DatabricksAdapter(
                server_hostname="test.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/test",
                access_token="test_token",
            )

            clause = adapter.generate_tuning_clause(None)
            assert clause == ""

    @patch("benchbox.platforms.databricks.adapter.databricks_sql")
    def test_apply_table_tunings_with_clustering(self, mock_databricks_sql):
        """Test applying table tunings with clustering."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock DESCRIBE EXTENDED response
        mock_cursor.fetchall.return_value = [
            ["col_name", "data_type", "comment"],
            ["Provider", "DELTA", None],
            ["Type", "MANAGED", None],
        ]

        adapter = DatabricksAdapter(
            server_hostname="test.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="test_token",
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
            mock_tuning_type.PARTITIONING = "partitioning"
            mock_tuning_type.CLUSTERING = "clustering"
            mock_tuning_type.DISTRIBUTION = "distribution"
            mock_tuning_type.SORTING = "sorting"

            def mock_get_columns_by_type(tuning_type):
                if tuning_type == mock_tuning_type.CLUSTERING:
                    return [mock_column]
                return []

            mock_tuning.get_columns_by_type.side_effect = mock_get_columns_by_type

            adapter.apply_table_tunings(mock_tuning, mock_connection)

            # Should execute clustering optimization via Z-ORDER
            execute_calls = [str(call) for call in mock_cursor.execute.call_args_list]
            assert any("OPTIMIZE TEST_TABLE ZORDER BY" in call for call in execute_calls)

        mock_cursor.close.assert_called()

    def test_apply_unified_tuning(self):
        """Test unified tuning configuration application."""
        with patch("benchbox.platforms.databricks.adapter.databricks_sql"):
            adapter = DatabricksAdapter(
                server_hostname="test.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/test",
                access_token="test_token",
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

    def test_apply_platform_optimizations_with_delta_features(self):
        """Test platform optimizations with Delta Lake features."""
        with patch("benchbox.platforms.databricks.adapter.databricks_sql"):
            adapter = DatabricksAdapter(
                server_hostname="test.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/test",
                access_token="test_token",
                enable_delta_optimization=True,
                auto_optimize=True,
            )

            mock_connection = Mock()
            mock_platform_config = Mock()

            # Should not raise exception - Delta optimizations handled in configure_for_benchmark
            adapter.apply_platform_optimizations(mock_platform_config, mock_connection)

    def test_apply_constraint_configuration(self):
        """Test constraint configuration application."""
        with patch("benchbox.platforms.databricks.adapter.databricks_sql"):
            adapter = DatabricksAdapter(
                server_hostname="test.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/test",
                access_token="test_token",
            )

            mock_connection = Mock()
            mock_primary_key_config = Mock()
            mock_primary_key_config.enabled = True
            mock_foreign_key_config = Mock()
            mock_foreign_key_config.enabled = False

            # Should not raise exception - constraints are informational in Databricks
            adapter.apply_constraint_configuration(mock_primary_key_config, mock_foreign_key_config, mock_connection)

    def test_authentication_methods(self):
        """Test different authentication methods."""
        with patch("benchbox.platforms.databricks.adapter.databricks_sql"):
            # Test access token authentication
            adapter1 = DatabricksAdapter(
                server_hostname="test.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/test",
                access_token="test_token",
            )
            assert adapter1.access_token == "test_token"
            assert adapter1.server_hostname == "test.cloud.databricks.com"
            assert adapter1.http_path == "/sql/1.0/warehouses/test"

    def test_unity_catalog_configuration(self):
        """Test Unity Catalog configuration options."""
        with patch("benchbox.platforms.databricks.adapter.databricks_sql"):
            adapter = DatabricksAdapter(
                server_hostname="test.cloud.databricks.com",
                http_path="/sql/1.0/warehouses/test",
                access_token="test_token",
                catalog="my_catalog",
                schema="my_schema",
            )

            assert adapter.catalog == "my_catalog"
            assert adapter.schema == "my_schema"
            assert adapter.enable_delta_optimization is True
            assert adapter.delta_auto_optimize is True
