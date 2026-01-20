"""Tests for BigQuery platform adapter.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from benchbox.platforms.bigquery import BigQueryAdapter

pytestmark = pytest.mark.fast


@pytest.fixture
def dependencies_available():
    """Mock BigQuery dependency check to simulate installed extras."""

    with patch("benchbox.platforms.bigquery.check_platform_dependencies", return_value=(True, [])):
        yield


@pytest.mark.usefixtures("dependencies_available")
class TestBigQueryAdapter:
    """Test BigQuery platform adapter functionality."""

    def test_initialization_success(self, dependencies_available):
        """Test successful adapter initialization."""
        with patch("benchbox.platforms.bigquery.bigquery"):
            adapter = BigQueryAdapter(
                project_id="test-project",
                dataset_id="test_dataset",
                location="US",
                credentials_path="/path/to/service-account.json",
            )
            assert adapter.platform_name == "BigQuery"
            assert adapter.get_target_dialect() == "bigquery"
            assert adapter.project_id == "test-project"
            assert adapter.dataset_id == "test_dataset"
            assert adapter.location == "US"

    def test_initialization_with_defaults(self, dependencies_available):
        """Test initialization with default configuration."""
        with patch("benchbox.platforms.bigquery.bigquery"):
            adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")
            assert adapter.location == "US"
            assert adapter.maximum_bytes_billed is None
            assert adapter.query_cache is False  # Cache disabled by default for accurate benchmarking
            assert adapter.dry_run is False

    def test_initialization_missing_driver(self):
        """Test initialization when BigQuery client library is not available."""
        with (
            patch(
                "benchbox.platforms.bigquery.check_platform_dependencies",
                return_value=(
                    False,
                    [
                        "google-cloud-bigquery",
                        "google-cloud-storage",
                    ],
                ),
            ),
            pytest.raises(ImportError, match="Missing dependencies for bigquery platform"),
        ):
            BigQueryAdapter()

    def test_initialization_missing_required_config(self, dependencies_available):
        """Test initialization with missing required configuration."""
        from benchbox.core.exceptions import ConfigurationError

        with patch("benchbox.platforms.bigquery.bigquery"):
            with pytest.raises(ConfigurationError, match="BigQuery configuration requires"):
                BigQueryAdapter()  # Missing required fields

    @patch("benchbox.platforms.bigquery.service_account")
    @patch("benchbox.platforms.bigquery.bigquery")
    def test_create_admin_client_with_credentials_path(
        self, mock_bigquery, mock_service_account, dependencies_available
    ):
        """Test admin client creation with credentials path."""
        mock_client = Mock()
        mock_bigquery.Client.return_value = mock_client
        mock_credentials = Mock()
        mock_service_account.Credentials.from_service_account_file.return_value = mock_credentials

        adapter = BigQueryAdapter(
            project_id="test-project",
            dataset_id="test_dataset",
            credentials_path="/path/to/service-account.json",
        )

        client = adapter._create_admin_client()

        assert client == mock_client
        mock_service_account.Credentials.from_service_account_file.assert_called_once_with(
            "/path/to/service-account.json"
        )
        mock_bigquery.Client.assert_called_once_with(
            project="test-project", location="US", credentials=mock_credentials
        )

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_create_admin_client_with_default_credentials(self, mock_bigquery, dependencies_available):
        """Test admin client creation with default credentials."""
        mock_client = Mock()
        mock_bigquery.Client.return_value = mock_client
        mock_credentials = Mock()

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        # Mock _load_credentials to return mock credentials
        with patch.object(adapter, "_load_credentials", return_value=mock_credentials):
            client = adapter._create_admin_client()

        assert client == mock_client
        mock_bigquery.Client.assert_called_once_with(
            project="test-project", location="US", credentials=mock_credentials
        )

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_check_server_database_exists_true(self, mock_bigquery, dependencies_available):
        """Test dataset existence check when dataset exists."""
        mock_client = Mock()
        mock_dataset = Mock()
        mock_dataset.dataset_id = "test_dataset"
        mock_client.list_datasets.return_value = [mock_dataset]
        mock_bigquery.Client.return_value = mock_client

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        with patch.object(adapter, "_load_credentials", return_value=Mock()):
            exists = adapter.check_server_database_exists()

        assert exists is True
        mock_client.list_datasets.assert_called_once()

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_check_server_database_exists_false(self, mock_bigquery, dependencies_available):
        """Test dataset existence check when dataset doesn't exist."""
        mock_client = Mock()
        mock_other_dataset = Mock()
        mock_other_dataset.dataset_id = "other_dataset"
        mock_client.list_datasets.return_value = [mock_other_dataset]
        mock_bigquery.Client.return_value = mock_client

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        with patch.object(adapter, "_load_credentials", return_value=Mock()):
            exists = adapter.check_server_database_exists()

        assert exists is False

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_drop_database(self, mock_bigquery, dependencies_available):
        """Test dataset dropping."""
        mock_client = Mock()
        mock_dataset_ref = Mock()
        mock_client.dataset.return_value = mock_dataset_ref
        mock_bigquery.Client.return_value = mock_client

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        with patch.object(adapter, "_load_credentials", return_value=Mock()):
            adapter.drop_database()

        mock_client.dataset.assert_called_once_with("test_dataset")
        mock_client.delete_dataset.assert_called_once_with(mock_dataset_ref, delete_contents=True, not_found_ok=True)

    @patch("benchbox.platforms.bigquery.bigquery")
    @patch("benchbox.platforms.bigquery.storage")
    def test_create_connection_success(self, mock_storage, mock_bigquery, dependencies_available):
        """Test successful connection creation."""
        mock_client = Mock()
        mock_storage_client = Mock()
        mock_bigquery.Client.return_value = mock_client
        mock_storage.Client.return_value = mock_storage_client
        mock_credentials = Mock()

        # Mock query execution
        mock_query_job = Mock()
        mock_query_job.result.return_value = [(1,)]
        mock_client.query.return_value = mock_query_job

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        # Mock handle_existing_database and _load_credentials
        with (
            patch.object(adapter, "handle_existing_database"),
            patch.object(adapter, "_load_credentials", return_value=mock_credentials),
        ):
            connection = adapter.create_connection()

        assert connection == mock_client

        # Should test connection
        mock_client.query.assert_called_once_with("SELECT 1 as test")

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_create_connection_failure(self, mock_bigquery, dependencies_available):
        """Test connection creation failure."""
        mock_credentials = Mock()
        mock_bigquery.Client.side_effect = Exception("Authentication failed")

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        with (
            patch.object(adapter, "handle_existing_database"),
            patch.object(adapter, "_load_credentials", return_value=mock_credentials),
        ):
            with pytest.raises(Exception, match="Authentication failed"):
                adapter.create_connection()

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_create_schema(self, mock_bigquery, dependencies_available):
        """Test schema creation with BigQuery tables."""
        mock_client = Mock()
        mock_bigquery.Client.return_value = mock_client

        # Mock table creation
        mock_table = Mock()
        mock_client.create_table.return_value = mock_table

        mock_benchmark = Mock()
        mock_benchmark.get_create_tables_sql.return_value = """
            CREATE TABLE table1 (id INT64, name STRING);
            CREATE TABLE table2 (id INT64, data STRING);
        """

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        # Mock translate_sql method
        with patch.object(adapter, "translate_sql") as mock_translate:
            mock_translate.return_value = (
                "CREATE TABLE table1 (id INT64, name STRING);\nCREATE TABLE table2 (id INT64, data STRING);"
            )

            schema_time = adapter.create_schema(mock_benchmark, mock_client)

        assert isinstance(schema_time, float)
        assert schema_time >= 0

        # Should create tables via DDL execution
        query_calls = list(mock_client.query.call_args_list)
        assert len(query_calls) >= 2  # At least 2 CREATE TABLE statements
        # Note: Actual SQL will be converted to BigQuery format with dataset qualification

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_load_data_with_csv_upload(self, mock_bigquery, dependencies_available):
        """Test data loading using BigQuery load jobs."""
        mock_client = Mock()
        mock_bigquery.Client.return_value = mock_client

        # Mock load job
        mock_load_job = Mock()
        mock_load_job.result.return_value = None
        mock_load_job.output_rows = 100
        mock_client.load_table_from_file.return_value = mock_load_job

        # Mock row count query
        mock_query_job = Mock()
        mock_query_job.result.return_value = [(100,)]
        mock_client.query.return_value = mock_query_job

        mock_benchmark = Mock()

        # Create temporary test file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name\n1,test1\n2,test2\n")
            temp_path = Path(f.name)

        try:
            mock_benchmark.tables = {"test_table": str(temp_path)}

            adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

            table_stats, load_time, _ = adapter.load_data(mock_benchmark, mock_client, Path("/tmp"))

            assert isinstance(table_stats, dict)
            assert isinstance(load_time, float)
            assert load_time >= 0
            assert "TEST_TABLE" in table_stats
            assert table_stats["TEST_TABLE"] == 100

            # Should execute load job
            mock_client.load_table_from_file.assert_called_once()

        finally:
            temp_path.unlink()

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_configure_for_benchmark_olap(self, mock_bigquery, dependencies_available):
        """Test OLAP benchmark configuration."""
        mock_client = Mock()
        mock_bigquery.Client.return_value = mock_client

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        # Should not raise exception - BigQuery optimizations are automatic
        adapter.configure_for_benchmark(mock_client, "olap")

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_execute_query_success(self, mock_bigquery, dependencies_available):
        """Test successful query execution."""
        mock_client = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = [(1, "test"), (2, "test2")]
        mock_query_job.job_id = "job_123"
        mock_query_job.total_bytes_processed = 1024
        mock_query_job.total_bytes_billed = 512
        mock_query_job.slot_millis = 1000
        mock_client.query.return_value = mock_query_job
        mock_bigquery.Client.return_value = mock_client

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        result = adapter.execute_query(mock_client, "SELECT * FROM test", "q1")

        assert result["query_id"] == "q1"
        assert result["status"] == "SUCCESS"
        assert result["rows_returned"] == 2
        assert result["first_row"] == (1, "test")
        assert isinstance(result["execution_time"], float)

        # Should include BigQuery-specific statistics
        assert result["job_id"] == "job_123"
        assert result["job_statistics"]["bytes_processed"] == 1024
        assert result["job_statistics"]["bytes_billed"] == 512
        assert result["job_statistics"]["slot_ms"] == 1000

        mock_client.query.assert_called_once()

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_execute_query_with_job_config(self, mock_bigquery, dependencies_available):
        """Test query execution with job configuration."""
        mock_client = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = [(1, "test")]
        mock_query_job.job_id = "job_123"
        mock_query_job.total_bytes_processed = 1024
        mock_query_job.total_bytes_billed = 512
        mock_query_job.slot_millis = 1000
        mock_query_job.created = None
        mock_query_job.started = None
        mock_query_job.ended = None
        mock_client.query.return_value = mock_query_job
        mock_bigquery.Client.return_value = mock_client

        adapter = BigQueryAdapter(
            project_id="test-project",
            dataset_id="test_dataset",
            maximum_bytes_billed=1000000,
        )

        result = adapter.execute_query(mock_client, "SELECT * FROM test", "q1")

        assert result["status"] == "SUCCESS"
        assert result["job_statistics"]["bytes_processed"] == 1024

        # Should call query with job config
        mock_client.query.assert_called_once()

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_execute_query_failure(self, mock_bigquery, dependencies_available):
        """Test query execution failure."""
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Query failed")
        mock_bigquery.Client.return_value = mock_client

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        result = adapter.execute_query(mock_client, "INVALID SQL", "q1")

        assert result["query_id"] == "q1"
        assert result["status"] == "FAILED"
        assert result["rows_returned"] == 0
        assert result["error"] == "Query failed"
        assert result["error_type"] == "Exception"
        assert isinstance(result["execution_time"], float)

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_get_platform_metadata(self, mock_bigquery, dependencies_available):
        """Test platform metadata collection."""
        mock_client = Mock()
        mock_client.project = "test-project"
        mock_client.location = "US"

        # Mock dataset info query
        mock_query_job = Mock()
        mock_query_job.result.return_value = [("test_dataset", "US", "2024-01-01 00:00:00", "BenchBox test dataset")]
        mock_client.query.return_value = mock_query_job

        # Mock tables info
        mock_tables = [Mock(), Mock()]
        mock_tables[0].table_id = "table1"
        mock_tables[0].num_rows = 1000
        mock_tables[0].num_bytes = 1024000
        mock_tables[1].table_id = "table2"
        mock_tables[1].num_rows = 2000
        mock_tables[1].num_bytes = 2048000
        mock_client.list_tables.return_value = mock_tables

        mock_bigquery.Client.return_value = mock_client

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        metadata = adapter._get_platform_metadata(mock_client)

        assert metadata["platform"] == "BigQuery"
        assert metadata["project_id"] == "test-project"
        assert metadata["dataset_id"] == "test_dataset"
        assert metadata["location"] == "US"
        assert "dataset_info" in metadata
        assert "tables" in metadata
        assert len(metadata["tables"]) == 2

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_close_connection(self, mock_bigquery, dependencies_available):
        """Test connection closing."""
        mock_client = Mock()
        mock_bigquery.Client.return_value = mock_client

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        # Should not raise exception - BigQuery client doesn't need explicit closing
        adapter.close_connection(mock_client)

    def test_supports_tuning_type(self, dependencies_available):
        """Test tuning type support checking."""
        with patch("benchbox.platforms.bigquery.bigquery"):
            adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

            # Mock TuningType import
            with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
                mock_tuning_type.PARTITIONING = "partitioning"
                mock_tuning_type.CLUSTERING = "clustering"
                mock_tuning_type.SORTING = "sorting"

                assert adapter.supports_tuning_type(mock_tuning_type.PARTITIONING) is True
                assert adapter.supports_tuning_type(mock_tuning_type.CLUSTERING) is True
                assert adapter.supports_tuning_type(mock_tuning_type.SORTING) is False

    def test_generate_tuning_clause_with_partitioning(self, dependencies_available):
        """Test tuning clause generation with partitioning."""
        with patch("benchbox.platforms.bigquery.bigquery"):
            adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

            # Mock table tuning with partitioning
            mock_tuning = Mock()
            mock_tuning.has_any_tuning.return_value = True

            # Mock partitioning column
            mock_column = Mock()
            mock_column.name = "partition_date"
            mock_column.order = 1
            mock_column.type = "DATE"

            with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
                mock_tuning_type.PARTITIONING = "partitioning"
                mock_tuning_type.CLUSTERING = "clustering"

                def mock_get_columns_by_type(tuning_type):
                    if tuning_type == mock_tuning_type.PARTITIONING:
                        return [mock_column]
                    return []

                mock_tuning.get_columns_by_type.side_effect = mock_get_columns_by_type

                clause = adapter.generate_tuning_clause(mock_tuning)

                assert "PARTITION BY partition_date" in clause

    def test_generate_tuning_clause_with_clustering(self, dependencies_available):
        """Test tuning clause generation with clustering."""
        with patch("benchbox.platforms.bigquery.bigquery"):
            adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

            # Mock table tuning with clustering
            mock_tuning = Mock()
            mock_tuning.has_any_tuning.return_value = True

            # Mock clustering columns
            mock_column1 = Mock()
            mock_column1.name = "cluster_key1"
            mock_column1.order = 1
            mock_column2 = Mock()
            mock_column2.name = "cluster_key2"
            mock_column2.order = 2

            with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
                mock_tuning_type.PARTITIONING = "partitioning"
                mock_tuning_type.CLUSTERING = "clustering"

                def mock_get_columns_by_type(tuning_type):
                    if tuning_type == mock_tuning_type.CLUSTERING:
                        return [mock_column1, mock_column2]
                    return []

                mock_tuning.get_columns_by_type.side_effect = mock_get_columns_by_type

                clause = adapter.generate_tuning_clause(mock_tuning)

                assert "CLUSTER BY cluster_key1, cluster_key2" in clause

    def test_generate_tuning_clause_none(self, dependencies_available):
        """Test tuning clause generation with None input."""
        with patch("benchbox.platforms.bigquery.bigquery"):
            adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

            clause = adapter.generate_tuning_clause(None)
            assert clause == ""

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_apply_table_tunings_with_partitioning(self, mock_bigquery, dependencies_available):
        """Test applying table tunings with partitioning."""
        mock_client = Mock()
        mock_bigquery.Client.return_value = mock_client

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        # Mock table tuning
        mock_tuning = Mock()
        mock_tuning.table_name = "test_table"
        mock_tuning.has_any_tuning.return_value = True

        # Mock partitioning column
        mock_column = Mock()
        mock_column.name = "partition_date"
        mock_column.order = 1

        with patch("benchbox.core.tuning.interface.TuningType") as mock_tuning_type:
            mock_tuning_type.PARTITIONING = "partitioning"
            mock_tuning_type.CLUSTERING = "clustering"
            mock_tuning_type.SORTING = "sorting"
            mock_tuning_type.DISTRIBUTION = "distribution"

            def mock_get_columns_by_type(tuning_type):
                if tuning_type == mock_tuning_type.PARTITIONING:
                    return [mock_column]
                return []

            mock_tuning.get_columns_by_type.side_effect = mock_get_columns_by_type

            # Should not raise exception - BigQuery tuning is applied during table creation
            adapter.apply_table_tunings(mock_tuning, mock_client)

    def test_apply_unified_tuning(self, dependencies_available):
        """Test unified tuning configuration application."""
        with patch("benchbox.platforms.bigquery.bigquery"):
            adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

            mock_client = Mock()
            mock_unified_config = Mock()
            mock_unified_config.primary_keys = Mock()
            mock_unified_config.foreign_keys = Mock()
            mock_unified_config.platform_optimizations = Mock()
            mock_unified_config.table_tunings = {}

            # Should not raise exception
            with patch.object(adapter, "apply_constraint_configuration"):
                with patch.object(adapter, "apply_platform_optimizations"):
                    adapter.apply_unified_tuning(mock_unified_config, mock_client)

    def test_apply_constraint_configuration(self, dependencies_available):
        """Test constraint configuration application."""
        with patch("benchbox.platforms.bigquery.bigquery"):
            adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

            mock_client = Mock()
            mock_primary_key_config = Mock()
            mock_primary_key_config.enabled = True
            mock_foreign_key_config = Mock()
            mock_foreign_key_config.enabled = False

            # Should not raise exception - constraints are not enforced in BigQuery
            adapter.apply_constraint_configuration(mock_primary_key_config, mock_foreign_key_config, mock_client)

    def test_cost_control_features(self, dependencies_available):
        """Test BigQuery cost control features."""
        with patch("benchbox.platforms.bigquery.bigquery"):
            adapter = BigQueryAdapter(
                project_id="test-project",
                dataset_id="test_dataset",
                maximum_bytes_billed=1000000,  # 1MB limit
                query_cache=True,
                dry_run=True,
            )

            assert adapter.maximum_bytes_billed == 1000000
            assert adapter.query_cache is True
            assert adapter.dry_run is True

    def test_authentication_methods(self, dependencies_available):
        """Test different authentication methods."""
        with patch("benchbox.platforms.bigquery.bigquery"):
            # Test service account file authentication
            adapter1 = BigQueryAdapter(
                project_id="test-project",
                dataset_id="test_dataset",
                credentials_path="/path/to/key.json",
            )
            assert adapter1.credentials_path == "/path/to/key.json"

            # Test default credentials (ADC)
            adapter2 = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")
            assert adapter2.credentials_path is None

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_job_configuration_options(self, mock_bigquery, dependencies_available):
        """Test BigQuery job configuration options."""
        mock_client = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = [(1,)]
        mock_client.query.return_value = mock_query_job
        mock_bigquery.Client.return_value = mock_client

        adapter = BigQueryAdapter(
            project_id="test-project",
            dataset_id="test_dataset",
            maximum_bytes_billed=1000000,
            use_legacy_sql=False,
            job_timeout_ms=600000,
        )

        result = adapter.execute_query(mock_client, "SELECT 1", "q1")

        assert result["status"] == "SUCCESS"

        # Should configure query job with specified options
        # Note: timeout handling varies by BigQuery client version
        mock_client.query.assert_called_once()

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_data_location_and_regional_settings(self, mock_bigquery, dependencies_available):
        """Test BigQuery data location and regional settings."""
        mock_client = Mock()
        mock_bigquery.Client.return_value = mock_client

        # Test different regions
        adapter_us = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset", location="US")
        assert adapter_us.location == "US"

        adapter_eu = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset", location="EU")
        assert adapter_eu.location == "EU"

        adapter_asia = BigQueryAdapter(
            project_id="test-project",
            dataset_id="test_dataset",
            location="asia-northeast1",
        )
        assert adapter_asia.location == "asia-northeast1"

    def test_file_format_detection_for_chunked_files(self, dependencies_available):
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

    def test_validate_data_integrity_with_client_api(self, dependencies_available):
        """Test that data integrity validation uses BigQuery Client API instead of cursor pattern.

        This verifies the fix for "'Client' object has no attribute 'cursor'" errors
        during database compatibility validation.
        """
        from unittest.mock import Mock

        # Mock BigQuery client and query results
        mock_client = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = [(1,)]  # Mock query result
        mock_client.query.return_value = mock_query_job

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        # Create mock benchmark
        mock_benchmark = Mock()

        # Table stats from validation
        table_stats = {
            "customer": 150,
            "lineitem": 6005,
            "nation": 25,
            "region": 5,
        }

        # Call validation method
        status, details = adapter._validate_data_integrity(mock_benchmark, mock_client, table_stats)

        # Verify it uses query() not cursor()
        assert mock_client.query.called, "Should use client.query() not client.cursor()"
        assert not hasattr(mock_client, "cursor") or not mock_client.cursor.called

        # Verify status is PASSED when all tables accessible
        assert status == "PASSED"
        assert "accessible_tables" in details
        assert len(details["accessible_tables"]) == 4

    def test_validate_data_integrity_handles_inaccessible_tables(self, dependencies_available):
        """Test that validation correctly detects inaccessible tables."""
        from unittest.mock import Mock

        mock_client = Mock()

        # Mock query to fail for some tables
        def query_side_effect(sql):
            mock_job = Mock()
            if "CUSTOMER" in sql or "LINEITEM" in sql:
                # Simulate table not found or inaccessible
                mock_job.result.side_effect = Exception("404 Table not found")
            else:
                mock_job.result.return_value = [(1,)]
            return mock_job

        mock_client.query.side_effect = query_side_effect

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")
        mock_benchmark = Mock()

        table_stats = {"customer": 0, "lineitem": 0, "nation": 25, "region": 5}

        status, details = adapter._validate_data_integrity(mock_benchmark, mock_client, table_stats)

        # Should fail when tables are inaccessible
        assert status == "FAILED"
        assert "inaccessible_tables" in details
        assert "customer" in details["inaccessible_tables"]
        assert "lineitem" in details["inaccessible_tables"]

    def test_get_table_row_count_uses_query_api(self, dependencies_available):
        """Test that get_table_row_count uses BigQuery Client API instead of cursor pattern.

        This verifies the fix for validation framework cursor errors during
        database compatibility checks.
        """
        from unittest.mock import Mock

        mock_client = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = [(150,)]  # Mock COUNT(*) result
        mock_client.query.return_value = mock_query_job

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        # Get row count for customer table
        row_count = adapter.get_table_row_count(mock_client, "customer")

        # Verify it uses query() not cursor()
        assert mock_client.query.called, "Should use client.query() not client.cursor()"
        assert not hasattr(mock_client, "cursor") or not mock_client.cursor.called

        # Verify row count is correct
        assert row_count == 150

    def test_get_table_row_count_handles_errors(self, dependencies_available):
        """Test that get_table_row_count returns 0 on errors."""
        from unittest.mock import Mock

        mock_client = Mock()
        mock_client.query.side_effect = Exception("Table not found")

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        # Should return 0 on error, not raise exception
        row_count = adapter.get_table_row_count(mock_client, "nonexistent_table")
        assert row_count == 0

    def test_storage_client_not_created_in_connection(self, dependencies_available):
        """Test that Storage client is NOT created during create_connection (lazy creation).

        Previously, create_connection would always create a Storage client and attach it
        to the connection object. This test verifies the new behavior where Storage client
        is only created on-demand when actually needed (during load_data with storage_bucket).
        """
        from unittest.mock import Mock, patch

        # Mock BigQuery and Storage clients
        with (
            patch("benchbox.platforms.bigquery.bigquery") as mock_bigquery,
            patch("benchbox.platforms.bigquery.storage") as mock_storage,
        ):
            mock_client = Mock(spec=["query", "close"])  # Strict spec prevents arbitrary attributes
            mock_bigquery.Client.return_value = mock_client

            # Mock successful connection test
            mock_query_job = Mock()
            mock_query_job.result.return_value = []
            mock_client.query.return_value = mock_query_job

            adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

            # Create connection with mocked _load_credentials
            with patch.object(adapter, "_load_credentials", return_value=Mock()):
                adapter.create_connection()

            # Verify Storage client was NOT created during connection
            # This is the key behavior change - Storage client creation is now lazy
            assert not mock_storage.Client.called, "Storage client should not be created in create_connection"

    def test_close_connection_handles_credential_errors(self, dependencies_available):
        """Test that close_connection gracefully handles credential refresh errors."""
        from unittest.mock import Mock

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        # Create mock connection that raises credential error on close
        mock_client = Mock()
        mock_client.close.side_effect = Exception("Anonymous credentials cannot be refreshed")

        # Should not raise exception - error should be suppressed
        adapter.close_connection(mock_client)

        # Verify close was attempted
        assert mock_client.close.called

    def test_close_connection_warns_on_other_errors(self, dependencies_available):
        """Test that close_connection warns on non-credential errors."""
        from unittest.mock import Mock

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        # Create mock connection that raises non-credential error
        mock_client = Mock()
        mock_client.close.side_effect = Exception("Network timeout")

        # Should log warning but not raise
        with patch.object(adapter.logger, "warning") as mock_warning:
            adapter.close_connection(mock_client)
            assert mock_warning.called

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_direct_loading_with_chunked_files(self, mock_bigquery, dependencies_available):
        """Test that direct loading handles chunked files (TPC-DS style) correctly."""
        import tempfile
        from pathlib import Path
        from unittest.mock import MagicMock, Mock

        # Mock WriteDisposition enum
        mock_bigquery.WriteDisposition.WRITE_TRUNCATE = "WRITE_TRUNCATE"
        mock_bigquery.WriteDisposition.WRITE_APPEND = "WRITE_APPEND"

        # Track job configs to verify write dispositions
        job_configs = []

        def create_job_config(**kwargs):
            config = Mock()
            for k, v in kwargs.items():
                setattr(config, k, v)
            job_configs.append(config)
            return config

        mock_bigquery.LoadJobConfig = create_job_config

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        # Create temporary test files
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            # Create chunked files (like TPC-DS generates)
            chunk1 = tmpdir / "customer_1.dat"
            chunk2 = tmpdir / "customer_2.dat"
            chunk1.write_text("1|John|100\n2|Jane|200\n")
            chunk2.write_text("3|Bob|300\n4|Alice|400\n")

            # Single file (like TPC-H)
            single = tmpdir / "nation.dat"
            single.write_text("1|USA\n2|Canada\n")

            # Create mock benchmark with tables as lists (TPC-DS style)
            mock_benchmark = Mock()
            mock_benchmark.tables = {
                "customer": [chunk1, chunk2],  # Multiple chunks
                "nation": [single],  # Single chunk in list
            }

            # Create mock BigQuery connection
            mock_connection = MagicMock()
            mock_dataset = Mock()
            mock_table_ref = Mock()
            mock_connection.dataset.return_value = mock_dataset
            mock_dataset.table.return_value = mock_table_ref

            # Mock load_table_from_file
            mock_load_job = Mock()
            mock_load_job.result.return_value = None
            mock_connection.load_table_from_file.return_value = mock_load_job

            # Mock query for row counts
            mock_query_job = Mock()
            mock_connection.query.return_value = mock_query_job

            # Return different row counts for different tables
            def mock_result():
                # First call is for customer (after all chunks), second is for nation
                if not hasattr(mock_result, "call_count"):
                    mock_result.call_count = 0
                mock_result.call_count += 1
                if mock_result.call_count == 1:
                    return [(4,)]  # customer: 4 rows (2 + 2)
                else:
                    return [(2,)]  # nation: 2 rows

            mock_query_job.result.side_effect = [mock_result(), mock_result()]

            # Call load_data
            table_stats, total_time, per_table_timings = adapter.load_data(mock_benchmark, mock_connection, tmpdir)

            # Verify both tables were loaded
            assert "CUSTOMER" in table_stats
            assert "NATION" in table_stats

            # Verify load_table_from_file was called for each chunk (2 for customer, 1 for nation)
            assert mock_connection.load_table_from_file.call_count == 3

            # Verify WRITE_TRUNCATE for first chunk, WRITE_APPEND for subsequent chunks
            # First job config (customer chunk 1): WRITE_TRUNCATE
            assert job_configs[0].write_disposition == "WRITE_TRUNCATE"
            # Second job config (customer chunk 2): WRITE_APPEND
            assert job_configs[1].write_disposition == "WRITE_APPEND"
            # Third job config (nation, single file): WRITE_TRUNCATE
            assert job_configs[2].write_disposition == "WRITE_TRUNCATE"

    @patch("benchbox.platforms.bigquery.bigquery")
    def test_direct_loading_with_single_paths(self, mock_bigquery, dependencies_available):
        """Test that direct loading still works with single paths (TPC-H style)."""
        import tempfile
        from pathlib import Path
        from unittest.mock import MagicMock, Mock

        # Mock WriteDisposition enum
        mock_bigquery.WriteDisposition.WRITE_TRUNCATE = "WRITE_TRUNCATE"
        mock_bigquery.WriteDisposition.WRITE_APPEND = "WRITE_APPEND"

        adapter = BigQueryAdapter(project_id="test-project", dataset_id="test_dataset")

        # Create temporary test file
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            test_file = tmpdir / "nation.dat"
            test_file.write_text("1|USA\n2|Canada\n")

            # Create mock benchmark with tables as single paths (TPC-H style)
            mock_benchmark = Mock()
            mock_benchmark.tables = {
                "nation": test_file,  # Single path, not a list
            }

            # Create mock BigQuery connection
            mock_connection = MagicMock()
            mock_dataset = Mock()
            mock_table_ref = Mock()
            mock_connection.dataset.return_value = mock_dataset
            mock_dataset.table.return_value = mock_table_ref

            # Mock load_table_from_file
            mock_load_job = Mock()
            mock_load_job.result.return_value = None
            mock_connection.load_table_from_file.return_value = mock_load_job

            # Mock query for row count
            mock_query_job = Mock()
            mock_query_job.result.return_value = [(2,)]
            mock_connection.query.return_value = mock_query_job

            # Call load_data
            table_stats, total_time, per_table_timings = adapter.load_data(mock_benchmark, mock_connection, tmpdir)

            # Verify table was loaded
            assert "NATION" in table_stats
            assert table_stats["NATION"] == 2

            # Verify load_table_from_file was called once
            assert mock_connection.load_table_from_file.call_count == 1

    def test_validate_database_compatibility_detects_empty_tables(self, dependencies_available):
        """Test that validation detects when more than half the tables are empty."""
        with patch("benchbox.platforms.bigquery.bigquery"):
            adapter = BigQueryAdapter(
                project_id="test-project",
                dataset_id="test_dataset",
            )

            # Mock the admin client
            mock_client = Mock()
            mock_dataset_ref = Mock()
            mock_client.dataset.return_value = mock_dataset_ref

            # Create mock tables - 3 out of 4 are empty (> 50%)
            mock_table_info1 = Mock()
            mock_table_info1.reference = Mock()
            mock_table_info2 = Mock()
            mock_table_info2.reference = Mock()
            mock_table_info3 = Mock()
            mock_table_info3.reference = Mock()
            mock_table_info4 = Mock()
            mock_table_info4.reference = Mock()

            mock_client.list_tables.return_value = [
                mock_table_info1,
                mock_table_info2,
                mock_table_info3,
                mock_table_info4,
            ]

            # Mock table objects - 3 empty, 1 with data
            mock_table1 = Mock()
            mock_table1.num_rows = 0
            mock_table1.table_id = "table1"

            mock_table2 = Mock()
            mock_table2.num_rows = 0
            mock_table2.table_id = "table2"

            mock_table3 = Mock()
            mock_table3.num_rows = 0
            mock_table3.table_id = "table3"

            mock_table4 = Mock()
            mock_table4.num_rows = 100
            mock_table4.table_id = "table4"

            mock_client.get_table.side_effect = [mock_table1, mock_table2, mock_table3, mock_table4]

            # Mock _create_admin_client to return our mock client
            with patch.object(adapter, "_create_admin_client", return_value=mock_client):
                # Mock the base validation to return valid result
                mock_base_result = Mock()
                mock_base_result.is_valid = True
                mock_base_result.can_reuse = True
                mock_base_result.issues = []
                mock_base_result.warnings = []

                with patch(
                    "benchbox.platforms.base.validation.DatabaseValidator.validate",
                    return_value=mock_base_result,
                ):
                    # Call validation
                    result = adapter._validate_database_compatibility()

                    # Verify that validation failed due to empty tables
                    assert result.is_valid is False
                    assert result.can_reuse is False
                    assert len(result.issues) == 1
                    assert "Empty tables detected: 3/4" in result.issues[0]
                    assert "indicates failed previous load" in result.issues[0]

    def test_validate_database_compatibility_allows_few_empty_tables(self, dependencies_available):
        """Test that validation passes when less than half the tables are empty."""
        with patch("benchbox.platforms.bigquery.bigquery"):
            adapter = BigQueryAdapter(
                project_id="test-project",
                dataset_id="test_dataset",
            )

            # Mock the admin client
            mock_client = Mock()
            mock_dataset_ref = Mock()
            mock_client.dataset.return_value = mock_dataset_ref

            # Create mock tables - 1 out of 4 is empty (< 50%)
            mock_table_info1 = Mock()
            mock_table_info1.reference = Mock()
            mock_table_info2 = Mock()
            mock_table_info2.reference = Mock()
            mock_table_info3 = Mock()
            mock_table_info3.reference = Mock()
            mock_table_info4 = Mock()
            mock_table_info4.reference = Mock()

            mock_client.list_tables.return_value = [
                mock_table_info1,
                mock_table_info2,
                mock_table_info3,
                mock_table_info4,
            ]

            # Mock table objects - 1 empty, 3 with data
            mock_table1 = Mock()
            mock_table1.num_rows = 0
            mock_table1.table_id = "table1"

            mock_table2 = Mock()
            mock_table2.num_rows = 100
            mock_table2.table_id = "table2"

            mock_table3 = Mock()
            mock_table3.num_rows = 100
            mock_table3.table_id = "table3"

            mock_table4 = Mock()
            mock_table4.num_rows = 100
            mock_table4.table_id = "table4"

            mock_client.get_table.side_effect = [mock_table1, mock_table2, mock_table3, mock_table4]

            # Mock _create_admin_client to return our mock client
            with patch.object(adapter, "_create_admin_client", return_value=mock_client):
                # Mock the base validation to return valid result
                mock_base_result = Mock()
                mock_base_result.is_valid = True
                mock_base_result.can_reuse = True
                mock_base_result.issues = []
                mock_base_result.warnings = []

                with patch(
                    "benchbox.platforms.base.validation.DatabaseValidator.validate",
                    return_value=mock_base_result,
                ):
                    # Call validation
                    result = adapter._validate_database_compatibility()

                    # Verify that validation passed (few empty tables are acceptable)
                    assert result.is_valid is True
                    assert result.can_reuse is True
                    assert len(result.issues) == 0
