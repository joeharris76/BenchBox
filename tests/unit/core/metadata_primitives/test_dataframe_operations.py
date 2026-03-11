"""Unit tests for DataFrame metadata operations.

This module tests:
- TestDataFrameMetadataCapabilities: Tests for capability detection
- TestDataFrameMetadataResult: Tests for result dataclass
- TestDataFrameMetadataOperationsManager: Tests for the operations manager
- TestPolarsMetadataOperations: Integration tests with Polars
- TestPandasMetadataOperations: Integration tests with Pandas
- TestUnsupportedOperationErrors: Tests for clear error messages

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import time

import pytest

from benchbox.core.metadata_primitives import (
    DATAFUSION_METADATA_CAPABILITIES,
    OPERATION_CATEGORIES,
    PANDAS_METADATA_CAPABILITIES,
    POLARS_METADATA_CAPABILITIES,
    PYSPARK_METADATA_CAPABILITIES,
    DataFrameMetadataCapabilities,
    DataFrameMetadataOperationsManager,
    DataFrameMetadataResult,
    MetadataOperationCategory,
    MetadataOperationType,
    UnsupportedOperationError,
    get_dataframe_metadata_manager,
    get_platform_capabilities,
    get_unsupported_message,
)

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


@pytest.mark.unit
class TestDataFrameMetadataCapabilities:
    """Test DataFrameMetadataCapabilities functionality."""

    def test_polars_capabilities(self):
        """Test Polars platform capabilities."""
        caps = POLARS_METADATA_CAPABILITIES
        assert caps.platform_name == "polars-df"
        assert caps.supports_schema_introspection is True
        assert caps.supports_describe is True
        assert caps.supports_catalog is False
        assert caps.supports_delta_lake is False

    def test_pandas_capabilities(self):
        """Test Pandas platform capabilities."""
        caps = PANDAS_METADATA_CAPABILITIES
        assert caps.platform_name == "pandas-df"
        assert caps.supports_schema_introspection is True
        assert caps.supports_describe is True
        assert caps.supports_catalog is False
        assert caps.supports_complex_types is False

    def test_pyspark_capabilities(self):
        """Test PySpark platform capabilities."""
        caps = PYSPARK_METADATA_CAPABILITIES
        assert caps.platform_name == "pyspark-df"
        assert caps.supports_schema_introspection is True
        assert caps.supports_catalog is True
        assert caps.supports_partitions is True

    def test_datafusion_capabilities(self):
        """Test DataFusion platform capabilities."""
        caps = DATAFUSION_METADATA_CAPABILITIES
        assert caps.platform_name == "datafusion-df"
        assert caps.supports_schema_introspection is True
        assert caps.supports_catalog is False

    def test_supports_operation_schema(self):
        """Test schema operation support checking."""
        caps = POLARS_METADATA_CAPABILITIES

        # Schema ops should be supported
        assert caps.supports_operation(MetadataOperationType.LIST_COLUMNS) is True
        assert caps.supports_operation(MetadataOperationType.GET_DTYPES) is True
        assert caps.supports_operation(MetadataOperationType.GET_SCHEMA) is True
        assert caps.supports_operation(MetadataOperationType.ROW_COUNT) is True

    def test_supports_operation_catalog(self):
        """Test catalog operation support checking."""
        polars_caps = POLARS_METADATA_CAPABILITIES
        pyspark_caps = PYSPARK_METADATA_CAPABILITIES

        # Polars doesn't support catalog ops
        assert polars_caps.supports_operation(MetadataOperationType.LIST_DATABASES) is False
        assert polars_caps.supports_operation(MetadataOperationType.LIST_TABLES) is False

        # PySpark does support catalog ops
        assert pyspark_caps.supports_operation(MetadataOperationType.LIST_DATABASES) is True
        assert pyspark_caps.supports_operation(MetadataOperationType.LIST_TABLES) is True

    def test_supports_operation_lakehouse(self):
        """Test lakehouse operation support checking."""
        caps = POLARS_METADATA_CAPABILITIES

        # Polars doesn't support Delta Lake ops by default
        assert caps.supports_operation(MetadataOperationType.TABLE_HISTORY) is False
        assert caps.supports_operation(MetadataOperationType.TABLE_DETAIL) is False

    def test_get_supported_operations(self):
        """Test getting list of supported operations."""
        caps = POLARS_METADATA_CAPABILITIES
        supported = caps.get_supported_operations()

        assert MetadataOperationType.LIST_COLUMNS in supported
        assert MetadataOperationType.GET_SCHEMA in supported
        assert MetadataOperationType.LIST_DATABASES not in supported

    def test_get_unsupported_operations(self):
        """Test getting list of unsupported operations."""
        caps = POLARS_METADATA_CAPABILITIES
        unsupported = caps.get_unsupported_operations()

        assert MetadataOperationType.LIST_DATABASES in unsupported
        assert MetadataOperationType.TABLE_HISTORY in unsupported
        assert MetadataOperationType.LIST_COLUMNS not in unsupported

    def test_get_supported_categories(self):
        """Test getting supported operation categories."""
        polars_caps = POLARS_METADATA_CAPABILITIES
        pyspark_caps = PYSPARK_METADATA_CAPABILITIES

        polars_categories = polars_caps.get_supported_categories()
        pyspark_categories = pyspark_caps.get_supported_categories()

        # Polars should support schema but not catalog
        assert MetadataOperationCategory.SCHEMA in polars_categories
        assert MetadataOperationCategory.CATALOG not in polars_categories

        # PySpark should support both
        assert MetadataOperationCategory.SCHEMA in pyspark_categories
        assert MetadataOperationCategory.CATALOG in pyspark_categories


@pytest.mark.unit
class TestGetPlatformCapabilities:
    """Test get_platform_capabilities function."""

    def test_get_polars_capabilities(self):
        """Test getting Polars capabilities."""
        caps = get_platform_capabilities("polars-df")
        assert caps.supports_schema_introspection is True
        assert caps.supports_catalog is False

    def test_get_pandas_capabilities(self):
        """Test getting Pandas capabilities."""
        caps = get_platform_capabilities("pandas-df")
        assert caps.supports_schema_introspection is True
        assert caps.supports_complex_types is False

    def test_get_pyspark_capabilities(self):
        """Test getting PySpark capabilities."""
        caps = get_platform_capabilities("pyspark-df")
        assert caps.supports_catalog is True

    def test_get_unknown_platform(self):
        """Test getting capabilities for unknown platform."""
        caps = get_platform_capabilities("unknown-platform")
        assert caps.supports_schema_introspection is True  # Basic support assumed
        assert caps.supports_catalog is False

    def test_get_capabilities_with_overrides(self):
        """Test getting capabilities with custom overrides."""
        caps = get_platform_capabilities(
            "pyspark-df",
            supports_delta_lake=True,
            supports_iceberg=True,
        )
        assert caps.supports_delta_lake is True
        assert caps.supports_iceberg is True


@pytest.mark.unit
class TestDataFrameMetadataResult:
    """Test DataFrameMetadataResult dataclass."""

    def test_success_result_creation(self):
        """Test creating a successful result."""
        start_time = time.time()
        result = DataFrameMetadataResult.success_result(
            operation_type=MetadataOperationType.LIST_COLUMNS,
            start_time=start_time,
            result_count=10,
            result_data=["col1", "col2", "col3"],
        )

        assert result.success is True
        assert result.operation_type == MetadataOperationType.LIST_COLUMNS
        assert result.result_count == 10
        assert result.result_data == ["col1", "col2", "col3"]
        assert result.error_message is None
        assert result.duration_ms >= 0

    def test_failure_result_creation(self):
        """Test creating a failure result."""
        result = DataFrameMetadataResult.failure_result(
            operation_type=MetadataOperationType.LIST_DATABASES,
            error_message="Catalog not configured",
        )

        assert result.success is False
        assert result.operation_type == MetadataOperationType.LIST_DATABASES
        assert result.error_message == "Catalog not configured"
        assert result.result_count == 0

    def test_failure_result_with_start_time(self):
        """Test creating a failure result with explicit start time."""
        start_time = time.time() - 0.1  # 100ms ago
        result = DataFrameMetadataResult.failure_result(
            operation_type=MetadataOperationType.TABLE_HISTORY,
            error_message="Delta Lake not available",
            start_time=start_time,
        )

        assert result.success is False
        # Use >= 99 to account for floating point timing imprecision
        assert result.duration_ms >= 99  # Approximately 100ms

    def test_result_metrics(self):
        """Test result with custom metrics."""
        start_time = time.time()
        result = DataFrameMetadataResult.success_result(
            operation_type=MetadataOperationType.ROW_COUNT,
            start_time=start_time,
            result_count=1,
            result_data=1000000,
            metrics={"row_count": 1000000, "scan_type": "metadata"},
        )

        assert result.metrics["row_count"] == 1000000
        assert result.metrics["scan_type"] == "metadata"


@pytest.mark.unit
class TestOperationCategories:
    """Test operation category mapping."""

    def test_schema_operations_category(self):
        """Test that schema operations map to schema category."""
        schema_ops = [
            MetadataOperationType.LIST_COLUMNS,
            MetadataOperationType.GET_DTYPES,
            MetadataOperationType.GET_SCHEMA,
            MetadataOperationType.DESCRIBE_STATS,
            MetadataOperationType.ROW_COUNT,
            MetadataOperationType.COLUMN_COUNT,
        ]

        for op in schema_ops:
            assert OPERATION_CATEGORIES[op] == MetadataOperationCategory.SCHEMA

    def test_catalog_operations_category(self):
        """Test that catalog operations map to catalog category."""
        catalog_ops = [
            MetadataOperationType.LIST_DATABASES,
            MetadataOperationType.LIST_TABLES,
            MetadataOperationType.LIST_TABLE_COLUMNS,
            MetadataOperationType.TABLE_EXISTS,
            MetadataOperationType.GET_TABLE_INFO,
        ]

        for op in catalog_ops:
            assert OPERATION_CATEGORIES[op] == MetadataOperationCategory.CATALOG

    def test_lakehouse_operations_category(self):
        """Test that lakehouse operations map to lakehouse category."""
        lakehouse_ops = [
            MetadataOperationType.TABLE_HISTORY,
            MetadataOperationType.TABLE_DETAIL,
            MetadataOperationType.FILE_METADATA,
            MetadataOperationType.PARTITION_INFO,
            MetadataOperationType.SNAPSHOT_INFO,
        ]

        for op in lakehouse_ops:
            assert OPERATION_CATEGORIES[op] == MetadataOperationCategory.LAKEHOUSE


@pytest.mark.unit
class TestUnsupportedOperationError:
    """Test UnsupportedOperationError exception."""

    def test_error_message(self):
        """Test error message formatting."""
        error = UnsupportedOperationError(
            operation=MetadataOperationType.LIST_DATABASES,
            platform_name="polars-df",
        )

        assert "polars-df" in str(error)
        assert "list_databases" in str(error)

    def test_error_with_suggestion(self):
        """Test error message with suggestion."""
        error = UnsupportedOperationError(
            operation=MetadataOperationType.LIST_DATABASES,
            platform_name="polars-df",
            suggestion="Use pyspark-df with a configured catalog",
        )

        assert "Use pyspark-df" in str(error)


@pytest.mark.unit
class TestGetUnsupportedMessage:
    """Test get_unsupported_message function."""

    def test_catalog_operation_message(self):
        """Test message for unsupported catalog operation."""
        message = get_unsupported_message(MetadataOperationType.LIST_DATABASES, "polars-df")

        assert "catalog operations" in message.lower()
        assert "pyspark-df" in message.lower()
        assert "schema introspection" in message.lower()

    def test_lakehouse_delta_message(self):
        """Test message for unsupported Delta Lake operation."""
        message = get_unsupported_message(MetadataOperationType.TABLE_HISTORY, "polars-df")

        assert "delta lake" in message.lower()
        assert "pyspark-df" in message.lower() or "polars" in message.lower()

    def test_lakehouse_iceberg_message(self):
        """Test message for unsupported Iceberg operation."""
        message = get_unsupported_message(MetadataOperationType.SNAPSHOT_INFO, "polars-df")

        assert "iceberg" in message.lower()


@pytest.mark.unit
class TestDataFrameMetadataOperationsManager:
    """Test DataFrameMetadataOperationsManager class."""

    def test_manager_initialization_polars(self):
        """Test manager initialization for Polars."""
        manager = DataFrameMetadataOperationsManager("polars-df")
        assert manager.platform_name == "polars-df"

        caps = manager.get_capabilities()
        assert caps.supports_schema_introspection is True
        assert caps.supports_catalog is False

    def test_manager_initialization_pandas(self):
        """Test manager initialization for Pandas."""
        manager = DataFrameMetadataOperationsManager("pandas-df")
        assert manager.platform_name == "pandas-df"

    def test_manager_initialization_pyspark_no_session(self):
        """Test manager initialization for PySpark without session."""
        manager = DataFrameMetadataOperationsManager("pyspark-df")
        assert manager.spark_session is None

        caps = manager.get_capabilities()
        assert caps.supports_catalog is True

    def test_supports_operation(self):
        """Test supports_operation method."""
        manager = DataFrameMetadataOperationsManager("polars-df")

        assert manager.supports_operation(MetadataOperationType.LIST_COLUMNS) is True
        assert manager.supports_operation(MetadataOperationType.LIST_DATABASES) is False

    def test_get_supported_operations(self):
        """Test get_supported_operations method."""
        manager = DataFrameMetadataOperationsManager("polars-df")
        supported = manager.get_supported_operations()

        assert MetadataOperationType.GET_SCHEMA in supported
        assert MetadataOperationType.LIST_DATABASES not in supported

    def test_validate_operation_success(self):
        """Test validate_operation for supported operation."""
        manager = DataFrameMetadataOperationsManager("polars-df")

        # Should not raise
        manager.validate_operation(MetadataOperationType.LIST_COLUMNS)

    def test_validate_operation_failure(self):
        """Test validate_operation for unsupported operation."""
        manager = DataFrameMetadataOperationsManager("polars-df")

        with pytest.raises(UnsupportedOperationError) as exc_info:
            manager.validate_operation(MetadataOperationType.LIST_DATABASES)

        assert "polars-df" in str(exc_info.value)
        assert "list_databases" in str(exc_info.value)


@pytest.mark.unit
class TestGetDataFrameMetadataManager:
    """Test get_dataframe_metadata_manager factory function."""

    def test_get_polars_manager(self):
        """Test getting Polars manager."""
        manager = get_dataframe_metadata_manager("polars-df")
        assert manager is not None
        assert manager.platform_name == "polars-df"

    def test_get_pandas_manager(self):
        """Test getting Pandas manager."""
        manager = get_dataframe_metadata_manager("pandas-df")
        assert manager is not None

    def test_get_non_dataframe_platform(self):
        """Test getting manager for non-DataFrame platform returns None."""
        manager = get_dataframe_metadata_manager("duckdb")
        assert manager is None

        manager = get_dataframe_metadata_manager("snowflake")
        assert manager is None


@pytest.mark.unit
class TestPolarsMetadataOperations:
    """Integration tests for Polars metadata operations."""

    @pytest.fixture
    def sample_polars_df(self):
        """Create a sample Polars DataFrame for testing."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        return pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
                "age": [25, 30, 35, 40, 45],
                "salary": [50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
            }
        )

    @pytest.fixture
    def polars_manager(self):
        """Create Polars metadata operations manager."""
        return DataFrameMetadataOperationsManager("polars-df")

    def test_execute_list_columns(self, polars_manager, sample_polars_df):
        """Test listing columns from Polars DataFrame."""
        result = polars_manager.execute_list_columns(sample_polars_df)

        assert result.success is True
        assert result.result_count == 4
        assert result.result_data == ["id", "name", "age", "salary"]
        assert result.duration_ms >= 0

    def test_execute_get_dtypes(self, polars_manager, sample_polars_df):
        """Test getting dtypes from Polars DataFrame."""
        result = polars_manager.execute_get_dtypes(sample_polars_df)

        assert result.success is True
        assert result.result_count == 4
        assert "id" in result.result_data
        assert "name" in result.result_data

    def test_execute_get_schema(self, polars_manager, sample_polars_df):
        """Test getting full schema from Polars DataFrame."""
        result = polars_manager.execute_get_schema(sample_polars_df)

        assert result.success is True
        assert result.result_count == 4
        assert isinstance(result.result_data, list)

        # Check schema structure
        schema = result.result_data
        assert all("name" in col for col in schema)
        assert all("dtype" in col for col in schema)
        assert all("nullable" in col for col in schema)

    def test_execute_describe_stats(self, polars_manager, sample_polars_df):
        """Test getting summary statistics from Polars DataFrame."""
        result = polars_manager.execute_describe_stats(sample_polars_df)

        assert result.success is True
        assert result.result_count > 0

    def test_execute_row_count(self, polars_manager, sample_polars_df):
        """Test getting row count from Polars DataFrame."""
        result = polars_manager.execute_row_count(sample_polars_df)

        assert result.success is True
        assert result.result_data == 5
        assert result.metrics.get("row_count") == 5

    def test_execute_column_count(self, polars_manager, sample_polars_df):
        """Test getting column count from Polars DataFrame."""
        result = polars_manager.execute_column_count(sample_polars_df)

        assert result.success is True
        assert result.result_data == 4
        assert result.metrics.get("column_count") == 4

    def test_catalog_operations_fail_on_polars(self, polars_manager):
        """Test that catalog operations raise appropriate errors on Polars."""
        with pytest.raises(UnsupportedOperationError):
            polars_manager.execute_list_databases()


@pytest.mark.unit
class TestPandasMetadataOperations:
    """Integration tests for Pandas metadata operations."""

    @pytest.fixture
    def sample_pandas_df(self):
        """Create a sample Pandas DataFrame for testing."""
        try:
            import pandas as pd
        except ImportError:
            pytest.skip("Pandas not installed")

        return pd.DataFrame(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
                "age": [25, 30, 35, 40, 45],
                "salary": [50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
            }
        )

    @pytest.fixture
    def pandas_manager(self):
        """Create Pandas metadata operations manager."""
        return DataFrameMetadataOperationsManager("pandas-df")

    def test_execute_list_columns(self, pandas_manager, sample_pandas_df):
        """Test listing columns from Pandas DataFrame."""
        result = pandas_manager.execute_list_columns(sample_pandas_df)

        assert result.success is True
        assert result.result_count == 4
        assert result.result_data == ["id", "name", "age", "salary"]

    def test_execute_get_dtypes(self, pandas_manager, sample_pandas_df):
        """Test getting dtypes from Pandas DataFrame."""
        result = pandas_manager.execute_get_dtypes(sample_pandas_df)

        assert result.success is True
        assert result.result_count == 4
        assert "id" in result.result_data

    def test_execute_get_schema(self, pandas_manager, sample_pandas_df):
        """Test getting full schema from Pandas DataFrame."""
        result = pandas_manager.execute_get_schema(sample_pandas_df)

        assert result.success is True
        assert result.result_count == 4

        # Check that nullable detection works
        schema = result.result_data
        assert all("nullable" in col for col in schema)

    def test_execute_row_count(self, pandas_manager, sample_pandas_df):
        """Test getting row count from Pandas DataFrame."""
        result = pandas_manager.execute_row_count(sample_pandas_df)

        assert result.success is True
        assert result.result_data == 5

    def test_execute_column_count(self, pandas_manager, sample_pandas_df):
        """Test getting column count from Pandas DataFrame."""
        result = pandas_manager.execute_column_count(sample_pandas_df)

        assert result.success is True
        assert result.result_data == 4


@pytest.mark.unit
class TestComplexityOperations:
    """Test complexity testing operations."""

    @pytest.fixture
    def wide_polars_df(self):
        """Create a wide Polars DataFrame for testing (100+ columns)."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        # Create a DataFrame with 150 columns
        data = {f"col_{i:03d}": list(range(10)) for i in range(150)}
        return pl.DataFrame(data)

    @pytest.fixture
    def complex_polars_df(self):
        """Create a Polars DataFrame with complex types."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        return pl.DataFrame(
            {
                "id": [1, 2, 3],
                "simple_col": ["a", "b", "c"],
                "array_col": [[1, 2], [3, 4], [5, 6]],
                "struct_col": [{"x": 1, "y": 2}, {"x": 3, "y": 4}, {"x": 5, "y": 6}],
            }
        )

    @pytest.fixture
    def polars_manager(self):
        """Create Polars metadata operations manager."""
        return DataFrameMetadataOperationsManager("polars-df")

    def test_execute_wide_table_schema(self, polars_manager, wide_polars_df):
        """Test wide table schema introspection."""
        result = polars_manager.execute_wide_table_schema(wide_polars_df)

        assert result.success is True
        assert result.result_count == 150
        assert result.metrics["column_count"] == 150
        assert result.metrics["is_wide_table"] is True
        assert len(result.result_data) == 150

    def test_execute_complex_type_introspection(self, polars_manager, complex_polars_df):
        """Test complex type introspection."""
        result = polars_manager.execute_complex_type_introspection(complex_polars_df)

        assert result.success is True
        assert result.result_count >= 2  # At least array_col and struct_col
        assert result.metrics["complex_column_count"] >= 2

    def test_wide_table_schema_pandas(self):
        """Test wide table schema introspection with Pandas."""
        try:
            import pandas as pd
        except ImportError:
            pytest.skip("Pandas not installed")

        # Create a wide DataFrame
        data = {f"col_{i:03d}": range(10) for i in range(150)}
        df = pd.DataFrame(data)

        manager = DataFrameMetadataOperationsManager("pandas-df")
        result = manager.execute_wide_table_schema(df)

        assert result.success is True
        assert result.result_count == 150
        assert result.metrics["is_wide_table"] is True

    def test_large_catalog_list_requires_spark(self):
        """Test that large catalog list requires SparkSession."""
        manager = DataFrameMetadataOperationsManager("polars-df")

        with pytest.raises(UnsupportedOperationError):
            manager.execute_large_catalog_list()


@pytest.mark.unit
class TestBenchmarkDataFrameIntegration:
    """Test MetadataPrimitivesBenchmark DataFrame integration."""

    def test_supports_dataframe_mode(self):
        """Test that benchmark reports DataFrame mode support."""
        from benchbox.core.metadata_primitives import MetadataPrimitivesBenchmark

        benchmark = MetadataPrimitivesBenchmark()
        assert benchmark.supports_dataframe_mode() is True

    def test_get_dataframe_operations(self):
        """Test getting DataFrame operations from benchmark."""
        from benchbox.core.metadata_primitives import MetadataPrimitivesBenchmark

        benchmark = MetadataPrimitivesBenchmark()
        ops = benchmark.get_dataframe_operations("polars-df")

        assert ops is not None
        assert isinstance(ops, DataFrameMetadataOperationsManager)

    def test_get_dataframe_operations_invalid_platform(self):
        """Test error for invalid platform."""
        from benchbox.core.metadata_primitives import MetadataPrimitivesBenchmark

        benchmark = MetadataPrimitivesBenchmark()

        with pytest.raises(ValueError, match="does not support DataFrame"):
            benchmark.get_dataframe_operations("duckdb")

    def test_get_dataframe_capabilities(self):
        """Test getting DataFrame capabilities from benchmark."""
        from benchbox.core.metadata_primitives import MetadataPrimitivesBenchmark

        benchmark = MetadataPrimitivesBenchmark()
        caps = benchmark.get_dataframe_capabilities("polars-df")

        assert caps is not None
        assert caps.supports_schema_introspection is True

    def test_run_dataframe_benchmark_polars(self):
        """Test running DataFrame benchmark with Polars."""
        try:
            import polars as pl
        except ImportError:
            pytest.skip("Polars not installed")

        from benchbox.core.metadata_primitives import MetadataPrimitivesBenchmark

        benchmark = MetadataPrimitivesBenchmark()
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

        result = benchmark.run_dataframe_benchmark(
            platform_name="polars-df",
            dataframes={"test_table": df},
            categories=["schema"],
            iterations=1,
        )

        assert result.total_queries > 0
        assert result.successful_queries > 0
        assert "schema" in result.category_summary
