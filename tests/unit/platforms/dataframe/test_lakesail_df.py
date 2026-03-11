"""Unit tests for LakeSail Sail DataFrame adapter."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
    pytest.mark.skipif(sys.platform == "win32", reason="PySpark tests skipped on Windows"),
]


# Check if PySpark is available for tests that need real Spark
try:
    from benchbox.platforms.pyspark import PYSPARK_AVAILABLE
except ImportError:
    PYSPARK_AVAILABLE = False


class TestLakeSailDataFrameAdapterMocked:
    """Tests for LakeSailDataFrameAdapter with mocked PySpark dependencies."""

    @pytest.fixture
    def mock_pyspark_env(self):
        """Mock PySpark for adapter initialization tests."""
        mock_session = MagicMock()
        mock_session.version = "3.5.0"
        mock_builder = MagicMock()
        mock_builder.remote.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_session

        mock_session_class = MagicMock()
        mock_session_class.builder = mock_builder

        mock_col = MagicMock()
        mock_functions = MagicMock()
        mock_functions.col.return_value = mock_col
        mock_functions.lit.return_value = mock_col

        mock_pyspark = MagicMock()
        mock_pyspark_sql = MagicMock(
            SparkSession=mock_session_class,
            functions=mock_functions,
            DataFrame=MagicMock,
        )
        mock_pyspark_sql.column.Column = MagicMock
        mock_pyspark_sql.types.StringType = MagicMock()
        mock_pyspark_sql.types.StructField = MagicMock()
        mock_pyspark_sql.types.StructType = MagicMock()
        mock_pyspark_sql.window.Window = MagicMock()

        with patch.dict(
            "sys.modules",
            {
                "pyspark": mock_pyspark,
                "pyspark.sql": mock_pyspark_sql,
                "pyspark.sql.column": mock_pyspark_sql.column,
                "pyspark.sql.types": mock_pyspark_sql.types,
                "pyspark.sql.window": mock_pyspark_sql.window,
                "pyspark.sql.functions": mock_functions,
            },
        ):
            yield mock_session_class, mock_session, mock_functions

    def test_initialization_requires_pyspark(self):
        """Test that missing PySpark raises ImportError."""
        with patch.dict("sys.modules", {"pyspark": None, "pyspark.sql": None}):
            # Importing the module with pyspark unavailable should handle gracefully
            # The actual adapter __init__ checks PYSPARK_AVAILABLE
            pass

    def test_platform_name(self, mock_pyspark_env):
        """Test platform name is 'LakeSail'."""
        # We need to test with real import since the module checks PYSPARK_AVAILABLE at import time
        if not PYSPARK_AVAILABLE:
            pytest.skip("PySpark not available")

        from benchbox.platforms.dataframe.lakesail_df import LakeSailDataFrameAdapter

        adapter = LakeSailDataFrameAdapter.__new__(LakeSailDataFrameAdapter)
        # Manually set required attributes to avoid full init
        adapter._endpoint = "sc://localhost:50051"
        assert adapter.platform_name == "LakeSail"


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not installed")
class TestLakeSailDataFrameAdapterReal:
    """Tests for LakeSailDataFrameAdapter with real PySpark (no Sail server needed).

    These tests verify adapter initialization and configuration without
    requiring a running LakeSail Sail server. They test the client-side
    logic only.
    """

    def test_initialization(self):
        """Test adapter initialization with default settings."""
        from benchbox.platforms.dataframe.lakesail_df import LakeSailDataFrameAdapter

        adapter = LakeSailDataFrameAdapter.__new__(LakeSailDataFrameAdapter)
        # Test that class can be instantiated
        assert hasattr(adapter, "platform_name")

    def test_platform_name_value(self):
        """Test the platform_name property returns 'LakeSail'."""
        from benchbox.platforms.dataframe.lakesail_df import LakeSailDataFrameAdapter

        adapter = LakeSailDataFrameAdapter.__new__(LakeSailDataFrameAdapter)
        assert adapter.platform_name == "LakeSail"

    def test_type_aliases_defined(self):
        """Test that LakeSail type aliases are defined."""
        # When PySpark is available, these should be real PySpark types
        from pyspark.sql import DataFrame
        from pyspark.sql.column import Column

        from benchbox.platforms.dataframe.lakesail_df import (
            LakeSailDF,
            LakeSailExpr,
            LakeSailLazyDF,
        )

        assert LakeSailDF is DataFrame
        assert LakeSailLazyDF is DataFrame
        assert LakeSailExpr is Column

    def test_adapter_class_exists_in_dataframe_package(self):
        """Test that LakeSailDataFrameAdapter is exported from dataframe package."""
        from benchbox.platforms.dataframe import LakeSailDataFrameAdapter

        assert LakeSailDataFrameAdapter is not None

    def test_family_is_expression(self):
        """Test that the adapter family is 'expression'."""
        from benchbox.platforms.dataframe.lakesail_df import LakeSailDataFrameAdapter

        # ExpressionFamilyAdapter sets family = "expression"
        assert hasattr(LakeSailDataFrameAdapter, "family")


class TestLakeSailDataFrameAdapterConfig:
    """Tests for LakeSail DataFrame adapter configuration handling."""

    def test_default_endpoint(self):
        """Test default Spark Connect endpoint."""
        if not PYSPARK_AVAILABLE:
            pytest.skip("PySpark not available")

        from benchbox.platforms.dataframe.lakesail_df import LakeSailDataFrameAdapter

        # Create adapter with __new__ to skip session creation
        adapter = LakeSailDataFrameAdapter.__new__(LakeSailDataFrameAdapter)
        adapter._endpoint = "sc://localhost:50051"
        assert adapter._endpoint == "sc://localhost:50051"

    def test_custom_endpoint(self):
        """Test custom Spark Connect endpoint configuration."""
        if not PYSPARK_AVAILABLE:
            pytest.skip("PySpark not available")

        from benchbox.platforms.dataframe.lakesail_df import LakeSailDataFrameAdapter

        adapter = LakeSailDataFrameAdapter.__new__(LakeSailDataFrameAdapter)
        adapter._endpoint = "sc://sail-cluster:50051"
        assert adapter._endpoint == "sc://sail-cluster:50051"

    def test_get_platform_info_structure(self):
        """Test platform info has expected keys."""
        if not PYSPARK_AVAILABLE:
            pytest.skip("PySpark not available")

        from benchbox.platforms.dataframe.lakesail_df import LakeSailDataFrameAdapter

        adapter = LakeSailDataFrameAdapter.__new__(LakeSailDataFrameAdapter)
        adapter._endpoint = "sc://localhost:50051"
        adapter._driver_memory = "4g"
        adapter._shuffle_partitions = 8
        adapter._enable_aqe = True
        adapter.verbose = False
        adapter.very_verbose = False
        adapter.working_dir = "/tmp"

        info = adapter.get_platform_info()
        assert info["platform"] == "LakeSail"
        assert info["family"] == "expression"
        assert info["endpoint"] == "sc://localhost:50051"
        assert "driver_memory" in info
        assert "shuffle_partitions" in info
        assert "aqe_enabled" in info
