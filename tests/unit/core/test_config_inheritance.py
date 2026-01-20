"""Unit tests for platform configuration inheritance.

Tests the configuration inheritance system that allows child platforms
to inherit SQL dialect, benchmark compatibility, and other settings
from parent platforms.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox.core.config_inheritance import (
    build_inherited_config,
    get_benchmark_compatibility,
    get_inherited_dialect,
    get_platform_family_dialect,
    resolve_dialect_for_query_translation,
)


class TestGetInheritedDialect:
    """Tests for get_inherited_dialect function."""

    @pytest.mark.fast
    def test_platform_without_inheritance_returns_self(self):
        """Platform without inheritance returns its own name as dialect."""
        dialect = get_inherited_dialect("duckdb")
        assert dialect == "duckdb"

    @pytest.mark.fast
    def test_clickhouse_returns_clickhouse(self):
        """ClickHouse returns clickhouse dialect."""
        dialect = get_inherited_dialect("clickhouse")
        assert dialect == "clickhouse"

    @pytest.mark.fast
    def test_trino_returns_trino(self):
        """Trino returns trino dialect."""
        dialect = get_inherited_dialect("trino")
        assert dialect == "trino"

    @pytest.mark.fast
    def test_case_insensitive(self):
        """Dialect resolution is case-insensitive."""
        assert get_inherited_dialect("DuckDB") == "duckdb"
        assert get_inherited_dialect("CLICKHOUSE") == "clickhouse"


class TestGetPlatformFamilyDialect:
    """Tests for get_platform_family_dialect function."""

    @pytest.mark.fast
    def test_clickhouse_family(self):
        """ClickHouse returns clickhouse family dialect."""
        dialect = get_platform_family_dialect("clickhouse")
        assert dialect == "clickhouse"

    @pytest.mark.fast
    def test_duckdb_family(self):
        """DuckDB returns duckdb family dialect."""
        dialect = get_platform_family_dialect("duckdb")
        assert dialect == "duckdb"

    @pytest.mark.fast
    def test_firebolt_family(self):
        """Firebolt returns firebolt family dialect."""
        dialect = get_platform_family_dialect("firebolt")
        assert dialect == "firebolt"


class TestBuildInheritedConfig:
    """Tests for build_inherited_config function."""

    @pytest.mark.fast
    def test_no_user_config_returns_empty_base(self):
        """Without user config, returns base configuration."""
        config = build_inherited_config("duckdb")
        assert isinstance(config, dict)

    @pytest.mark.fast
    def test_user_config_preserved(self):
        """User configuration is preserved in result."""
        user_config = {"timeout": 30, "memory_limit": "1GB"}
        config = build_inherited_config("duckdb", user_config)

        assert config["timeout"] == 30
        assert config["memory_limit"] == "1GB"

    @pytest.mark.fast
    def test_platform_family_added(self):
        """Platform family is added to config when available."""
        config = build_inherited_config("clickhouse")
        assert "_platform_family" in config
        assert config["_platform_family"] == "clickhouse"


class TestResolveDialectForQueryTranslation:
    """Tests for resolve_dialect_for_query_translation function."""

    @pytest.mark.fast
    def test_duckdb_returns_duckdb(self):
        """DuckDB resolves to duckdb dialect."""
        dialect = resolve_dialect_for_query_translation("duckdb")
        assert dialect == "duckdb"

    @pytest.mark.fast
    def test_clickhouse_returns_clickhouse(self):
        """ClickHouse resolves to clickhouse dialect."""
        dialect = resolve_dialect_for_query_translation("clickhouse")
        assert dialect == "clickhouse"

    @pytest.mark.fast
    def test_trino_returns_trino(self):
        """Trino resolves to trino dialect."""
        dialect = resolve_dialect_for_query_translation("trino")
        assert dialect == "trino"

    @pytest.mark.fast
    def test_firebolt_returns_firebolt(self):
        """Firebolt resolves to firebolt dialect."""
        dialect = resolve_dialect_for_query_translation("firebolt")
        assert dialect == "firebolt"

    @pytest.mark.fast
    def test_case_insensitive(self):
        """Dialect resolution is case-insensitive."""
        assert resolve_dialect_for_query_translation("DuckDB") == "duckdb"


class TestGetBenchmarkCompatibility:
    """Tests for get_benchmark_compatibility function."""

    @pytest.mark.fast
    def test_returns_dict(self):
        """Benchmark compatibility returns a dictionary."""
        compat = get_benchmark_compatibility("duckdb")
        assert isinstance(compat, dict)

    @pytest.mark.fast
    def test_handles_unknown_platform(self):
        """Handles platforms without explicit compatibility."""
        # Should not raise, just return empty dict
        compat = get_benchmark_compatibility("clickhouse")
        assert isinstance(compat, dict)
