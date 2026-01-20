"""Tests for core configuration utilities.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from benchbox.core.config_utils import (
    build_benchmark_config,
    build_platform_adapter_config,
    deep_merge_dicts,
    load_config_file,
    save_config_file,
    validate_config_sections,
    validate_numeric_config,
)

pytestmark = pytest.mark.fast


class TestDeepMergeDicts:
    """Test deep dictionary merging utility."""

    def test_simple_merge(self):
        """Test merging simple dictionaries."""
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}
        result = deep_merge_dicts(base, override)

        assert result == {"a": 1, "b": 3, "c": 4}

    def test_nested_merge(self):
        """Test merging nested dictionaries."""
        base = {"database": {"host": "localhost", "port": 5432}, "logging": {"level": "INFO"}}
        override = {"database": {"port": 3306, "username": "admin"}, "cache": {"enabled": True}}

        result = deep_merge_dicts(base, override)

        expected = {
            "database": {"host": "localhost", "port": 3306, "username": "admin"},
            "logging": {"level": "INFO"},
            "cache": {"enabled": True},
        }
        assert result == expected

    def test_original_unchanged(self):
        """Test that original dictionaries remain unchanged."""
        base = {"a": {"nested": 1}}
        override = {"a": {"nested": 2}}

        deep_merge_dicts(base, override)

        assert base == {"a": {"nested": 1}}
        assert override == {"a": {"nested": 2}}

    def test_override_non_dict_with_dict(self):
        """Test overriding non-dict values with dict values."""
        base = {"config": "simple_value"}
        override = {"config": {"complex": "value"}}

        result = deep_merge_dicts(base, override)

        assert result == {"config": {"complex": "value"}}


class TestLoadConfigFile:
    """Test configuration file loading utility."""

    def test_load_yaml_file(self, tmp_path):
        """Test loading YAML configuration file."""
        config_file = tmp_path / "config.yaml"
        config_data = {"key": "value", "nested": {"item": 123}}

        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        result = load_config_file(config_file)
        assert result == config_data

    def test_load_json_file(self, tmp_path):
        """Test loading JSON configuration file."""
        config_file = tmp_path / "config.json"
        config_data = {"key": "value", "nested": {"item": 123}}

        with open(config_file, "w") as f:
            json.dump(config_data, f)

        result = load_config_file(config_file)
        assert result == config_data

    def test_load_file_no_extension(self, tmp_path):
        """Test loading file without extension (tries YAML first)."""
        config_file = tmp_path / "config"
        config_data = {"key": "value"}

        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        result = load_config_file(config_file)
        assert result == config_data

    def test_load_nonexistent_file(self, tmp_path):
        """Test loading nonexistent file raises FileNotFoundError."""
        config_file = tmp_path / "nonexistent.yaml"

        with pytest.raises(FileNotFoundError):
            load_config_file(config_file)

    def test_load_invalid_yaml(self, tmp_path):
        """Test loading invalid YAML raises ValueError."""
        config_file = tmp_path / "invalid.yaml"

        with open(config_file, "w") as f:
            f.write("invalid: yaml: content: [")

        with pytest.raises(ValueError, match="Failed to parse configuration file"):
            load_config_file(config_file)

    def test_load_empty_file_returns_empty_dict(self, tmp_path):
        """Test loading empty file returns empty dict."""
        config_file = tmp_path / "empty.yaml"
        config_file.touch()

        result = load_config_file(config_file)
        assert result == {}


class TestSaveConfigFile:
    """Test configuration file saving utility."""

    def test_save_yaml_file(self, tmp_path):
        """Test saving configuration as YAML."""
        config_file = tmp_path / "config.yaml"
        config_data = {"key": "value", "nested": {"item": 123}}

        save_config_file(config_data, config_file, "yaml")

        with open(config_file) as f:
            loaded_data = yaml.safe_load(f)

        assert loaded_data == config_data

    def test_save_json_file(self, tmp_path):
        """Test saving configuration as JSON."""
        config_file = tmp_path / "config.json"
        config_data = {"key": "value", "nested": {"item": 123}}

        save_config_file(config_data, config_file, "json")

        with open(config_file) as f:
            loaded_data = json.load(f)

        assert loaded_data == config_data

    def test_save_creates_directory(self, tmp_path):
        """Test saving creates parent directories."""
        config_file = tmp_path / "nested" / "dir" / "config.yaml"
        config_data = {"key": "value"}

        save_config_file(config_data, config_file, "yaml")

        assert config_file.exists()
        assert config_file.parent.exists()

    def test_save_invalid_format(self, tmp_path):
        """Test saving with invalid format raises ValueError."""
        config_file = tmp_path / "config.txt"
        config_data = {"key": "value"}

        with pytest.raises(ValueError, match="Unsupported format"):
            save_config_file(config_data, config_file, "invalid")


class TestBuildBenchmarkConfig:
    """Test benchmark configuration building utility."""

    def test_build_from_dict(self):
        """Test building config from dictionary."""
        config = {
            "scale_factor": 1.0,
            "very_verbose": True,
            "force": True,
            "benchmark": "tpch",
            "platform": "duckdb",
            "output": "/custom/path",
            "compress": True,
        }

        result = build_benchmark_config(config)

        expected = {
            "scale_factor": 1.0,
            "verbose": True,
            "force_regenerate": True,
            "output_dir": "/custom/path/tpch_sf1",
            "compress_data": True,
            "compression_type": "zstd",
        }
        assert result == expected

    def test_build_from_namespace(self):
        """Test building config from argparse Namespace."""
        args = SimpleNamespace(
            scale=0.5, verbose=True, force=False, benchmark="tpcds", platform="duckdb", output=None, compress=False
        )

        result = build_benchmark_config(args, platform="duckdb")

        expected = {
            "scale_factor": 0.5,
            "verbose": True,
            "force_regenerate": False,
            "output_dir": str(Path.cwd() / "benchmark_runs" / "datagen" / "tpcds_sf05"),
        }
        assert result == expected

    def test_build_default_output_dir(self):
        """Test building config with default output directory."""
        config = {"scale_factor": 0.01, "benchmark": "tpch", "platform": "duckdb"}

        result = build_benchmark_config(config)

        assert result["output_dir"] == str(Path.cwd() / "benchmark_runs" / "datagen" / "tpch_sf001")

    def test_build_clickhouse_output_dir(self):
        """Test building config for ClickHouse platform."""
        config = {
            "scale_factor": 0.01,
            "benchmark": "tpch",
            "platform": "clickhouse",
            "data_path": "/custom/clickhouse/path",
        }

        result = build_benchmark_config(config)

        assert result["output_dir"] == "/custom/clickhouse/path"


class TestBuildPlatformAdapterConfig:
    """Test platform adapter configuration building utility."""

    def test_build_duckdb_config_from_dict(self):
        """Test building DuckDB config from dictionary."""
        config = {"benchmark": "tpch", "scale_factor": 1.0, "memory_limit": "8GB", "force": True}

        result = build_platform_adapter_config("duckdb", config, scale_factor=1.0)

        assert result["memory_limit"] == "8GB"
        assert result["force_recreate"] is True
        assert "database_path" in result
        assert "tpch_sf1" in result["database_path"]

    def test_build_databricks_config_from_namespace(self):
        """Test building Databricks config from argparse Namespace."""
        args = SimpleNamespace(
            server_hostname="test.databricks.com",
            http_path="/sql/1.0/warehouses/test",
            access_token="token123",
            catalog="workspace",
            benchmark="tpcds",
        )

        result = build_platform_adapter_config("databricks", args, benchmark_name="tpcds")

        expected = {
            "server_hostname": "test.databricks.com",
            "http_path": "/sql/1.0/warehouses/test",
            "access_token": "token123",
            "catalog": "workspace",
            "schema": "tpcds_schema",
        }
        assert result == expected

    def test_build_clickhouse_config(self):
        """Test building ClickHouse config."""
        args = SimpleNamespace(
            mode="server",
            data_path="/tmp/ch_data",
            host="clickhouse.example.com",
            port=9000,
            user="admin",
            password="secret",
            secure=True,
        )

        result = build_platform_adapter_config("clickhouse", args)

        expected = {
            "mode": "server",
            "data_path": "/tmp/ch_data",
            "host": "clickhouse.example.com",
            "port": 9000,
            "user": "admin",
            "password": "secret",
            "secure": True,
        }
        assert result == expected

    def test_build_unknown_platform_config(self):
        """Test building config for unknown platform returns empty."""
        args = SimpleNamespace()

        result = build_platform_adapter_config("unknown", args)

        assert result == {}


class TestValidateConfigSections:
    """Test configuration section validation utility."""

    def test_validate_all_sections_present(self):
        """Test validation passes when all sections are present."""
        config = {"database": {}, "benchmarks": {}, "output": {}}
        required = ["database", "benchmarks", "output"]

        result = validate_config_sections(config, required)

        assert result is True

    def test_validate_missing_sections(self):
        """Test validation fails when sections are missing."""
        config = {"database": {}, "output": {}}
        required = ["database", "benchmarks", "output"]

        result = validate_config_sections(config, required)

        assert result is False

    def test_validate_empty_config(self):
        """Test validation fails for empty config."""
        config = {}
        required = ["database"]

        result = validate_config_sections(config, required)

        assert result is False


class TestValidateNumericConfig:
    """Test numeric configuration validation utility."""

    def test_validate_valid_values(self):
        """Test validation passes for valid numeric values."""
        config = {"timeout": 30, "scale_factor": 1.5, "max_workers": 4}
        validations = {"timeout": (0, 300, True), "scale_factor": (0.01, 100.0, True), "max_workers": (1, 16, True)}

        errors = validate_numeric_config(config, validations)

        assert errors == []

    def test_validate_out_of_range_values(self):
        """Test validation fails for out-of-range values."""
        config = {"timeout": -5, "scale_factor": 200.0, "max_workers": 0}
        validations = {"timeout": (0, 300, True), "scale_factor": (0.01, 100.0, True), "max_workers": (1, 16, True)}

        errors = validate_numeric_config(config, validations)

        assert len(errors) == 3
        assert "timeout" in errors[0]
        assert "scale_factor" in errors[1]
        assert "max_workers" in errors[2]

    def test_validate_missing_required_values(self):
        """Test validation fails for missing required values."""
        config = {}
        validations = {"timeout": (0, 300, True), "optional_setting": (0, 100, False)}

        errors = validate_numeric_config(config, validations)

        assert len(errors) == 1
        assert "timeout" in errors[0]
        assert "missing" in errors[0]

    def test_validate_non_numeric_values(self):
        """Test validation fails for non-numeric values."""
        config = {"timeout": "invalid", "scale_factor": None}
        validations = {"timeout": (0, 300, True), "scale_factor": (0.01, 100.0, True)}

        errors = validate_numeric_config(config, validations)

        assert len(errors) == 2
        assert "numeric" in errors[0]
        assert "numeric" in errors[1]
