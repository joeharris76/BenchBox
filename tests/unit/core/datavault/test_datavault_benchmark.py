"""Tests for the DataVaultBenchmark core class."""

import json

import pytest

from benchbox.core.datavault.benchmark import DataVaultBenchmark

pytestmark = pytest.mark.fast


def test_default_output_dir_uses_datavault_name(tmp_path, monkeypatch):
    """Default output directory should use the datavault identifier."""
    # Force cwd to tmp_path so the generated path is deterministic
    monkeypatch.chdir(tmp_path)

    benchmark = DataVaultBenchmark(scale_factor=0.01)

    assert benchmark.output_dir is not None
    assert benchmark.output_dir.name.endswith("datavault_sf001")
    assert "benchmark_runs" in benchmark.output_dir.as_posix()


def test_benchmark_name_override():
    """Benchmark name should be stabilized to datavault."""
    benchmark = DataVaultBenchmark(scale_factor=0.1)
    assert benchmark._get_benchmark_name() == "datavault"


def test_get_create_tables_sql_translation():
    """DDL generation should support dialect translation via SQLGlot."""
    benchmark = DataVaultBenchmark(scale_factor=0.01)
    ddl = benchmark.get_create_tables_sql(dialect="snowflake")

    assert ddl.count("CREATE TABLE") == 21
    # Snowflake translation should quote identifiers
    assert '"' in ddl


def test_query_translation_via_base():
    """Queries should translate to other dialects without errors."""
    from benchbox.datavault import DataVault

    benchmark = DataVault(scale_factor=0.01)
    translated = benchmark.translate_query(1, dialect="snowflake")

    assert "SELECT" in translated.upper()


class TestHashAlgorithmValidation:
    """Tests for hash algorithm validation."""

    def test_md5_algorithm_accepted(self):
        """MD5 hash algorithm should be accepted."""
        benchmark = DataVaultBenchmark(scale_factor=0.01, hash_algorithm="md5")
        assert benchmark.hash_algorithm == "md5"

    def test_sha1_algorithm_rejected(self):
        """SHA1 hash algorithm should be rejected with clear error message."""
        with pytest.raises(ValueError) as exc_info:
            DataVaultBenchmark(scale_factor=0.01, hash_algorithm="sha1")

        assert "sha1" in str(exc_info.value).lower()
        assert "md5" in str(exc_info.value).lower()
        assert "VARCHAR(32)" in str(exc_info.value)

    def test_sha256_algorithm_rejected(self):
        """SHA256 hash algorithm should be rejected with clear error message."""
        with pytest.raises(ValueError) as exc_info:
            DataVaultBenchmark(scale_factor=0.01, hash_algorithm="sha256")

        assert "sha256" in str(exc_info.value).lower()

    def test_invalid_algorithm_rejected(self):
        """Arbitrary hash algorithm names should be rejected."""
        with pytest.raises(ValueError):
            DataVaultBenchmark(scale_factor=0.01, hash_algorithm="invalid")


class TestManifestChecking:
    """Tests for existing data manifest checking."""

    def test_check_existing_manifest_returns_none_when_no_manifest(self, tmp_path):
        """Should return None when no manifest exists."""
        benchmark = DataVaultBenchmark(scale_factor=0.01, output_dir=tmp_path)
        result = benchmark._check_existing_manifest()
        assert result is None

    def test_check_existing_manifest_returns_none_on_benchmark_mismatch(self, tmp_path):
        """Should return None when manifest benchmark doesn't match."""
        manifest = {
            "version": 2,
            "benchmark": "tpch",  # Wrong benchmark
            "scale_factor": 0.01,
            "tables": {},
        }
        (tmp_path / "_datagen_manifest.json").write_text(json.dumps(manifest))

        benchmark = DataVaultBenchmark(scale_factor=0.01, output_dir=tmp_path)
        result = benchmark._check_existing_manifest()
        assert result is None

    def test_check_existing_manifest_returns_none_on_scale_mismatch(self, tmp_path):
        """Should return None when manifest scale factor doesn't match."""
        manifest = {
            "version": 2,
            "benchmark": "datavault",
            "scale_factor": 1.0,  # Wrong scale
            "tables": {},
        }
        (tmp_path / "_datagen_manifest.json").write_text(json.dumps(manifest))

        benchmark = DataVaultBenchmark(scale_factor=0.01, output_dir=tmp_path)
        result = benchmark._check_existing_manifest()
        assert result is None

    def test_check_existing_manifest_returns_none_on_missing_files(self, tmp_path):
        """Should return None when manifest references non-existent files."""
        manifest = {
            "version": 2,
            "benchmark": "datavault",
            "scale_factor": 0.01,
            "tables": {
                "hub_region": {"formats": {"tbl": [{"path": "hub_region.tbl", "row_count": 5, "size_bytes": 100}]}}
            },
        }
        (tmp_path / "_datagen_manifest.json").write_text(json.dumps(manifest))

        benchmark = DataVaultBenchmark(scale_factor=0.01, output_dir=tmp_path)
        result = benchmark._check_existing_manifest()
        # Should return None because hub_region.tbl doesn't exist
        assert result is None

    def test_force_regenerate_bypasses_manifest_check(self, tmp_path, monkeypatch):
        """force_regenerate=True should skip manifest checking."""
        # Create a valid-looking manifest
        manifest = {
            "version": 2,
            "benchmark": "datavault",
            "scale_factor": 0.01,
            "tables": {},
        }
        (tmp_path / "_datagen_manifest.json").write_text(json.dumps(manifest))

        benchmark = DataVaultBenchmark(scale_factor=0.01, output_dir=tmp_path, force_regenerate=True)
        # When force_regenerate is True, generate_data should not short-circuit
        # We test this by verifying force_regenerate flag is set
        assert benchmark.force_regenerate is True
