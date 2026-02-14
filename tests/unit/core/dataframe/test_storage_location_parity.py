"""Tests for DataFrame/SQL storage location parity.

Asserts that DataFrame and SQL default data directories share the same
``benchmark_runs/`` root, eliminating the historical asymmetry where SQL data
lived project-locally but DataFrame cache lived under ``~/.benchbox/``.

Copyright 2026 Joe Harris / BenchBox Project
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from benchbox.core.dataframe.data_loader import DEFAULT_CACHE_DIR, DataCache
from benchbox.utils.path_utils import get_benchmark_runs_dataframe_path, get_benchmark_runs_datagen_path

pytestmark = pytest.mark.fast


class TestStorageLocationParity:
    """Verify DataFrame and SQL defaults share benchmark_runs/ root."""

    def test_default_cache_dir_suffix_is_benchmark_runs_dataframe_data(self):
        """DEFAULT_CACHE_DIR should represent the benchmark_runs/datagen suffix."""
        assert Path("benchmark_runs") / "datagen" == DEFAULT_CACHE_DIR

    def test_dataframe_and_sql_share_benchmark_runs_root(self):
        """DataFrame and SQL datagen paths share the same benchmark_runs/ parent."""
        df_path = get_benchmark_runs_dataframe_path()
        sql_path = get_benchmark_runs_datagen_path("tpch", 1.0)

        # Both should be under benchmark_runs/
        assert df_path.parent.name == "benchmark_runs"
        assert sql_path.parent.parent.name == "benchmark_runs"

        # Both should share the same benchmark_runs/ root
        df_root = df_path.parent
        sql_root = sql_path.parent.parent
        assert df_root == sql_root

    def test_datacache_default_resolves_to_benchmark_runs(self):
        """DataCache with no arguments should use benchmark_runs/datagen/."""
        with patch.dict("os.environ", {}, clear=True):
            # Remove BENCHBOX_CACHE_DIR if set
            import os

            env = os.environ.copy()
            env.pop("BENCHBOX_CACHE_DIR", None)
            with patch.dict("os.environ", env, clear=True):
                cache = DataCache()
                assert cache.cache_dir == Path.cwd() / "benchmark_runs" / "datagen"

    def test_benchbox_cache_dir_env_overrides_default(self):
        """BENCHBOX_CACHE_DIR environment variable should override the default."""
        with patch.dict("os.environ", {"BENCHBOX_CACHE_DIR": "/custom/cache"}):
            cache = DataCache()
            assert str(cache.cache_dir) == "/custom/cache"

    def test_explicit_cache_dir_takes_precedence(self):
        """Explicit cache_dir parameter should take precedence over everything."""
        explicit = Path("/explicit/path")
        with patch.dict("os.environ", {"BENCHBOX_CACHE_DIR": "/env/path"}):
            cache = DataCache(cache_dir=explicit)
            assert cache.cache_dir == explicit

    def test_default_not_in_home_directory(self):
        """Default DataFrame cache should NOT be under home directory."""
        cache = DataCache()
        assert not str(cache.cache_dir).startswith(str(Path.home() / ".benchbox"))

    def test_path_helper_default(self):
        """get_benchmark_runs_dataframe_path() returns benchmark_runs/datagen/."""
        path = get_benchmark_runs_dataframe_path()
        assert path == Path.cwd() / "benchmark_runs" / "datagen"

    def test_path_helper_override(self):
        """get_benchmark_runs_dataframe_path() respects base_dir override."""
        path = get_benchmark_runs_dataframe_path("/custom/dir")
        assert path == Path("/custom/dir")
