"""Tests for benchbox.utils.lazy_loader module."""

import pytest

from benchbox.utils.lazy_loader import LazyImportSpec, LazyLoader

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


def _make_exception(name, spec, original):
    """Test exception factory."""
    msg = f"Cannot import '{name}' from {spec.module}.{spec.attribute}"
    if spec.optional_dependencies:
        msg += f" (install: {', '.join(spec.optional_dependencies)})"
    return ImportError(msg)


class TestLazyImportSpec:
    def test_defaults(self):
        spec = LazyImportSpec(module="core.tpch", attribute="TPCHBenchmark")
        assert spec.module == "core.tpch"
        assert spec.attribute == "TPCHBenchmark"
        assert spec.optional_dependencies == ()
        assert spec.store_errors is True
        assert spec.eager is False

    def test_frozen(self):
        spec = LazyImportSpec(module="core.tpch", attribute="TPCHBenchmark")
        with pytest.raises(AttributeError):
            spec.module = "changed"

    def test_optional_dependencies(self):
        spec = LazyImportSpec(
            module="platforms.snowflake",
            attribute="SnowflakeAdapter",
            optional_dependencies=("snowflake-connector-python",),
        )
        assert spec.optional_dependencies == ("snowflake-connector-python",)


class TestLazyLoader:
    def setup_method(self):
        self.loader = LazyLoader("benchbox", _make_exception)

    def test_register_and_resolve(self):
        spec = LazyImportSpec(module="utils.formatting", attribute="format_duration")
        self.loader.register("format_duration", spec)

        result, error = self.loader.resolve("format_duration")
        assert result is not None
        assert error is None
        assert callable(result)

    def test_resolve_caches_result(self):
        spec = LazyImportSpec(module="utils.formatting", attribute="format_duration")
        self.loader.register("format_duration", spec)

        result1, _ = self.loader.resolve("format_duration")
        result2, _ = self.loader.resolve("format_duration")
        assert result1 is result2

    def test_resolve_unknown_raises(self):
        with pytest.raises(AttributeError, match="Unknown lazy import"):
            self.loader.resolve("nonexistent")

    def test_resolve_bad_module_raises(self):
        spec = LazyImportSpec(module="does_not_exist", attribute="Foo")
        self.loader.register("bad", spec)
        with pytest.raises(ImportError, match="Cannot import"):
            self.loader.resolve("bad")

    def test_resolve_bad_module_suppress(self):
        spec = LazyImportSpec(module="does_not_exist", attribute="Foo")
        self.loader.register("bad", spec)
        result, error = self.loader.resolve("bad", suppress_exceptions=True)
        assert result is None
        assert isinstance(error, ImportError)

    def test_cached_error_returns_error(self):
        spec = LazyImportSpec(module="does_not_exist", attribute="Foo", store_errors=True)
        self.loader.register("bad", spec)
        self.loader.resolve("bad", suppress_exceptions=True)

        result, error = self.loader.resolve("bad")
        assert result is None
        assert isinstance(error, ImportError)

    def test_store_errors_false(self):
        spec = LazyImportSpec(module="does_not_exist", attribute="Foo", store_errors=False)
        self.loader.register("no_store", spec)
        self.loader.resolve("no_store", suppress_exceptions=True)
        assert self.loader.cache.get("no_store") is None

    def test_get_success(self):
        spec = LazyImportSpec(module="utils.formatting", attribute="format_duration")
        self.loader.register("format_duration", spec)

        result = self.loader.get("format_duration")
        assert callable(result)

    def test_get_failure_raises(self):
        spec = LazyImportSpec(module="does_not_exist", attribute="Foo")
        self.loader.register("bad", spec)
        with pytest.raises(ImportError, match="Cannot import"):
            self.loader.get("bad")

    def test_clear_cache(self):
        spec = LazyImportSpec(module="utils.formatting", attribute="format_duration")
        self.loader.register("format_duration", spec)
        self.loader.resolve("format_duration")
        assert "format_duration" in self.loader.cache

        self.loader.clear()
        assert "format_duration" not in self.loader.cache

    def test_register_many(self):
        specs = {
            "a": LazyImportSpec(module="utils.formatting", attribute="format_duration"),
            "b": LazyImportSpec(module="utils.formatting", attribute="format_duration"),
        }
        self.loader.register_many(specs)
        assert "a" in self.loader.registry_items()
        assert "b" in self.loader.registry_items()

    def test_preload(self):
        spec_good = LazyImportSpec(module="utils.formatting", attribute="format_duration")
        spec_bad = LazyImportSpec(module="does_not_exist", attribute="Foo")
        self.loader.register("good", spec_good)
        self.loader.register("bad", spec_bad)

        self.loader.preload("good", "bad")
        assert "good" in self.loader.cache
        assert "bad" in self.loader.cache

    def test_eager_registration(self):
        spec = LazyImportSpec(module="utils.formatting", attribute="format_duration", eager=True)
        self.loader.register("eager_item", spec)
        assert "eager_item" in self.loader.cache

    def test_registry_items(self):
        spec = LazyImportSpec(module="utils.formatting", attribute="format_duration")
        self.loader.register("item", spec)
        items = self.loader.registry_items()
        assert "item" in items
        assert items["item"] is spec

    def test_debug_logging(self, caplog):
        import logging

        loader = LazyLoader("benchbox", _make_exception, debug=True)
        loader._logger.setLevel(logging.DEBUG)

        spec = LazyImportSpec(module="utils.formatting", attribute="format_duration")
        loader.register("fmt", spec)

        with caplog.at_level(logging.DEBUG, logger="benchbox"):
            loader.resolve("fmt")

        assert any("Lazy-loaded" in r.message for r in caplog.records)

    def test_debug_logging_failure(self, caplog):
        import logging

        loader = LazyLoader("benchbox", _make_exception, debug=True)
        loader._logger.setLevel(logging.DEBUG)

        spec = LazyImportSpec(module="does_not_exist", attribute="Foo")
        loader.register("bad", spec)

        with caplog.at_level(logging.DEBUG, logger="benchbox"):
            loader.resolve("bad", suppress_exceptions=True)

        assert any("Failed lazy-load" in r.message for r in caplog.records)

    def test_cache_property(self):
        assert self.loader.cache is self.loader._cache
