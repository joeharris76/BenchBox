"""Generic lazy-loading registry utilities for BenchBox imports.

This module centralizes the lazy import logic used by :mod:`benchbox.__init__`
and provides structured diagnostics when modules fail to load. Consolidating
this behaviour keeps the public package namespace declarative while ensuring
tests and contributors have a single place to manipulate cache state.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from importlib import import_module
from typing import Callable, Optional

ExceptionFactory = Callable[[str, "LazyImportSpec", Optional[Exception]], Exception]


@dataclass(frozen=True)
class LazyImportSpec:
    """Descriptor for a lazily imported attribute."""

    module: str
    attribute: str
    optional_dependencies: tuple[str, ...] = ()
    store_errors: bool = True
    eager: bool = False


class LazyLoader:
    """Manage the lazy import lifecycle for a namespace."""

    def __init__(
        self,
        namespace: str,
        exception_factory: ExceptionFactory,
        logger: logging.Logger | None = None,
        debug: bool = False,
    ) -> None:
        self.namespace = namespace
        self._exception_factory = exception_factory
        self._registry: dict[str, LazyImportSpec] = {}
        self._cache: dict[str, type[object] | ImportError | None] = {}
        self._logger = logger or logging.getLogger(namespace)
        self._debug = debug

    @property
    def cache(self) -> dict[str, type[object] | ImportError | None]:
        """Expose the underlying cache for testing and introspection."""

        return self._cache

    def register(self, name: str, spec: LazyImportSpec) -> None:
        """Register a lazy import specification."""

        self._registry[name] = spec
        if spec.eager:
            self.resolve(name, suppress_exceptions=True)

    def register_many(self, mapping: dict[str, LazyImportSpec]) -> None:
        """Register multiple lazy import specifications."""

        for name, spec in mapping.items():
            self.register(name, spec)

    def resolve(
        self,
        name: str,
        suppress_exceptions: bool = False,
    ) -> tuple[type[object] | None, ImportError | None]:
        """Resolve a lazy import, caching the result."""

        if name not in self._registry:
            raise AttributeError(f"Unknown lazy import '{name}' for {self.namespace}")

        cached = self._cache.get(name)
        if isinstance(cached, ImportError):
            return None, cached
        if cached is not None:
            return cached, None

        spec = self._registry[name]
        module_path = f"{self.namespace}.{spec.module}"

        try:
            module = import_module(module_path)
            attribute = getattr(module, spec.attribute)
            self._cache[name] = attribute
            if self._debug:
                self._logger.debug("Lazy-loaded %s.%s", module_path, spec.attribute)
            return attribute, None
        except ImportError as exc:
            if spec.store_errors:
                self._cache[name] = exc
            else:
                self._cache[name] = None

            if self._debug:
                self._logger.debug("Failed lazy-load for %s.%s: %s", module_path, spec.attribute, exc)

            if suppress_exceptions:
                return None, exc

            raise self._exception_factory(name, spec, exc) from exc

    def get(self, name: str) -> type[object]:
        """Return the lazily imported attribute or raise enhanced errors."""

        result, error = self.resolve(name)
        if result is not None:
            return result

        spec = self._registry[name]
        raise self._exception_factory(name, spec, error)

    def clear(self) -> None:
        """Clear the cached import results (primarily for tests)."""

        self._cache.clear()

    def preload(self, *names: str) -> None:
        """Eagerly load specific entries, ignoring failures."""

        for name in names:
            self.resolve(name, suppress_exceptions=True)

    def registry_items(self) -> dict[str, LazyImportSpec]:
        """Expose the registered specifications."""

        return dict(self._registry)
