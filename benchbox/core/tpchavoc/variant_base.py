"""Shared utilities for TPC-Havoc variant generation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class VariantGenerator(ABC):
    """Abstract base class for query variant generators."""

    def __init__(self, variant_id: int, description: str) -> None:
        self.variant_id = variant_id
        self.description = description

    @abstractmethod
    def generate(self, base_query: str, params: dict[str, Any] | None = None) -> str:
        """Generate variant SQL from the base query."""

    def get_description(self) -> str:
        """Return a human-readable description of the variant."""
        return self.description


class StaticSQLVariant(VariantGenerator):
    """Simple variant generator that returns a static SQL string."""

    def __init__(self, variant_id: int, description: str, sql: str) -> None:
        super().__init__(variant_id, description)
        self._sql = sql

    def generate(self, base_query: str, params: dict[str, Any] | None = None) -> str:  # noqa: ARG002
        return self._sql


__all__ = ["VariantGenerator", "StaticSQLVariant"]
