"""Tests for benchmark mixin contracts and MRO safety.

Verifies that CursorValidationQueryExecutionMixin enforces its method
contract at class definition time and that required methods resolve to
PlatformAdapter (not stubs) in the standard inheritance chain.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import pytest

from benchbox.core.benchmark_mixins import CursorValidationQueryExecutionMixin

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


class _FakeParent:
    """Minimal stand-in providing the methods CursorValidationQueryExecutionMixin requires."""

    def log_verbose(self, msg: str) -> None:
        pass

    def log_very_verbose(self, msg: str) -> None:
        pass

    def _build_query_result_with_validation(self, **kwargs: Any) -> dict[str, Any]:
        return {"status": "SUCCESS", **kwargs}


# ---------------------------------------------------------------------------
# MRO resolution tests
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestCursorValidationMixinMRO:
    """Guard against MRO-shadowing regressions (see commit f2ca27e5)."""

    def test_required_methods_resolve_to_parent_not_mixin(self):
        """Verify log_verbose and friends resolve past the mixin to the parent."""

        class ConcreteAdapter(CursorValidationQueryExecutionMixin, _FakeParent):
            pass

        adapter = ConcreteAdapter()
        # These must NOT raise NotImplementedError — they should reach _FakeParent.
        adapter.log_verbose("test")
        adapter.log_very_verbose("test")
        result = adapter._build_query_result_with_validation(query_id="q1")
        assert result["status"] == "SUCCESS"

    def test_mixin_does_not_define_required_methods(self):
        """Ensure the mixin itself has no stub implementations that could shadow."""
        mixin_own_methods = set(CursorValidationQueryExecutionMixin.__dict__.keys())
        for method_name in CursorValidationQueryExecutionMixin._REQUIRED_METHODS:
            assert method_name not in mixin_own_methods, (
                f"{method_name} must NOT be defined on CursorValidationQueryExecutionMixin "
                f"— it would shadow the real implementation via MRO"
            )

    def test_real_adapters_resolve_methods_correctly(self):
        """Spot-check that Trino/Firebolt/Presto resolve methods to PlatformAdapter."""
        from benchbox.platforms.base.adapter import PlatformAdapter

        for adapter_name in ("trino", "firebolt", "presto"):
            module = __import__(f"benchbox.platforms.{adapter_name}", fromlist=["_"])
            adapter_cls = next(v for k, v in vars(module).items() if k.endswith("Adapter") and k[0].isupper())
            mro = adapter_cls.__mro__
            for method_name in CursorValidationQueryExecutionMixin._REQUIRED_METHODS:
                # Find the first class in MRO that defines the method
                provider = next(c for c in mro if method_name in c.__dict__)
                assert provider is not CursorValidationQueryExecutionMixin, (
                    f"{adapter_cls.__name__}.{method_name} resolves to the mixin — "
                    f"expected PlatformAdapter or a concrete parent"
                )


# ---------------------------------------------------------------------------
# __init_subclass__ enforcement tests
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestCursorValidationMixinSubclassEnforcement:
    """Verify __init_subclass__ catches missing method providers."""

    def test_subclass_without_provider_raises_type_error(self):
        """A concrete class using the mixin without a parent that provides methods must fail."""
        with pytest.raises(TypeError, match="does not inherit a concrete implementation"):

            class BrokenAdapter(CursorValidationQueryExecutionMixin):
                pass

    def test_intermediate_mixin_skips_enforcement(self):
        """Classes with 'Mixin' or 'Base' suffix are not checked (they're not concrete)."""

        class IntermediateMixin(CursorValidationQueryExecutionMixin):
            pass  # Should NOT raise

    def test_valid_subclass_passes_enforcement(self):
        """A concrete adapter with a proper parent passes the check."""

        class ValidAdapter(CursorValidationQueryExecutionMixin, _FakeParent):
            pass  # Should NOT raise


# ---------------------------------------------------------------------------
# execute_query integration test
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestCursorValidationExecuteQuery:
    """Test execute_query flow through the mixin."""

    def test_execute_query_returns_success_with_valid_parent(self):
        """End-to-end: mixin execute_query should succeed when parent provides methods."""

        class TestAdapter(CursorValidationQueryExecutionMixin, _FakeParent):
            pass

        adapter = TestAdapter()
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1, "row")]

        result = adapter.execute_query(mock_conn, "SELECT 1", "q1")
        assert result["status"] == "SUCCESS"
        assert result["query_id"] == "q1"
