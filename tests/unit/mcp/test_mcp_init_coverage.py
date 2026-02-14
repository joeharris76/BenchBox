from __future__ import annotations

import types
from unittest.mock import MagicMock

import pytest

import benchbox.mcp as mcp_module

pytestmark = pytest.mark.fast


def test_create_server_delegates_to_factory(monkeypatch):
    sentinel = object()
    fake_server_module = types.ModuleType("benchbox.mcp.server")
    fake_server_module.create_benchbox_server = lambda: sentinel
    monkeypatch.setitem(__import__("sys").modules, "benchbox.mcp.server", fake_server_module)

    assert mcp_module.create_server() is sentinel


def test_run_server_invokes_run(monkeypatch):
    fake_server = MagicMock()
    monkeypatch.setattr(mcp_module, "create_server", lambda: fake_server)

    mcp_module.run_server()

    fake_server.run.assert_called_once_with()


def test_getattr_lazy_loads_error_observability_and_execution(monkeypatch):
    import sys

    errors_mod = types.ModuleType("benchbox.mcp.errors")
    errors_mod.ErrorCode = "EC"
    errors_mod.ErrorCategory = "CAT"
    errors_mod.MCPError = RuntimeError
    errors_mod.make_error = lambda: "ERR"

    observability_mod = types.ModuleType("benchbox.mcp.observability")
    observability_mod.ToolCallContext = "CTX"
    observability_mod.get_metrics_collector = lambda: "METRICS"

    execution_mod = types.ModuleType("benchbox.mcp.execution")
    execution_mod.ExecutionStatus = "STATUS"
    execution_mod.ExecutionState = "STATE"
    execution_mod.get_execution_tracker = lambda: "TRACKER"

    monkeypatch.setitem(sys.modules, "benchbox.mcp.errors", errors_mod)
    monkeypatch.setitem(sys.modules, "benchbox.mcp.observability", observability_mod)
    monkeypatch.setitem(sys.modules, "benchbox.mcp.execution", execution_mod)

    assert mcp_module.__getattr__("ErrorCode") == "EC"
    assert mcp_module.__getattr__("ErrorCategory") == "CAT"
    assert mcp_module.__getattr__("MCPError") is RuntimeError
    assert mcp_module.__getattr__("make_error")() == "ERR"
    assert mcp_module.__getattr__("ToolCallContext") == "CTX"
    assert mcp_module.__getattr__("get_metrics_collector")() == "METRICS"
    assert mcp_module.__getattr__("ExecutionStatus") == "STATUS"
    assert mcp_module.__getattr__("ExecutionState") == "STATE"
    assert mcp_module.__getattr__("get_execution_tracker")() == "TRACKER"


def test_getattr_raises_for_unknown_name():
    with pytest.raises(AttributeError, match="has no attribute"):
        mcp_module.__getattr__("does_not_exist")
