from __future__ import annotations

import runpy
from unittest.mock import MagicMock

import pytest

import benchbox.mcp

pytestmark = pytest.mark.fast


def test_module_main_invokes_run_server(monkeypatch):
    run_server = MagicMock()
    monkeypatch.setattr(benchbox.mcp, "run_server", run_server)

    runpy.run_module("benchbox.mcp.__main__", run_name="__main__")

    run_server.assert_called_once_with()
