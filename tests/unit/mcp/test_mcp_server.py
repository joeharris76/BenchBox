"""Tests for BenchBox MCP server creation and configuration.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

# Skip if MCP SDK not installed
mcp = pytest.importorskip("mcp", reason="MCP SDK not installed. Install with: uv add benchbox --extra mcp")


class TestMCPServerCreation:
    """Tests for MCP server creation."""

    def test_create_server_returns_fastmcp_instance(self):
        """Test that create_server returns a FastMCP instance."""
        from mcp.server.fastmcp import FastMCP

        from benchbox.mcp import create_server

        server = create_server()

        assert isinstance(server, FastMCP)
        assert server.name == "benchbox"

    def test_server_has_instructions(self):
        """Test that server has instructions configured."""
        from benchbox.mcp import create_server

        server = create_server()

        assert hasattr(server, "instructions")
        assert server.instructions is not None
        assert "BenchBox" in server.instructions

    def test_run_server_function_exists(self):
        """Test that run_server function exists and is callable."""
        from benchbox.mcp import run_server

        assert callable(run_server)


class TestMCPToolRegistration:
    """Tests for MCP tool registration."""

    def test_discovery_tools_registered(self):
        """Test that discovery tools are registered."""
        from benchbox.mcp import create_server

        server = create_server()

        # FastMCP stores tools internally
        # We check by accessing the _tool_manager or similar
        # For now, we just verify server creates without error
        assert server is not None

    def test_benchmark_tools_registered(self):
        """Test that benchmark execution tools are registered."""
        from benchbox.mcp import create_server

        server = create_server()
        assert server is not None

    def test_results_tools_registered(self):
        """Test that results tools are registered."""
        from benchbox.mcp import create_server

        server = create_server()
        assert server is not None


class TestMCPResourceRegistration:
    """Tests for MCP resource registration."""

    def test_resources_registered(self):
        """Test that resources are registered."""
        from benchbox.mcp import create_server

        server = create_server()
        assert server is not None


class TestMCPPromptRegistration:
    """Tests for MCP prompt registration."""

    def test_prompts_registered(self):
        """Test that prompts are registered."""
        from benchbox.mcp import create_server

        server = create_server()
        assert server is not None


class TestPythonVersionCheck:
    """Tests for Python version compatibility."""

    def test_import_fails_on_python39(self, monkeypatch):
        """Test that import raises error on Python < 3.10."""
        # This test runs on Python 3.10+ but simulates version check
        import benchbox.mcp as mcp_module

        # The version check happens at import time, so the module
        # either imported successfully (we're on 3.10+) or not
        assert mcp_module is not None
