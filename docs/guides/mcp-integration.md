<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# MCP Integration Guide

```{tags} advanced, guide
```

This guide explains how to integrate BenchBox with AI assistants using the Model Context Protocol (MCP). Supported clients include Claude Code, ChatGPT, OpenCode, and other MCP-compatible tools.

## Overview

BenchBox provides an MCP server that exposes benchmarking capabilities to AI assistants. This enables:

- **Interactive benchmarking**: Run benchmarks through natural language
- **Result analysis**: AI-assisted performance analysis and recommendations
- **Configuration validation**: Verify settings before running benchmarks
- **Platform discovery**: Explore available platforms and benchmarks

## Prerequisites

- BenchBox installed with the `mcp` extra
- An MCP-compatible AI assistant (Claude Code, ChatGPT, OpenCode, etc.)

Install the MCP dependencies:

```bash
uv sync --extra mcp
```

## Quick Test

Before configuring your AI agent, verify the MCP server works:

```bash
# Start the server (it will wait for input)
uv run python -m benchbox.mcp
```

The server should start without errors. Press `Ctrl+C` to stop.

For interactive testing, use the MCP Inspector:

```bash
npx @anthropic-ai/inspector "uv run python -m benchbox.mcp"
```

This opens a web UI where you can browse and test all BenchBox tools.

## Agent Setup

Choose your AI assistant below for setup instructions.

### Claude Code

#### Quick Setup

Add BenchBox as an MCP server using the Claude Code CLI:

```bash
# Project-scoped (shared with team via .mcp.json)
claude mcp add --transport stdio benchbox --scope project \
  -- uv run python -m benchbox.mcp

# User-scoped (available in all your projects)
claude mcp add --transport stdio benchbox --scope user \
  -- uv run python -m benchbox.mcp
```

#### Manual Configuration

Create or edit `.mcp.json` in your project root:

```json
{
  "mcpServers": {
    "benchbox": {
      "type": "stdio",
      "command": "uv",
      "args": ["run", "python", "-m", "benchbox.mcp"]
    }
  }
}
```

#### Verifying Installation

```bash
# List configured servers
claude mcp list

# Check server status in Claude Code
/mcp
```

---

### ChatGPT / Codex

OpenAI's ChatGPT desktop app supports MCP servers for tool integration.

#### Configuration

1. Open ChatGPT desktop settings
2. Navigate to **Features** > **MCP Servers**
3. Click **Add Server** and configure:

```json
{
  "name": "benchbox",
  "transport": "stdio",
  "command": "uv",
  "args": ["run", "python", "-m", "benchbox.mcp"],
  "cwd": "/path/to/your/project"
}
```

#### Environment Setup

Ensure BenchBox is installed in the project directory:

```bash
cd /path/to/your/project
uv sync --extra mcp
```

#### Verifying Installation

Ask ChatGPT: "What MCP tools are available?" - it should list BenchBox tools including `list_platforms`, `run_benchmark`, etc.

---

### OpenCode

[OpenCode](https://github.com/opencode-ai/opencode) is an open-source AI coding assistant that supports MCP.

#### Configuration

Create or edit `~/.opencode/config.json`:

```json
{
  "mcp": {
    "servers": {
      "benchbox": {
        "command": "uv",
        "args": ["run", "python", "-m", "benchbox.mcp"],
        "cwd": "/path/to/benchbox/project"
      }
    }
  }
}
```

#### Per-Project Configuration

Alternatively, create `.opencode.json` in your project root:

```json
{
  "mcp": {
    "servers": {
      "benchbox": {
        "command": "uv",
        "args": ["run", "python", "-m", "benchbox.mcp"]
      }
    }
  }
}
```

#### Verifying Installation

```bash
opencode --list-mcp-servers
```

---

### Other MCP Clients

For other MCP-compatible clients, use the standard stdio transport:

**Command:** `uv run python -m benchbox.mcp`

**Transport:** stdio (JSON-RPC over stdin/stdout)

Most clients support a configuration like:

```json
{
  "command": "uv",
  "args": ["run", "python", "-m", "benchbox.mcp"],
  "transport": "stdio"
}
```

Consult your client's documentation for the specific configuration format.

## Using BenchBox Tools

Once configured, Claude Code can use BenchBox tools through natural language. Example interactions:

### Discovering Platforms

> "What database platforms are available in BenchBox?"

Claude will use `list_platforms()` to show available platforms with their capabilities.

### Running Benchmarks

> "Run TPC-H at scale factor 0.1 on DuckDB"

Claude will use `run_benchmark(platform="duckdb", benchmark="tpch", scale_factor=0.1)`.

### Validating Configuration

> "Can I run TPC-DS on Polars with scale factor 10?"

Claude will use `validate_config()` to check if the configuration is valid.

### Analyzing Results

> "Show me the slowest queries from my last benchmark run"

Claude will use `list_recent_runs()` and `get_results()` to analyze performance.

## Using MCP Prompts

BenchBox provides reusable prompt templates for common analysis tasks. These are invoked as slash commands:

```
/mcp__benchbox__analyze_results tpch duckdb
/mcp__benchbox__compare_platforms tpch "duckdb,polars-df" 0.1
/mcp__benchbox__identify_regressions
/mcp__benchbox__benchmark_planning testing
```

### Prompt Arguments

MCP prompts use **positional arguments**, not named parameters:

```
# Correct - positional arguments separated by spaces
/mcp__benchbox__compare_platforms tpch "duckdb,polars" 1

# Incorrect - named parameters not supported
/mcp__benchbox__compare_platforms benchmark=tpch scale_factor=1
```

### Available Prompts

| Prompt | Arguments | Description |
|--------|-----------|-------------|
| `analyze_results` | benchmark, platform, focus | Analyze benchmark results |
| `compare_platforms` | benchmark, platforms, scale_factor | Compare performance across platforms |
| `identify_regressions` | baseline_run, comparison_run, threshold_percent | Find performance regressions |
| `benchmark_planning` | use_case, platforms, time_budget_minutes | Plan benchmark strategy |
| `troubleshoot_failure` | error_message, platform, benchmark | Diagnose benchmark failures |

## Configuration Scopes

MCP server configuration can be stored at different scopes depending on your client:

### Claude Code Scopes

| Scope | Location | Shared | Use Case |
|-------|----------|--------|----------|
| Local | `.claude.json` in project | No | Personal development |
| Project | `.mcp.json` at project root | Yes (via git) | Team-shared configuration |
| User | `~/.claude.json` | No | Cross-project personal tools |

Use `--scope` flag when adding:

```bash
claude mcp add --transport stdio benchbox --scope project -- uv run python -m benchbox.mcp
```

### Other Clients

Most clients support:
- **Project-level**: Configuration file in project root (e.g., `.opencode.json`)
- **User-level**: Configuration in home directory (e.g., `~/.opencode/config.json`)

## Troubleshooting

### Server Not Starting

1. Verify MCP dependencies are installed:
   ```bash
   uv sync --extra mcp
   ```

2. Test the server directly:
   ```bash
   uv run python -m benchbox.mcp
   ```
   The server should start and wait for JSON-RPC input. Press Ctrl+C to exit.

3. Check for import errors in the output.

4. Verify working directory - the server must run from a directory where BenchBox is installed.

### Tool Calls Failing

1. Check your client's MCP server status (e.g., `/mcp` in Claude Code)
2. Verify the tool exists by asking "What BenchBox tools are available?"
3. Check argument types match expected types (e.g., `scale_factor` is a number, not a string)

### Permission Issues (Claude Code)

Project-scoped servers require approval on first use. If prompted, approve the server or reset choices:

```bash
claude mcp reset-project-choices
```

### Connection Issues

If the client can't connect to the server:

1. Ensure `uv` is in your PATH
2. Try using absolute paths in configuration:
   ```json
   {
     "command": "/path/to/uv",
     "args": ["run", "python", "-m", "benchbox.mcp"],
     "cwd": "/path/to/benchbox/project"
   }
   ```
3. Check client logs for connection errors

## Related Documentation

- [MCP Reference](../reference/mcp.md) - Complete tool and prompt reference
- [CLI Reference](../reference/cli/index.md) - Command-line interface documentation
- [API Reference](../reference/api-reference.md) - Python API documentation
