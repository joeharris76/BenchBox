<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# MCP Integration Guide

```{tags} advanced, guide
```

This guide explains how to integrate BenchBox with AI assistants like Claude Code using the Model Context Protocol (MCP).

## Overview

BenchBox provides an MCP server that exposes benchmarking capabilities to AI assistants. This enables:

- **Interactive benchmarking**: Run benchmarks through natural language
- **Result analysis**: AI-assisted performance analysis and recommendations
- **Configuration validation**: Verify settings before running benchmarks
- **Platform discovery**: Explore available platforms and benchmarks

## Prerequisites

- BenchBox installed with the `mcp` extra
- Claude Code or another MCP-compatible AI assistant

Install the MCP dependencies:

```bash
uv sync --extra mcp
```

## Configuring Claude Code

### Quick Setup

Add BenchBox as an MCP server using the Claude Code CLI:

```bash
# Project-scoped (shared with team via .mcp.json)
claude mcp add --transport stdio benchbox --scope project \
  -- uv run python -m benchbox.mcp

# User-scoped (available in all your projects)
claude mcp add --transport stdio benchbox --scope user \
  -- uv run python -m benchbox.mcp
```

### Manual Configuration

Alternatively, create or edit `.mcp.json` in your project root:

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

### Verifying Installation

After configuration, verify the server is available:

```bash
# List configured servers
claude mcp list

# Check server status in Claude Code
/mcp
```

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

MCP server configuration can be stored at different scopes:

| Scope | Location | Shared | Use Case |
|-------|----------|--------|----------|
| Local | `.claude.json` in project | No | Personal development |
| Project | `.mcp.json` at project root | Yes (via git) | Team-shared configuration |
| User | `~/.claude.json` | No | Cross-project personal tools |

Use `--scope` flag when adding:

```bash
claude mcp add --transport stdio benchbox --scope project -- uv run python -m benchbox.mcp
```

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

3. Check for import errors in the output.

### Tool Calls Failing

1. Use `/mcp` in Claude Code to check server status
2. Verify the tool exists with `list_platforms()` or similar
3. Check argument types match expected types (e.g., `scale_factor` is a number)

### Permission Issues

Project-scoped servers require approval on first use. If prompted, approve the server or reset choices:

```bash
claude mcp reset-project-choices
```

## Related Documentation

- [MCP Reference](../reference/mcp.md) - Complete tool and prompt reference
- [CLI Reference](../reference/cli/index.md) - Command-line interface documentation
- [API Reference](../reference/api-reference.md) - Python API documentation
