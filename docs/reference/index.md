<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# Reference Documentation

```{tags} reference
```

Complete reference documentation for BenchBox APIs, CLI, and data formats.

## Command Line Interface

- [CLI Reference](cli/index.md) - Complete command-line interface documentation
  - [run](cli/run.md) - Primary benchmark execution command
  - [convert](cli/convert.md) - Convert data to Parquet, Delta Lake, or Iceberg
  - [shell](cli/shell.md) - Interactive SQL shell
  - [platforms](cli/platforms.md) - Platform management
  - [results](cli/results.md) - Export, view, and compare results
  - [tuning](cli/tuning.md) - Tuning configuration commands
  - [utilities](cli/utilities.md) - Utility commands

## Python API

- [API Reference](api-reference.md) - Python API overview
- [Python API Documentation](python-api/index.rst) - Detailed API reference (Sphinx)

## Integrations

- [MCP Server Reference](mcp.md) - Model Context Protocol server for AI assistants

## Data Formats & Schemas

- [Result Export Formats](result-formats.md) - Comprehensive guide to JSON, CSV, HTML exports
- [Result Schema v1.0](result-schema-v1.md) - Benchmark result JSON schema specification

## Related Documentation

- [Usage Guides](../usage/index.md) - Practical usage examples
- [How-To Guides](../guides/index.md) - Task-oriented guides
- [Platform Documentation](../platforms/index.md) - Platform-specific reference

```{toctree}
:maxdepth: 2
:caption: Reference Documentation
:hidden:

cli/index
cli
api-reference
mcp
result-formats
result-schema-v1
python-api/index
python-api/platforms
```
