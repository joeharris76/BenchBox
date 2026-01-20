# BenchBox CLI Reference

```{tags} reference, cli
```

This section provides comprehensive documentation for all BenchBox command-line interface (CLI) commands and options.

## Global Options

These options are available for all BenchBox commands:

- `--version`: Show version information and exit
- `--help`: Show help message and exit

## Commands Overview

BenchBox provides the following commands:

| Command                | Purpose                                            | Documentation                             |
| ---------------------- | -------------------------------------------------- | ----------------------------------------- |
| `run`                  | Execute benchmarks with full configuration control | [run](run.md)                             |
| `convert`              | Convert data to Parquet, Delta Lake, or Iceberg    | [convert](convert.md)                     |
| `shell`                | Launch interactive SQL shell for database          | [shell](shell.md)                         |
| `platforms`            | Manage database platform adapters                  | [platforms](platforms.md)                 |
| `check-deps`           | Check dependency status and installation guidance  | [utilities](utilities.md#check-deps)      |
| `profile`              | Profile system resources                           | [utilities](utilities.md#profile)         |
| `benchmarks`           | Manage benchmark suites                            | [utilities](utilities.md#benchmarks)      |
| `validate`             | Validate configuration files                       | [utilities](utilities.md#validate)        |
| `tuning init`          | Generate sample tuning configurations              | [tuning](tuning.md#tuning-init)           |
| `df-tuning`            | Manage DataFrame platform tuning configurations    | [tuning](tuning.md#df-tuning)             |
| `export`               | Export benchmark results to various formats        | [results](results.md#export)              |
| `results`              | Display exported benchmark results                 | [results](results.md#results)             |
| `compare`              | Compare benchmark results for regression detection | [results](results.md#compare)             |

## Quick Start

```bash
# Run a simple TPC-H benchmark on DuckDB
benchbox run --platform duckdb --benchmark tpch --scale 0.01

# Get help for any command
benchbox run --help
benchbox run --help-topic all       # All options including advanced
benchbox run --help-topic examples  # Categorized usage examples
```

## Documentation Sections

```{toctree}
:maxdepth: 1
:caption: CLI Commands

run
convert
shell
platforms
utilities
tuning
results
configuration
workflows
```
