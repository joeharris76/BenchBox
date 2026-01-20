<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# Concepts

```{tags} concept
```

Conceptual documentation explaining BenchBox architecture, terminology, and design principles.

## Core Concepts

- [Architecture](architecture.md) - System architecture and component overview
- [Workflow](workflow.md) - Benchmark execution workflow and lifecycle
- [Data Model](data-model.md) - Data structures and relationships
- [Glossary](glossary.md) - Terms and definitions

## Comparisons

- [Benchmarking Tools Compared](benchmarking-tools-compared.md) - BenchBox vs HammerDB, BenchBase, and LakeBench

## Understanding BenchBox

BenchBox is designed around a few key concepts:

### Benchmarks
Standard workloads that test database performance across different dimensions (analytics, transactions, data loading).

### Platforms
Database adapters that execute benchmarks on specific database systems (DuckDB, Databricks, Snowflake, etc.).

### Execution Modes
Different ways to run benchmarks:
- **Power Test**: Sequential query execution measuring single-user performance
- **Throughput Test**: Concurrent query streams measuring multi-user scalability
- **Data Generation**: Creating benchmark datasets at various scale factors

### Results & Validation
Structured output capturing execution metrics, query timings, and optional validation against expected results.

## Related Documentation

- [Getting Started](../usage/getting-started.md) - Start using BenchBox
- [Design Documents](../design/index.md) - Detailed design specifications
- [Development Guide](../development/index.md) - Contributing to BenchBox

```{toctree}
:maxdepth: 1
:caption: Concept Guides
:hidden:

architecture
workflow
data-model
glossary
benchmarking-tools-compared
```
