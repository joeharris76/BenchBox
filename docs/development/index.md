<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# Developer Documentation

```{tags} contributor
```

Documentation for contributors and developers working on BenchBox.

## Getting Started with Development

- [Development Guide](development.md) - Setting up development environment and workflow
- [Testing](testing.md) - Testing strategies, running tests, and test organization

## Contributing

- [Adding New Platforms](adding-new-platforms.md) - How to add support for new database platforms
- [Import Patterns](import-patterns.md) - Lazy loading and dependency management patterns
- [TPC Compilation Guide](tpc-compilation-guide.md) - Compiling TPC benchmark tools

## Architecture & Design

- [Architecture Overview](../design/architecture.md) - High-level system architecture
- [Code Structure](../design/structure.md) - Codebase organization
- [DB API 2.0](db-api-2.md) - Python DB API 2.0 specification and platform integration
- [Runtime Modules](runtime-modules.md) - Runtime module organization and responsibilities
- [Data Sharing](data-sharing.md) - Data sharing between benchmark phases
- [Dependency Compatibility](dependency-compatibility.md) - Managing optional dependencies

## Testing

- [Testing Guide](testing.md) - Testing strategies and test organization
- [Testing Index](../testing/index.md) - Test documentation overview
- [Live Integration Tests](../testing/live-integration-tests.md) - Running tests against live databases

## Reference

- [Read Primitives Catalog](read-primitives-catalog.md) - Catalog of primitive read operations

## Related Documentation

- [Concepts](../concepts/index.md) - Core concepts and glossary

```{toctree}
:maxdepth: 1
:caption: Development Guides
:hidden:

getting-started
development
platform-development
adding-new-platforms
adding-dataframe-platform
architecture-design
data-dependencies
import-patterns
tpc-compilation-guide
testing
read-primitives-catalog
runtime-modules
data-sharing
dependency-compatibility
db-api-2
test-quality-guidelines
../platform-config-audit
```
