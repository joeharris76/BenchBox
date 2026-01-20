<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# BenchBox Architecture Design Document

```{tags} contributor, concept
```

## 1. High-Level Architecture

BenchBox is designed as a modular, extensible library for embedding benchmark datasets and queries for database evaluation. The architecture follows these key principles:

- **Modularity**: Clear separation between different components
- **Extensibility**: Easy to add new benchmark types
- **Self-contained**: with minimal external dependencies
- **Cross-Database Compatibility**: Support for multiple database systems

### 1.2 Component Responsibilities

1. **Core Framework**: Provides base interfaces, abstract classes, and common utilities
2. **Benchmarks**: Concrete implementations of specific benchmarks (TPC-H, TPC-DS, etc.)
3. **Data Generator**: Generates benchmark data according to specifications
4. **SQL Manager**: Stores and translates SQL queries for different database dialects

## 2. Core Interfaces and Abstract Classes

### 2.1 BaseBenchmark Abstract Class

```python
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union
import pathlib

class BaseBenchmark(ABC):
    """Base abstract class for all benchmark implementations."""

    def __init__(self, scale_factor: float = 1.0, output_dir: Optional[pathlib.Path] = None):
        """
        Initialize a benchmark instance.

        Args:
            scale_factor: Size of the generated dataset
            output_dir: Directory to store generated data files
        """
        self.scale_factor = scale_factor
        self.output_dir = output_dir or pathlib.Path("./data")
        self._initialize()

    @abstractmethod
    def _initialize(self) -> None:
        """Initialize benchmark-specific components."""
        pass

    @abstractmethod
    def get_schema(self) -> Dict[str, Dict]:
        """
        Get the schema definition for this benchmark.

        Returns:
            Dictionary mapping table names to their schema definitions
        """
        pass

    @abstractmethod
    def generate_data(self) -> Dict[str, pathlib.Path]:
        """
        Generate benchmark data.

        Returns:
            Dictionary mapping table names to generated data file paths
        """
        pass

    @abstractmethod
    def get_query(self, query_id: Union[int, str]) -> str:
        """
        Get a specific query by ID.

        Args:
            query_id: Identifier for the query

        Returns:
            SQL query string
        """
        pass

    @abstractmethod
    def get_queries(self) -> Dict[Union[int, str], str]:
        """
        Get all queries for this benchmark.

        Returns:
            Dictionary mapping query IDs to SQL query strings
        """
        pass

    def translate_query(self, query_id: Union[int, str], dialect: str) -> str:
        """
        Translate a query to a specific SQL dialect.

        Args:
            query_id: Identifier for the query
            dialect: Target SQL dialect

        Returns:
            Translated SQL query string
        """
        query = self.get_query(query_id)
        return self._translate_sql(query, dialect)

    def _translate_sql(self, sql: str, dialect: str) -> str:
        """
        Translate SQL from the benchmark's native dialect to the target dialect.

        Args:
            sql: SQL query string
            dialect: Target SQL dialect

        Returns:
            Translated SQL query string
        """
        # Use sqlglot for translation
        import sqlglot
        return sqlglot.transpile(sql, read="ansi", write=dialect)[0]
```

### 2.2 DataGenerator Interface

```python
from abc import ABC, abstractmethod
from typing import Dict, Optional, List
import pathlib

class DataGenerator(ABC):
    """Interface for benchmark data generators."""

    @abstractmethod
    def generate_table(self, table_name: str, schema: Dict, scale_factor: float,
                       output_path: pathlib.Path) -> pathlib.Path:
        """
        Generate data for a specific table.

        Args:
            table_name: Name of the table
            schema: Schema definition for the table
            scale_factor: Size multiplier for the generated data
            output_path: Path to write the generated data

        Returns:
            Path to the generated data file
        """
        pass

    @abstractmethod
    def generate_all(self, schemas: Dict[str, Dict], scale_factor: float,
                    output_dir: pathlib.Path) -> Dict[str, pathlib.Path]:
        """
        Generate data for all tables in the benchmark.

        Args:
            schemas: Dictionary mapping table names to their schema definitions
            scale_factor: Size multiplier for the generated data
            output_dir: Directory to write the generated data files

        Returns:
            Dictionary mapping table names to generated data file paths
        """
        pass
```

### 2.3 QueryManager Interface

```python
from abc import ABC, abstractmethod
from typing import Dict, Union

class QueryManager(ABC):
    """Interface for managing benchmark queries."""

    @abstractmethod
    def get_query(self, query_id: Union[int, str]) -> str:
        """
        Get a specific query by ID.

        Args:
            query_id: Identifier for the query

        Returns:
            SQL query string
        """
        pass

    @abstractmethod
    def get_all_queries(self) -> Dict[Union[int, str], str]:
        """
        Get all queries managed by this instance.

        Returns:
            Dictionary mapping query IDs to SQL query strings
        """
        pass

    @abstractmethod
    def translate_query(self, query_id: Union[int, str], dialect: str) -> str:
        """
        Translate a query to a specific SQL dialect.

        Args:
            query_id: Identifier for the query
            dialect: Target SQL dialect

        Returns:
            Translated SQL query string
        """
        pass
```

## 3. Module Structure and Dependencies

### 3.1 Module Organization

```
benchbox/
│
├── __init__.py                  # Package exports
├── core/                        # Core framework components
│   ├── __init__.py
│   ├── base.py                  # Base abstract classes
│   ├── data/                    # Data generation framework
│   │   ├── __init__.py
│   │   ├── generator.py         # Data generator interfaces
│   │   ├── schema.py            # Schema definition utilities
│   │   └── random.py            # Random data generation utilities
│   ├── sql/                     # SQL management framework
│   │   ├── __init__.py
│   │   ├── manager.py           # Query manager interfaces
│   │   └── translator.py        # SQL translation utilities
│   └── utils/                   # Common utilities
│       ├── __init__.py
│       ├── file.py              # File handling utilities
│       └── validation.py        # Validation utilities
│
├── benchmarks/                  # Benchmark implementations
│   ├── __init__.py
│   ├── tpch/                    # TPC-H benchmark
│   │   ├── __init__.py
│   │   ├── benchmark.py         # TPC-H implementation
│   │   ├── schema.py            # TPC-H schema definitions
│   │   ├── generator.py         # TPC-H data generator
│   │   └── queries/             # TPC-H query templates
│   │       ├── __init__.py
│   │       ├── q1.sql
│   │       └── ...
│   ├── tpcds/                   # TPC-DS benchmark
│   │   ├── ...
│   ├── ssb/                     # Star Schema Benchmark
│   │   ├── ...
│   └── ...                      # Other benchmarks
│
└── cli/                         # Command-line interface
    ├── __init__.py
    └── main.py                  # CLI entry point
```

### 3.2 Dependency Relationships

```
┌─────────────────┐     ┌───────────────┐     ┌─────────────────┐
│                 │     │               │     │                 │
│ Benchmark Impl  │────▶│ Core Framework│◀────│ CLI Application │
│                 │     │               │     │                 │
└─────────────────┘     └───────────────┘     └─────────────────┘
        │                      │                       │
        │                      │                       │
        │                      ▼                       │
        │              ┌───────────────┐               │
        └────────────▶│  Data Generator│◀──────────────┘
                      │               │
                      └───────────────┘
                            │
                            │
                            ▼
                      ┌───────────────┐
                      │               │
                      │  SQL Manager  │
                      │               │
                      └───────────────┘
```

## 4. Design Patterns and Extensibility

### 4.1 Strategy Pattern for Data Generation

The architecture employs the Strategy pattern for data generation, allowing different generation algorithms to be plugged in based on the benchmark type.

```python
from abc import ABC, abstractmethod
import pathlib
from typing import Dict

class GenerationStrategy(ABC):
    """Strategy interface for data generation algorithms."""

    @abstractmethod
    def generate(self, schema: Dict, scale_factor: float, output_path: pathlib.Path) -> pathlib.Path:
        """Generate data according to the strategy."""
        pass

class RandomDataStrategy(GenerationStrategy):
    """Generate random data based on schema constraints."""

    def generate(self, schema: Dict, scale_factor: float, output_path: pathlib.Path) -> pathlib.Path:
        # Implementation for random data generation
        pass

class DeterministicDataStrategy(GenerationStrategy):
    """Generate deterministic data based on benchmark specifications."""

    def generate(self, schema: Dict, scale_factor: float, output_path: pathlib.Path) -> pathlib.Path:
        # Implementation for deterministic data generation
        pass
```

### 4.2 Factory Method for Benchmark Creation

```python
class BenchmarkFactory:
    """Factory for creating benchmark instances."""

    @staticmethod
    def create_benchmark(benchmark_type: str, scale_factor: float = 1.0, **kwargs):
        """
        Create a benchmark instance of the specified type.

        Args:
            benchmark_type: Type of benchmark ('tpch', 'tpcds', 'ssb', etc.)
            scale_factor: Size of the generated dataset
            **kwargs: Additional benchmark-specific parameters

        Returns:
            BaseBenchmark instance
        """
        benchmark_type = benchmark_type.lower()

        if benchmark_type == 'tpch':
            from benchbox import TPCH
            return TPCH(scale_factor=scale_factor, **kwargs)
        elif benchmark_type == 'tpcds':
            from benchbox import TPCDS
            return TPCDS(scale_factor=scale_factor, **kwargs)
        elif benchmark_type == 'ssb':
            from benchbox import SSB
            return SSB(scale_factor=scale_factor, **kwargs)
        # Add more benchmark types as they are implemented
        else:
            raise ValueError(f"Unsupported benchmark type: {benchmark_type}")
```

### 4.3 Template Method for Benchmark Execution

The BaseBenchmark class uses the Template Method pattern to define the skeleton of the benchmark execution process, with specific steps implemented by subclasses.

```python
class BaseBenchmark(ABC):
    # ... other methods ...

    def run(self, connection, query_ids=None):
        """
        Run benchmark queries against a database connection.

        Args:
            connection: Database connection object
            query_ids: List of query IDs to run (runs all if None)

        Returns:
            Dictionary mapping query IDs to execution results
        """
        # 1. Prepare benchmark
        self._prepare_benchmark(connection)

        # 2. Determine which queries to run
        if query_ids is None:
            queries = self.get_queries()
        else:
            queries = {qid: self.get_query(qid) for qid in query_ids}

        # 3. Execute queries and collect results
        results = {}
        for qid, query in queries.items():
            results[qid] = self._execute_query(connection, query)

        # 4. Post-process results
        return self._post_process_results(results)

    @abstractmethod
    def _prepare_benchmark(self, connection):
        """Prepare the database for benchmark execution."""
        pass

    @abstractmethod
    def _execute_query(self, connection, query):
        """Execute a query and return its result."""
        pass

    @abstractmethod
    def _post_process_results(self, results):
        """Post-process benchmark results."""
        pass
```

## 5. Key Design Decisions

### 5.1 Data Generation Strategy

**Decision**: Use official TPC tools (dbgen for TPC-H, dsdgen for TPC-DS) for data generation.

**Rationale**:
- Official tools ensure specification compliance
- Template-based query generation works independently of data generation

**Implementation Strategy**:
- Use external TPC tools for official compliance
- Use pseudo-random number generators with fixed seeds for deterministic parameter generation
- Support both full data generation and query-only usage patterns

### 5.2 Embedded Query Storage

**Decision**: Embed SQL queries directly in the library code rather than loading from external files.

**Rationale**:
- Simplifies distribution and packaging
- Eliminates file system dependencies
- Allows for programmatic query manipulation and introspection

**Implementation Strategy**:
- Store queries as string constants or templates in Python modules
- Organize queries by benchmark and query ID
- Provide interface for retrieving and customizing queries

### 5.3 SQL Dialect Translation

**Decision**: Use sqlglot for SQL translation between different database dialects.

**Rationale**:
- Leverages an established SQL parsing and translation library
- Supports a wide range of SQL dialects
- Provides a clean abstraction for SQL manipulation

**Implementation Strategy**:
- Wrap sqlglot functionality in a simple interface
- Implement benchmark-specific SQL transformations when needed
- Cache translated queries for performance

### 5.4 Extensibility Model

**Decision**: Use abstract base classes and interfaces to define extension points.

**Rationale**:
- Provides clear contracts for implementing new benchmarks
- Ensures consistency across different benchmark implementations
- Simplifies the process of adding new benchmark types

**Implementation Strategy**:
- Define core interfaces for key components
- Implement concrete benchmark classes for each supported benchmark
- Document extension patterns and provide examples

## 6. Class/Interface Diagram

```
┌───────────────────┐     ┌───────────────────┐     ┌───────────────────┐
│  BaseBenchmark    │     │  DataGenerator    │     │   QueryManager    │
│  (Abstract)       │     │  (Interface)      │     │  (Interface)      │
├───────────────────┤     ├───────────────────┤     ├───────────────────┤
│ - scale_factor    │     │                   │     │                   │
│ - output_dir      │     │                   │     │                   │
├───────────────────┤     ├───────────────────┤     ├───────────────────┤
│ + get_schema()    │     │ + generate_table()│     │ + get_query()     │
│ + generate_data() │◄────┤ + generate_all()  │     │ + get_all_queries()│
│ + get_query()     │     │                   │     │ + translate_query()│
│ + get_queries()   │◄────┼───────────────────┘     │                   │
│ + translate_query()│    │                         └───────────────────┘
└───────────────────┘    │                                  ▲
          ▲              │                                  │
          │              │                                  │
┌─────────┴──────────┐   │                        ┌─────────┴──────────┐
│                    │   │                        │                    │
│ TPCHBenchmark      │   │                        │ TPCHQueryManager   │
│                    │   │                        │                    │
├────────────────────┤   │                        ├────────────────────┤
│ - tables           │   │                        │ - queries          │
│ - data_generator   │───┘                        │                    │
│ - query_manager    │─────────────────────────── │                    │
├────────────────────┤                            ├────────────────────┤
│ + _initialize()    │                            │ + get_query()      │
│ + get_schema()     │                            │ + get_all_queries()│
│ + generate_data()  │                            │ + translate_query()│
│ + get_query()      │                            │                    │
│ + get_queries()    │                            │                    │
└────────────────────┘                            └────────────────────┘
```

## 7. Implementation Considerations

### 7.1 Performance Considerations

- Use lazy loading for queries and other resources to minimize startup time
- Implement incremental data generation for large datasets
- Consider using Rust components for performance-critical data generation paths
- Cache translated queries to avoid redundant translation

### 7.2 Memory Management

- Use generators and iterators for large data generation to minimize memory usage
- Implement stream-based data writing for large tables
- Consider chunked processing for very large benchmark datasets

### 7.3 Testing Strategy

- Unit test each component separately
- Integration test benchmarks against small-scale data
- Validate generated data against benchmark specifications
- Test SQL translation across multiple dialects

### 7.4 Documentation Standards

- Document public APIs with detailed docstrings
- Provide examples for common use cases
- Include benchmark-specific documentation (schema, query specifications, etc.)
- Document extension points and patterns

## 8. Roadmap and Future Extensions

### 8.1 Phase 1: Core Framework and TPC-H

1. Implement BaseBenchmark and core interfaces
2. Develop data generation framework
3. Implement TPC-H benchmark
4. Add sqlglot integration for SQL translation

### 8.2 Phase 2: Additional Benchmarks

1. Implement TPC-DS benchmark
2. Implement Star Schema Benchmark (SSB)
3. Add H2O/db-benchmark
4. Add ClickBench

### 8.3 Phase 3: Features

1. Add support for custom benchmarks
2. Implement benchmark execution framework
3. Add result analysis and visualization tools
4. Optimize performance for large-scale benchmarks

## 9. Conclusion

The proposed architecture provides a solid foundation for the BenchBox library, with clear separation of concerns, well-defined interfaces, and a modular structure. The design prioritizes extensibility, allowing new benchmarks to be added with minimal effort, while maintaining the hermetic nature of the library.

The architecture balances flexibility and simplicity, providing powerful abstractions while keeping the implementation straightforward. By leveraging established design patterns and following best practices, the design ensures that BenchBox will be maintainable and extensible as it evolves.
