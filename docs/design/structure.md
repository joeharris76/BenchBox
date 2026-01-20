<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# BenchBox Repository Structure

```{tags} contributor, concept
```

```
BenchBox/
├── .github/                        # GitHub specific files
│   └── workflows/                  # GitHub Actions CI/CD workflows
│       ├── test.yml                # Run tests on PRs and pushes
│       ├── lint.yml                # Run linting checks
│       └── release.yml             # Handle releases to PyPI
├── benchbox/                       # Main package directory
│   ├── __init__.py                 # Package initialization
│   ├── base.py                     # Base class for benchmarks
│   ├── tpch.py                     # TPC-H implementation
│   ├── tpcds.py                    # TPC-DS implementation
│   ├── tpcdi.py                    # TPC-DI implementation
│   ├── ssb.py                      # SSB implementation
│   ├── amplab.py                   # AMPLab benchmark implementation
│   ├── h2odb.py                    # H2O/db-benchmark implementation
│   ├── clickbench.py               # ClickBench implementation
│   ├── util/                       # Utility modules
│   │   ├── __init__.py
│   │   ├── data_gen.py             # Data generation utilities
│   │   └── sql.py                  # SQL handling utilities
│   └── data/                       # Static data files (if needed)
│       └── __init__.py
├── tests/                          # Test directory
│   ├── __init__.py
│   ├── conftest.py                 # Common pytest fixtures
│   ├── test_base.py                # Tests for base functionality
│   ├── test_tpch.py                # Tests for TPC-H benchmark
│   └── ...                         # Other benchmark tests
├── docs/                           # Documentation
│   ├── conf.py                     # Sphinx configuration
│   ├── index.rst                   # Documentation index
│   └── ...                         # Other documentation files
├── examples/                       # Example scripts
│   ├── basic_usage.py
│   ├── dialect_translation.py
│   └── ...                         # Other examples
├── .gitignore                      # Git ignore file
├── .pre-commit-config.yaml         # pre-commit hooks configuration
├── pyproject.toml                  # Project metadata and build config
├── README.md                       # Project README
├── CONTRIBUTING.md                 # Contribution guidelines
├── LICENSE                         # Project license
└── CHANGELOG.md                    # Project changelog
```
