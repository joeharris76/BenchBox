# BenchBox Makefile
# This makefile provides commands for building, testing and development

.PHONY: test test-unit test-integration test-tpch test-all test-fast test-medium test-slow test-pytest clean lint install develop coverage coverage-html coverage-report test-duckdb test-sqlite test-read-primitives test-benchmarks test-ci typecheck validate-imports format dependency-check docs-build docs-serve docs-clean docs-linkcheck docs-validate docs-check test-pyspark

# Primary test commands using pytest marker system
test: test-fast
	@echo "Default test run completed. Use 'make help' to see all test options."

test-all:
	uv run -- python -m pytest -m ""

test-unit:
	uv run -- python -m pytest -m "unit" --tb=short

test-integration:
	uv run -- python -m pytest -m "integration and not live_integration" --tb=short

test-tpch:
	uv run -- python -m pytest -m "tpch" --tb=short

# Quick tests without data generation (using fast marker)
test-quick:
	uv run -- python -m pytest -m "fast and not slow" --tb=short --maxfail=5

# Verbose test output for all tests
test-verbose:
	uv run -- python -m pytest -v

# Enhanced pytest commands using comprehensive marker system
test-pytest:
	uv run -- python -m pytest

# Speed-based testing
test-fast:
	uv run -- python -m pytest -m "fast" --tb=short

test-medium:
	uv run -- python -m pytest -m "medium" --tb=short

test-slow:
	uv run -- python -m pytest -m "slow" --tb=short -v

# Development cycle testing
test-dev:
	uv run -- python -m pytest -m "fast and unit" --tb=short --maxfail=3

# Smoke tests (alias for test-quick)
test-smoke: test-quick

# Database-specific testing
test-duckdb:
	uv run -- python -m pytest -m "duckdb" --tb=short

test-sqlite:
	uv run -- python -m pytest -m "sqlite" --tb=short

test-pyspark:
	./scripts/run_pyspark_tests.sh

# Benchmark-specific testing
test-read-primitives:
	uv run -- python -m pytest -m "primitives" --tb=short

test-benchmarks:
	uv run -- python -m pytest -m "tpch or tpcds or ssb or amplab or clickbench or h2odb or merge" --tb=short

test-tpcds:
	uv run -- python -m pytest -m "tpcds" --tb=short

# TPC-H C-compatible testing
test-tpch-c-compatible:
	uv run -- python -m pytest -m "c_compatible" --tb=short

test-tpch-c-params:
	uv run -- python -m pytest -m "tpch_c_params" --tb=short

test-tpch-c-queries:
	uv run -- python -m pytest -m "tpch_c_queries" --tb=short

test-tpch-c-comparison:
	uv run -- python -m pytest -m "tpch_c_comparison" --tb=short

test-tpch-c-regression:
	uv run -- python -m pytest -m "tpch_c_regression" --tb=short

test-tpch-c-performance:
	uv run -- python -m pytest -m "tpch_c_performance" --tb=short

test-tpch-c-validation:
	uv run -- python -m pytest -m "tpch_c_validation" --tb=short

test-tpch-c-comprehensive:
	uv run -- python -m pytest -m "tpch_c_comprehensive" --tb=short -v

test-tpch-c-all:
	uv run -- python -m pytest tests/test_tpch_c_compatible_base.py tests/test_tpch_parameter_generation.py tests/test_tpch_query_comparison.py tests/test_tpch_regression_compatibility.py tests/test_tpch_performance_edge_cases.py tests/test_tpch_c_compatible_suite.py -m "tpch_c" --tb=short

# Feature-specific testing
test-olap:
	uv run -- python -m pytest -m "olap" --tb=short

test-window:
	uv run -- python -m pytest -m "window_functions" --tb=short

# CI/CD testing
test-ci:
	uv run -- python -m pytest -c pytest-ci.ini -m "not (slow or flaky or local_only)"

# TPC-H C-compatible CI/CD testing
test-tpch-c-ci:
	uv run -- python -m pytest -c pytest-ci.ini -m "tpch_c and not (slow or flaky or local_only)"

test-tpch-c-ci-fast:
	uv run -- python -m pytest -c pytest-ci.ini -m "tpch_c and fast and not (slow or flaky or local_only)"

test-tpch-c-ci-params:
	uv run -- python -m pytest -c pytest-ci.ini -m "tpch_c_params and not (slow or flaky or local_only)"

test-tpch-c-ci-queries:
	uv run -- python -m pytest -c pytest-ci.ini -m "tpch_c_queries and not (slow or flaky or local_only)"

test-tpch-c-ci-validation:
	uv run -- python -m pytest -c pytest-ci.ini -m "tpch_c_validation and not (slow or flaky or local_only)"

test-tpch-c-ci-comprehensive:
	uv run -- python -m pytest -c pytest-ci.ini -m "tpch_c_comprehensive and not (slow or flaky or local_only)" --maxfail=5

# Parallel testing
test-parallel:
	uv run -- python -m pytest -n auto --tb=short

test-parallel-fast:
	uv run -- python -m pytest -n auto -m "fast" --tb=short

# Live integration tests (require cloud credentials)
test-live:
	@echo "Running live integration tests (requires cloud credentials)"
	@echo "See .env.example for credential setup"
	uv run -- python -m pytest -m "live_integration" --tb=short -v

test-live-databricks:
	@echo "Running Databricks live tests (requires DATABRICKS_TOKEN)"
	uv run -- python -m pytest -m "live_databricks" --tb=short -v

test-live-snowflake:
	@echo "Running Snowflake live tests (requires SNOWFLAKE_PASSWORD)"
	uv run -- python -m pytest -m "live_snowflake" --tb=short -v

test-live-bigquery:
	@echo "Running BigQuery live tests (requires BIGQUERY_PROJECT)"
	uv run -- python -m pytest -m "live_bigquery" --tb=short -v

test-live-all:
	@echo "Running all live integration tests (requires credentials for all platforms)"
	uv run -- python -m pytest -m "live_integration" --tb=short -v

# Coverage commands using pytest
coverage:
	uv run -- python -m pytest -c pytest-ci.ini --cov=benchbox --cov-report=term-missing

coverage-html:
	uv run -- python -m pytest -c pytest-ci.ini --cov=benchbox --cov-report=html:htmlcov

coverage-report:
	uv run -- python -m pytest -c pytest-ci.ini --cov=benchbox --cov-report=xml:coverage.xml --cov-report=term-missing

# Install and development
install:
	uv sync

develop:
	uv sync --group dev

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf __pycache__/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	find . -name '*.pyo' -delete
	find . -name '.DS_Store' -delete

# Linting (ruff only)
lint:
	uv run ruff check .

# Type checking
typecheck:
	ty check

# Type checking with uv (for development)
typecheck-uv:
	uv run ty check

# Import validation
validate-imports:
	uv run -- python scripts/validate_imports.py

# Dependency matrix / validation
dependency-check:
	uv run -- python -m benchbox.utils.dependency_validation $(ARGS)
# Format code (ruff formatter)
format:
	uv run ruff format .

##@ Documentation

# Build Sphinx documentation locally
docs-build:
	@echo "Building documentation..."
	@cd docs && sphinx-build -b html --keep-going . _build/html
	@echo "✅ Docs built: docs/_build/html/index.html"

# Build and serve documentation on http://localhost:8000
docs-serve: docs-build
	@echo "Serving docs at http://localhost:8000"
	@echo "Press Ctrl+C to stop"
	@cd docs/_build/html && python -m http.server 8000

# Clean documentation build artifacts
docs-clean:
	@echo "Cleaning documentation build artifacts..."
	@rm -rf docs/_build
	@echo "✅ Documentation artifacts cleaned"

# Check for broken links in documentation
docs-linkcheck:
	@echo "Checking documentation for broken links..."
	@cd docs && sphinx-build -b linkcheck . _build/linkcheck
	@echo ""
	@echo "Link check results:"
	@cat docs/_build/linkcheck/output.txt || echo "No broken links found!"

# Validate example file references
docs-validate:
	@echo "Validating example file references..."
	@uv run -- python scripts/validate_example_references.py
	@echo ""
	@echo "Checking example file syntax..."
	@uv run -- python scripts/check_example_syntax.py

# Run all documentation checks (build, linkcheck, validate)
docs-check: docs-validate docs-linkcheck docs-build
	@echo ""
	@echo "✅ All documentation checks passed!"

# Create distribution packages
dist: clean
	uv build

# Run a specific test file
# Usage: make run-test TEST=tests/specialized/test_tpch_minimal.py
run-test:
	uv run -- python $(TEST)

# Help
help:
	@echo "BenchBox Makefile"
	@echo "----------------"
	@echo "Available commands:"
	@echo ""
	@echo "Core Testing:"
	@echo "  make test            Run default test suite (fast tests)"
	@echo "  make test-all        Run all tests"
	@echo "  make test-unit       Run unit tests only"
	@echo "  make test-integration Run integration tests only"
	@echo "  make test-tpch       Run TPC-H tests only"
	@echo "  make test-quick      Run quick tests without slow operations"
	@echo "  make test-verbose    Run tests with verbose output"
	@echo ""
	@echo "Speed-Based Testing:"
	@echo "  make test-pytest     Run all tests with pytest"
	@echo "  make test-fast       Run fast tests (< 1 sec)"
	@echo "  make test-medium     Run medium speed tests (1-10 sec)"
	@echo "  make test-slow       Run slow tests (> 10 sec)"
	@echo "  make test-dev        Fast development cycle testing"
	@echo "  make test-smoke      Quick smoke testing"
	@echo "  make test-ci         CI-optimized test suite"
	@echo ""
	@echo "Database-Specific Testing:"
	@echo "  make test-duckdb     Run DuckDB-specific tests"
	@echo "  make test-sqlite     Run SQLite-specific tests"
	@echo ""
	@echo "Benchmark-Specific Testing:"
	@echo "  make test-read-primitives Run primitives benchmark tests"
	@echo "  make test-benchmarks Run all benchmark tests"
	@echo "  make test-tpcds      Run TPC-DS tests"
	@echo ""
	@echo "TPC-H C-Compatible Testing:"
	@echo "  make test-tpch-c-compatible  Run all C-compatible tests"
	@echo "  make test-tpch-c-params      Run C-compatible parameter tests"
	@echo "  make test-tpch-c-queries     Run C-compatible query tests"
	@echo "  make test-tpch-c-comparison  Run C vs Python comparison tests"
	@echo "  make test-tpch-c-regression  Run C-compatible regression tests"
	@echo "  make test-tpch-c-performance Run C-compatible performance tests"
	@echo "  make test-tpch-c-validation  Run C-compatible validation tests"
	@echo "  make test-tpch-c-comprehensive Run comprehensive C-compatible suite"
	@echo "  make test-tpch-c-all         Run all TPC-H C tests"
	@echo ""
	@echo "Feature-Specific Testing:"
	@echo "  make test-olap       Run OLAP functionality tests"
	@echo "  make test-window     Run window functions tests"
	@echo ""
	@echo "CI/CD Testing:"
	@echo "  make test-ci         Run CI-optimized test suite"
	@echo "  make test-tpch-c-ci  Run TPC-H C-compatible CI tests"
	@echo "  make test-tpch-c-ci-fast         Run fast TPC-H C CI tests"
	@echo "  make test-tpch-c-ci-params       Run TPC-H C parameter CI tests"
	@echo "  make test-tpch-c-ci-queries      Run TPC-H C query CI tests"
	@echo "  make test-tpch-c-ci-validation   Run TPC-H C validation CI tests"
	@echo "  make test-tpch-c-ci-comprehensive Run TPC-H C comprehensive CI tests"
	@echo ""
	@echo "Parallel Testing:"
	@echo "  make test-parallel   Run tests in parallel"
	@echo "  make test-parallel-fast Run fast tests in parallel"
	@echo ""
	@echo "Live Integration Testing (requires cloud credentials):"
	@echo "  make test-live       Run all live integration tests"
	@echo "  make test-live-databricks Run Databricks live tests"
	@echo "  make test-live-snowflake  Run Snowflake live tests"
	@echo "  make test-live-bigquery   Run BigQuery live tests"
	@echo "  make test-live-all   Run all live tests (all platforms)"
	@echo ""
	@echo "Utility:"
	@echo "  make run-test TEST=path Run a specific test file"
	@echo ""
	@echo "Coverage:"
	@echo "  make coverage        Run tests with coverage report"
	@echo "  make coverage-html   Generate HTML coverage report"
	@echo "  make coverage-report Generate comprehensive coverage reports"
	@echo ""
	@echo "Development:"
	@echo "  make lint            Check code style"
	@echo "  make typecheck       Run type checking with ty"
	@echo "  make typecheck-uv    Run type checking with uv (development)"
	@echo "  make validate-imports Validate import structure and detect circular dependencies"
	@echo "  make dependency-check Validate lock file against pyproject specs (ARGS='--matrix' to show summary)"
	@echo "  make format          Format code with ruff"
	@echo "  make clean           Remove build artifacts"
	@echo "  make install         Install the package"
	@echo "  make develop         Install the package in development mode"
	@echo "  make dist            Create distribution packages"
	@echo ""
	@echo "Documentation:"
	@echo "  make docs-build      Build Sphinx documentation locally"
	@echo "  make docs-serve      Build and serve docs at http://localhost:8000"
	@echo "  make docs-clean      Clean documentation build artifacts"
	@echo "  make docs-linkcheck  Check for broken links in documentation"
	@echo "  make docs-validate   Validate example file references and syntax"
	@echo "  make docs-check      Run all documentation checks (validate, linkcheck, build)"
	@echo ""
	@echo "  make help            Show this help message"
