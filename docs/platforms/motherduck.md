<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# MotherDuck Platform

```{tags} intermediate, guide, motherduck, sql-platform, cloud
```

MotherDuck is the managed cloud version of DuckDB, providing serverless analytics with cloud storage integration. BenchBox provides first-class MotherDuck support, inheriting DuckDB's SQL dialect and benchmark compatibility.

## Features

- **Serverless DuckDB** - No infrastructure management
- **DuckDB dialect** - Inherits SQL dialect from DuckDB
- **Token authentication** - Simple token-based auth
- **Hybrid queries** - Access both local and cloud data
- **Auto-scaling** - Automatic resource management

## Quick Start

```bash
# Install DuckDB (includes MotherDuck support)
uv add duckdb

# Set your token
export MOTHERDUCK_TOKEN=your-token

# Run benchmark
benchbox run --platform motherduck --benchmark tpch --scale 1.0
```

## Authentication

### Getting Your Token

1. Sign in to [app.motherduck.com](https://app.motherduck.com)
2. Navigate to [app.motherduck.com/token-request](https://app.motherduck.com/token-request)
3. Copy your authentication token

### Configuration

**Environment Variable (recommended):**

```bash
export MOTHERDUCK_TOKEN=your-motherduck-token

benchbox run --platform motherduck --benchmark tpch --scale 0.1
```

**CLI Option:**

```bash
benchbox run --platform motherduck --benchmark tpch --scale 0.1 \
    --platform-option token=your-motherduck-token
```

## Configuration Options

| Option | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| `token` | `MOTHERDUCK_TOKEN` | - | Authentication token (required) |
| `database` | - | `benchbox` | MotherDuck database name |
| `memory_limit` | - | `4GB` | Local memory limit for hybrid queries |

## Usage Examples

### Basic Benchmark

```bash
# TPC-H at scale factor 1
benchbox run --platform motherduck --benchmark tpch --scale 1.0

# TPC-DS at scale factor 10
benchbox run --platform motherduck --benchmark tpcds --scale 10.0
```

### Custom Database Name

```bash
benchbox run --platform motherduck --benchmark tpch --scale 1.0 \
    --platform-option database=my_benchmarks
```

### With Memory Limit

```bash
# Increase local memory for hybrid queries
benchbox run --platform motherduck --benchmark tpch --scale 10.0 \
    --platform-option memory_limit=8GB
```

## Python API

```python
from benchbox import TPCH
from benchbox.platforms.motherduck import MotherDuckAdapter

# Initialize adapter
adapter = MotherDuckAdapter(
    token="your-motherduck-token",  # Or use MOTHERDUCK_TOKEN env var
    database="benchmarks",
    memory_limit="4GB",
)

# Load and run benchmark
benchmark = TPCH(scale_factor=1.0)
benchmark.generate_data()
adapter.load_benchmark(benchmark)
results = adapter.run_benchmark(benchmark)
```

## Architecture

MotherDuck inherits from DuckDB, which means:

- **SQL Dialect**: Uses DuckDB's SQL dialect for query translation
- **Data Types**: Same data type mappings as DuckDB
- **Benchmark Compatibility**: Supports all benchmarks that DuckDB supports

```python
from benchbox.core.platform_registry import PlatformRegistry

# Check platform family
family = PlatformRegistry.get_platform_family("motherduck")
# Returns: "duckdb"

# Check inheritance
parent = PlatformRegistry.get_inherited_platform("motherduck")
# Returns: "duckdb"
```

## Hybrid Queries

MotherDuck supports hybrid queries that access both local and cloud data:

```python
# The adapter handles this automatically
# Local data is uploaded to MotherDuck during benchmark loading
```

## Comparison: MotherDuck vs DuckDB

| Feature | MotherDuck | DuckDB |
|---------|------------|--------|
| Deployment | Cloud managed | Local embedded |
| Authentication | Token required | None |
| Scaling | Automatic | Manual |
| Data Location | Cloud | Local filesystem |
| Cost | Pay-per-use | Free |
| Best For | Production, large scale | Development, testing |

## When to Use MotherDuck

**Use MotherDuck when:**
- You need cloud-based analytics without infrastructure management
- Running benchmarks at larger scale factors (SF > 10)
- Sharing benchmark databases across team members
- Comparing local vs cloud DuckDB performance

**Use DuckDB instead when:**
- Developing and testing locally
- No cloud credentials available
- Cost is a primary concern
- Running small-scale benchmarks (SF < 1)

## Troubleshooting

### Invalid Token

```
ValueError: MotherDuck requires authentication token.
```

**Solution:** Ensure `MOTHERDUCK_TOKEN` is set or pass `--platform-option token=...`

### Connection Failed

```
ConnectionError: Failed to connect to MotherDuck
```

**Solutions:**
1. Verify your token at [app.motherduck.com](https://app.motherduck.com)
2. Check network connectivity
3. Ensure DuckDB >= 0.9.0 is installed (`uv add duckdb>=0.9.0`)

### Database Not Found

MotherDuck databases are created automatically when you first connect. If you see database-related errors:

```bash
# Specify a fresh database name
benchbox run --platform motherduck --benchmark tpch --scale 0.1 \
    --platform-option database=fresh_benchmark_db
```

## Related Documentation

- [DuckDB Platform](duckdb.md) - Local DuckDB benchmarking
- [Deployment Modes Guide](deployment-modes.md) - Platform deployment architecture
- [Platform Selection Guide](platform-selection-guide.md) - Choose the right platform
- [Getting Started](../usage/getting-started.md) - Quick start guide
