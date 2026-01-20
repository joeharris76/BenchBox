<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# TimescaleDB Platform

```{tags} intermediate, guide, timescaledb, sql-platform
```

TimescaleDB is a PostgreSQL extension that transforms PostgreSQL into a time-series database with automatic time-based partitioning (hypertables), native compression, and continuous aggregates. BenchBox provides first-class TimescaleDB support for time-series benchmarking.

## Features

- **Automatic hypertables** - Time-series tables automatically converted to hypertables
- **Native compression** - Configurable compression policies for historical data
- **Chunk management** - Automatic time-based partitioning with configurable intervals
- **PostgreSQL compatible** - Uses PostgreSQL COPY for efficient bulk loading
- **TSBS support** - Optimized for Time Series Benchmark Suite (TSBS) DevOps workload
- **Two deployment modes** - Self-hosted and Timescale Cloud

## Deployment Modes

TimescaleDB supports two deployment modes, selectable via the colon syntax:

```bash
# Self-hosted (default)
benchbox run --platform timescaledb --benchmark tsbs-devops --scale 1.0

# Timescale Cloud (managed service)
benchbox run --platform timescaledb:cloud --benchmark tsbs-devops --scale 1.0
```

### Self-Hosted Mode (Default)

Connect to a self-hosted PostgreSQL server with TimescaleDB extension:
- TimescaleDB 2.x extension must be installed
- Full database management (CREATE/DROP database) available

### Timescale Cloud Mode

Connect to Timescale Cloud managed service:
- Requires SSL connection (`sslmode=require`)
- Database management (DROP/CREATE) disabled for managed databases
- Sign up at [timescale.com](https://www.timescale.com/cloud)

**Cloud Configuration:**

```bash
# Option 1: Service URL (recommended)
export TIMESCALE_SERVICE_URL=postgres://user:pass@abc123.tsdb.cloud.timescale.com:5432/tsdb?sslmode=require

# Option 2: Individual environment variables
export TIMESCALE_HOST=abc123.rc8ft3nbrw.tsdb.cloud.timescale.com
export TIMESCALE_PASSWORD=your-password
export TIMESCALE_USER=tsdbadmin  # optional, defaults to 'tsdbadmin'

# Run benchmark
benchbox run --platform timescaledb:cloud --benchmark tsbs-devops --scale 1.0
```

## Installation

TimescaleDB support uses the same driver as PostgreSQL:

```bash
# Install with pip
pip install psycopg2-binary

# Or with system psycopg2 (recommended for production)
pip install psycopg2
```

**Server requirements:** TimescaleDB 2.x extension must be installed on your PostgreSQL server.

```sql
-- Install TimescaleDB extension (on server)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
```

## Configuration

### CLI Options

```bash
benchbox run --platform timescaledb --benchmark tsbs-devops --scale 1.0 \
  --platform-option host=localhost \
  --platform-option port=5432 \
  --platform-option database=metrics \
  --platform-option chunk_interval="1 day" \
  --platform-option compression_enabled=true
```

### Platform Options

| Option | Default | Description |
|--------|---------|-------------|
| `host` | localhost | TimescaleDB server hostname |
| `port` | 5432 | TimescaleDB server port |
| `database` | auto-generated | Database name |
| `username` | postgres | Database username |
| `password` | (none) | Database password |
| `schema` | public | Target schema |
| `chunk_interval` | 1 day | Time interval for hypertable chunks |
| `compression_enabled` | false | Enable automatic compression |
| `compression_after` | 7 days | Compress chunks older than this |

## Usage Examples

### TSBS DevOps Benchmark

The TSBS DevOps benchmark is ideal for TimescaleDB:

```bash
# Run TSBS DevOps benchmark
benchbox run --platform timescaledb --benchmark tsbs-devops --scale 1.0 \
  --platform-option host=localhost \
  --platform-option chunk_interval="1 hour" \
  --platform-option compression_enabled=true
```

### Python API

```python
from benchbox import TSBSDevOps
from benchbox.platforms.timescaledb import TimescaleDBAdapter

# Initialize adapter with TimescaleDB features
adapter = TimescaleDBAdapter(
    host="localhost",
    port=5432,
    database="metrics_db",
    username="postgres",
    password="password",
    chunk_interval="1 hour",
    compression_enabled=True,
    compression_after="7 days",
)

# Load and run benchmark
benchmark = TSBSDevOps(scale_factor=1.0)
benchmark.generate_data()
adapter.load_benchmark(benchmark)
results = adapter.run_benchmark(benchmark)
```

### With Compression

```bash
# Enable compression for historical data
benchbox run --platform timescaledb --benchmark tsbs-devops \
  --platform-option compression_enabled=true \
  --platform-option compression_after="3 days"
```

## Hypertable Configuration

TimescaleDB automatically converts tables with a `time` column to hypertables. The `chunk_interval` option controls partitioning:

| Interval | Use Case |
|----------|----------|
| 1 hour | High-frequency IoT data |
| 1 day | Standard metrics collection |
| 1 week | Lower-frequency data |

```bash
# High-frequency data (small chunks)
benchbox run --platform timescaledb --benchmark tsbs-devops \
  --platform-option chunk_interval="1 hour"

# Standard metrics (larger chunks)
benchbox run --platform timescaledb --benchmark tsbs-devops \
  --platform-option chunk_interval="1 day"
```

## Compression

TimescaleDB's native compression can significantly reduce storage requirements:

```bash
# Enable compression with custom policy
benchbox run --platform timescaledb --benchmark tsbs-devops \
  --platform-option compression_enabled=true \
  --platform-option compression_after="7 days"
```

Compression is applied automatically to chunks older than the `compression_after` interval.

## Performance Tuning

### Recommended Server Settings

For benchmark workloads, consider these PostgreSQL/TimescaleDB settings:

```sql
-- postgresql.conf
shared_buffers = 4GB
effective_cache_size = 12GB
work_mem = 256MB
maintenance_work_mem = 1GB

-- TimescaleDB specific
timescaledb.max_background_workers = 8
timescaledb.max_insert_batch_size = 10000
```

### Chunk Skipping

TimescaleDB automatically skips irrelevant chunks based on time predicates:

```bash
# Enable chunk skipping (enabled by default)
benchbox run --platform timescaledb --benchmark tsbs-devops
```

## When to Use TimescaleDB vs PostgreSQL

| Use TimescaleDB when... | Use PostgreSQL when... |
|-------------------------|------------------------|
| Time-series workloads | Relational/OLTP workloads |
| High-volume metrics | TPC-H/TPC-DS benchmarks |
| Need compression | Need row-oriented storage |
| TSBS DevOps benchmark | Standard SQL benchmarks |

## Troubleshooting

### Extension Not Found

```bash
# Check if TimescaleDB is installed
psql -c "SELECT * FROM pg_extension WHERE extname = 'timescaledb';"

# Install if missing
psql -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
```

### Hypertable Creation Failed

```bash
# Verify table has a time column
psql -c "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'your_table';"

# Manual hypertable creation
psql -c "SELECT create_hypertable('your_table', 'time', if_not_exists => TRUE);"
```

### Compression Not Applied

```bash
# Check compression policies
psql -c "SELECT * FROM timescaledb_information.compression_settings;"

# Manually compress old chunks
psql -c "SELECT compress_chunk(c) FROM show_chunks('your_table', older_than => INTERVAL '7 days') c;"
```

## Related Documentation

- [PostgreSQL Platform](postgresql.md) - Base PostgreSQL support
- [TSBS DevOps Benchmark](../benchmarks/tsbs-devops.md) - Time-series benchmark
- [Platform Selection Guide](platform-selection-guide.md)
- [InfluxDB Platform](influxdb.md) - Alternative time-series database
