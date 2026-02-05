<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# Result Export Formats

```{tags} reference, validation
```

BenchBox exports benchmark results in multiple formats for analysis, visualization, and integration with external tools.

## Export Commands

### Basic Export

```bash
# Run benchmark and export results
benchbox run --platform duckdb --benchmark tpch --scale 0.1

# Results are automatically saved to benchmark_runs/results/
ls benchmark_runs/results/
# tpch_duckdb_sf0.01_20251212_143021.json
```

### Specify Formats

```bash
# Export JSON (default)
benchbox run --platform duckdb --benchmark tpch --format json

# Export multiple formats
benchbox run --platform duckdb --benchmark tpch --format json,csv,html
```

### Custom Output Directory

```bash
# Local directory
benchbox run --platform duckdb --benchmark tpch --output ./my_results/

# Cloud storage
benchbox run --platform snowflake --benchmark tpch --output s3://bucket/results/
```

## JSON Format (Schema v1.1)

The JSON export is the canonical format containing complete benchmark details.

### Schema Structure

```json
{
  "_schema": {
    "version": "1.1",
    "generator": "benchbox-exporter",
    "generated_at": "2025-12-12T14:30:21.123456Z"
  },
  "benchmark": {
    "name": "tpch",
    "scale_factor": 0.1,
    "version": "3.0.1"
  },
  "platform": {
    "name": "duckdb",
    "version": "0.10.0",
    "dialect": "duckdb"
  },
  "execution": {
    "timestamp": "2025-12-12T14:30:21.123456Z",
    "duration_ms": 45230,
    "status": "completed"
  },
  "phases": {
    "setup": { ... },
    "power_test": { ... },
    "throughput_test": { ... }
  },
  "metrics": {
    "geometric_mean_time": 1.234,
    "power_at_size": 89.5,
    "throughput_at_size": 156.2,
    "qph_at_size": 1250.5
  }
}
```

### Field Reference

#### Schema Block
| Field | Type | Description |
|-------|------|-------------|
| `version` | string | Schema version (currently "1.1") |
| `generator` | string | Tool that generated the export |
| `generated_at` | string | ISO 8601 timestamp |

#### Benchmark Block
| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Benchmark identifier (tpch, tpcds, ssb, etc.) |
| `scale_factor` | float | Data scale factor |
| `version` | string | TPC specification version |
| `tuning` | string | Tuning mode (tuned, notuning, auto) |

#### Platform Block
| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Platform identifier |
| `version` | string | Platform/driver version |
| `dialect` | string | SQL dialect used |
| `connection_info` | object | Anonymized connection details |

#### Execution Block
| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | string | Run start time (ISO 8601) |
| `duration_ms` | int | Total execution time |
| `status` | string | completed, failed, partial |
| `phases_run` | array | List of executed phases |

#### Phases Block

**Setup Phase:**
```json
{
  "setup": {
    "data_generation": {
      "duration_ms": 5230,
      "status": "completed",
      "tables_generated": 8,
      "total_rows_generated": 150000,
      "total_data_size_bytes": 45000000
    },
    "schema_creation": {
      "duration_ms": 120,
      "tables_created": 8,
      "constraints_applied": 16,
      "indexes_created": 8
    },
    "data_loading": {
      "duration_ms": 3500,
      "total_rows_loaded": 150000,
      "tables_loaded": 8,
      "per_table_stats": { ... }
    }
  }
}
```

**Power Test Phase:**
```json
{
  "power_test": {
    "start_time": "2025-12-12T14:30:25.000Z",
    "end_time": "2025-12-12T14:31:42.000Z",
    "duration_ms": 77000,
    "geometric_mean_time": 1.234,
    "power_at_size": 89.5,
    "query_executions": [
      {
        "query_id": "Q1",
        "stream_id": "power",
        "execution_order": 1,
        "execution_time_ms": 1520,
        "status": "success",
        "rows_returned": 4,
        "row_count_validation": {
          "expected": 4,
          "actual": 4,
          "status": "valid"
        }
      }
    ]
  }
}
```

**Throughput Test Phase:**
```json
{
  "throughput_test": {
    "start_time": "2025-12-12T14:31:45.000Z",
    "end_time": "2025-12-12T14:35:12.000Z",
    "duration_ms": 207000,
    "num_streams": 4,
    "total_queries_executed": 88,
    "throughput_at_size": 156.2,
    "streams": [
      {
        "stream_id": 1,
        "duration_ms": 51000,
        "query_executions": [ ... ]
      }
    ]
  }
}
```

#### Metrics Block
| Field | Type | Description |
|-------|------|-------------|
| `geometric_mean_time` | float | Geometric mean of query times (seconds) |
| `power_at_size` | float | TPC Power metric |
| `throughput_at_size` | float | TPC Throughput metric |
| `qph_at_size` | float | Queries per hour at scale |
| `cost_estimate_usd` | float | Estimated run cost (cloud platforms) |

### Query Execution Details

Each query execution record contains:

```json
{
  "query_id": "Q6",
  "stream_id": "power",
  "execution_order": 6,
  "execution_time_ms": 234,
  "status": "success",
  "rows_returned": 1,
  "row_count_validation": {
    "expected": 1,
    "actual": 1,
    "status": "valid"
  },
  "cost": 0.0023,
  "query_plan": {
    "root_operation": "ProjectionExec",
    "total_operators": 12,
    "estimated_rows": 150000
  },
  "plan_fingerprint": "a1b2c3d4..."
}
```

## CSV Format

CSV export provides tabular query-level data for spreadsheet analysis.

### Query Results CSV

```csv
query_id,stream_id,execution_order,execution_time_ms,status,rows_returned,expected_rows,validation_status
Q1,power,1,1520,success,4,4,valid
Q2,power,2,892,success,460,460,valid
Q3,power,3,1230,success,10,10,valid
...
```

### Summary CSV

```csv
metric,value
benchmark,tpch
scale_factor,0.1
platform,duckdb
geometric_mean_time,1.234
power_at_size,89.5
throughput_at_size,156.2
total_duration_ms,45230
```

## HTML Format

HTML export generates a standalone report with embedded visualizations.

```bash
# Generate HTML report
benchbox run --platform duckdb --benchmark tpch --format html

# Open in browser
open benchmark_runs/results/tpch_duckdb_sf0.01_*.html
```

The HTML report includes:
- Summary metrics card
- Query timing bar chart
- Phase duration breakdown
- Validation status table
- Platform and configuration details

## Loading Results in Python

### Load JSON Results

```python
import json
from pathlib import Path

# Load result file
result_file = Path("benchmark_runs/results/tpch_duckdb_sf0.01_20251212_143021.json")
with result_file.open() as f:
    results = json.load(f)

# Access metrics
print(f"Power at Size: {results['metrics']['power_at_size']}")
print(f"Geometric Mean: {results['metrics']['geometric_mean_time']}s")

# Access query details
for query in results['phases']['power_test']['query_executions']:
    print(f"{query['query_id']}: {query['execution_time_ms']}ms")
```

### Load into Pandas

```python
import pandas as pd
import json

# Load JSON
with open("benchmark_runs/results/tpch_duckdb_sf0.01_*.json") as f:
    results = json.load(f)

# Convert queries to DataFrame
queries = results['phases']['power_test']['query_executions']
df = pd.DataFrame(queries)

# Analyze
print(df.describe())
print(df.groupby('query_id')['execution_time_ms'].mean())
```

### Load CSV Results

```python
import pandas as pd

# Load query results
df = pd.read_csv("benchmark_runs/results/tpch_duckdb_sf0.01_queries.csv")

# Quick analysis
print(f"Total queries: {len(df)}")
print(f"Mean execution time: {df['execution_time_ms'].mean():.2f}ms")
print(f"Slowest query: {df.loc[df['execution_time_ms'].idxmax(), 'query_id']}")
```

## Visualization Examples

### Query Timing Chart

```python
import matplotlib.pyplot as plt
import json

# Load results
with open("results.json") as f:
    results = json.load(f)

queries = results['phases']['power_test']['query_executions']
df = pd.DataFrame(queries)

# Create bar chart
plt.figure(figsize=(12, 6))
plt.bar(df['query_id'], df['execution_time_ms'])
plt.xlabel('Query')
plt.ylabel('Execution Time (ms)')
plt.title(f"TPC-H Power Test - {results['platform']['name']}")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('query_timing.png')
```

### Platform Comparison

```python
import json
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Load multiple results
results_dir = Path("benchmark_runs/results")
all_results = []

for result_file in results_dir.glob("tpch_*_sf0.01_*.json"):
    with result_file.open() as f:
        data = json.load(f)
        all_results.append({
            'platform': data['platform']['name'],
            'power_at_size': data['metrics']['power_at_size'],
            'geometric_mean': data['metrics']['geometric_mean_time']
        })

df = pd.DataFrame(all_results)

# Compare platforms
plt.figure(figsize=(10, 6))
plt.barh(df['platform'], df['power_at_size'])
plt.xlabel('Power@Size')
plt.title('TPC-H Power Metric by Platform')
plt.tight_layout()
plt.savefig('platform_comparison.png')
```

## Schema Versioning

### Current Version: 1.1

Version 1.1 includes:
- Query plan capture
- Cost estimation
- Row count validation details
- Phase-level error tracking

### Version History

| Version | Changes |
|---------|---------|
| 1.1 | Added query plans, cost estimation, validation details |
| 1.0 | Initial canonical schema |

### Loading Legacy Results

```python
from benchbox.core.results.loader import load_result

# Automatically handles schema versions
result = load_result("old_result.json")
print(f"Schema version: {result.schema_version}")
```

## Anonymization

Results are anonymized by default to remove sensitive information:

- Connection strings → hashed
- Hostnames → generalized
- Usernames → removed
- API keys/tokens → stripped

### Disable Anonymization

```bash
# For internal use only
benchbox run --platform snowflake --benchmark tpch \
  --no-anonymize
```

### Anonymization Config

```python
from benchbox.core.results.exporter import ResultExporter
from benchbox.core.results.anonymization import AnonymizationConfig

config = AnonymizationConfig(
    anonymize_hostname=True,
    anonymize_username=True,
    anonymize_connection_string=True,
    preserve_platform_version=True
)

exporter = ResultExporter(anonymize=True, anonymization_config=config)
```

## Integration Examples

### Export to Data Warehouse

```python
import json
import pandas as pd

# Load results
with open("results.json") as f:
    results = json.load(f)

# Flatten to table
queries = []
for q in results['phases']['power_test']['query_executions']:
    queries.append({
        'run_id': results['execution']['timestamp'],
        'benchmark': results['benchmark']['name'],
        'platform': results['platform']['name'],
        'scale_factor': results['benchmark']['scale_factor'],
        **q
    })

df = pd.DataFrame(queries)

# Upload to warehouse
# df.to_sql('benchmark_queries', engine, if_exists='append')
```

### CI/CD Integration

```bash
# Run benchmark and check threshold
benchbox run --platform duckdb --benchmark tpch --scale 0.01 \
  --output ./results/

# Parse results in CI script
python -c "
import json
import sys

with open('results/tpch_duckdb_sf0.01_*.json') as f:
    results = json.load(f)

power = results['metrics']['power_at_size']
if power < 50:  # Performance threshold
    print(f'FAIL: Power@Size {power} below threshold 50')
    sys.exit(1)
print(f'PASS: Power@Size {power}')
"
```

## Related Documentation

- [Getting Started](../usage/getting-started.md)
- [Python API](python-api/results.md)
- [Understanding Results](../tutorials/understanding-results.md)
