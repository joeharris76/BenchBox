(cli-configuration)=
# Configuration

```{tags} reference, cli, configuration
```

BenchBox supports configuration through multiple sources with the following precedence:

1. **Command-line arguments** (highest priority)
2. **Environment variables**
3. **Configuration files**
4. **Default values** (lowest priority)

## Configuration File Format

BenchBox uses YAML configuration files. Default location: `~/.benchbox/config.yaml`

Example configuration:
```yaml
# Output settings
output:
  compression:
    enabled: true
    type: zstd
    level: 3
  formats:
    - json
    - csv

# Platform settings
platforms:
  databricks:
    enabled: true
    warehouse_id: "abc123"

  bigquery:
    enabled: true
    project_id: "my-project"

# Tuning settings
tuning:
  default_mode: notuning
  enable_constraints: false
```

## Platform Configuration

Each platform can have specific configuration options:

**Databricks:**
```yaml
platforms:
  databricks:
    warehouse_id: "warehouse-id"
    catalog: "main"
    schema: "benchbox"
```

**BigQuery:**
```yaml
platforms:
  bigquery:
    project_id: "my-project"
    dataset: "benchbox"
    location: "US"
```

**Snowflake:**
```yaml
platforms:
  snowflake:
    account: "account-name"
    warehouse: "COMPUTE_WH"
    database: "BENCHBOX"
    schema: "PUBLIC"
```

(cli-env-vars)=
## Environment Variables

BenchBox recognizes these environment variables:

### General Settings

- `BENCHBOX_NON_INTERACTIVE=true`: Enable non-interactive mode
- `BENCHBOX_NO_COMPRESSION=true`: Disable data compression
- `BENCHBOX_CONFIG_PATH=/path/to/config.yaml`: Custom config file location

### Platform Authentication

**Databricks:**
- `DATABRICKS_TOKEN`: Authentication token
- `DATABRICKS_HOST`: Workspace URL

**BigQuery:**
- `GOOGLE_APPLICATION_CREDENTIALS`: Service account key file path

**Snowflake:**
- `SNOWFLAKE_USER`: Username
- `SNOWFLAKE_PASSWORD`: Password
- `SNOWFLAKE_ACCOUNT`: Account identifier

**Redshift:**
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key

**ClickHouse:**
- `CLICKHOUSE_HOST`: Server hostname
- `CLICKHOUSE_USER`: Username
- `CLICKHOUSE_PASSWORD`: Password

(cli-platform-options)=
## Platform-Specific Options

Each platform supports specific options via `--platform-option KEY=VALUE`:

### Universal Keys (All Platforms)

These keys are available for every platform:

| Key | Description |
|-----|-------------|
| `driver_version` | Pin the Python driver package to a specific version (e.g. `1.2.0`). Useful for reproducing results or testing a specific connector release. |
| `driver_auto_install` | When `true`, automatically installs the requested `driver_version` via uv if the package is not already present. |

```{note}
`uv run benchbox run` syncs the environment to `uv.lock` before Python starts, which
can silently revert a version you installed manually. Use `driver_version` +
`driver_auto_install=true` or `uv run --with "pkg==X"` to reliably test a specific
version. See {ref}`driver-version-management` for the full guide.
```

Example: pin DuckDB driver and auto-install it:
```bash
benchbox run --platform duckdb --benchmark tpch \
  --platform-option driver_version=1.2.0 \
  --platform-option driver_auto_install=true
```

Example: pin Snowflake connector version:
```bash
benchbox run --platform snowflake --benchmark tpch \
  --platform-option driver_version=3.12.0 \
  --platform-option driver_auto_install=true \
  --platform-option account=xy12345.us-east-1 \
  --platform-option warehouse=COMPUTE_WH
```

### Athena Spark Engine Version

For Athena Spark only, the Spark engine version can be explicitly selected:

| Key | Description |
|-----|-------------|
| `engine_version` | Spark engine version string (e.g. `PySpark engine version 3`). Defaults to the workgroup's configured version. **Athena Spark only** - on other cloud platforms the engine version is auto-detected. |

```bash
benchbox run --platform athena-spark --benchmark tpch \
  --platform-option workgroup=my-spark-workgroup \
  --platform-option s3_staging_dir=s3://my-bucket/benchbox \
  --platform-option "engine_version=PySpark engine version 3"
```

### ClickHouse Options

- `mode=local`: Use local ClickHouse instance
- `secure=true`: Enable TLS encryption
- `port=9000`: Custom port number
- `database=default`: Target database name

Example:
```bash
benchbox run --platform clickhouse --benchmark tpch \
  --platform-option mode=local \
  --platform-option secure=true \
  --platform-option port=9440
```

### View Platform Details

Use `benchbox platforms status` to see platform information and capabilities:

```bash
benchbox platforms status clickhouse
benchbox platforms status databricks
```

## Related

- [Run Command](run.md) - Platform and tuning options during execution
- [Platforms Command](platforms.md) - Platform management and setup
- [Platform Documentation](../../platforms/index.md) - Detailed platform guides
