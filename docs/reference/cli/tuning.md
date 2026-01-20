# Tuning Commands

```{tags} reference, cli, tuning
```

Commands for creating and managing tuning configurations for SQL and DataFrame platforms.

(cli-tuning-init)=
## `tuning init` - Create Tuning Configuration

Generate sample unified tuning configurations for specific platforms.

```{note}
The `create-sample-tuning` command is deprecated. Use `tuning init` instead.
```

### Options

- `--platform TEXT`: Target platform (required) - duckdb, databricks, snowflake, etc.
- `--output TEXT`: Output file path (default: tuning_config.yaml)

### Usage Examples

```bash
# Create sample tuning for Databricks
benchbox tuning init --platform databricks

# Create with custom output path
benchbox tuning init --platform snowflake \
  --output ./configs/snowflake-tuning.yaml
```

(cli-df-tuning)=
## `df-tuning` - DataFrame Tuning Configuration

Manage tuning configurations for DataFrame platforms. Create sample configurations, validate existing configs, and view smart defaults for your system.

### Subcommands

#### `df-tuning create-sample` - Create Sample Configuration

Generate a sample tuning configuration file for a specific DataFrame platform.

**Options:**
- `--platform TEXT`: Target DataFrame platform (required): `polars`, `pandas`, `dask`, `modin`, `cudf`
- `--output TEXT`: Output file path (default: `{platform}_tuning.yaml`)
- `--smart-defaults`: Include auto-detected system-optimal settings

**Usage Examples:**

```bash
# Create sample Polars tuning config
benchbox df-tuning create-sample --platform polars

# Create with smart defaults based on your system
benchbox df-tuning create-sample --platform polars --smart-defaults

# Custom output path
benchbox df-tuning create-sample --platform pandas --output ./configs/pandas_tuning.yaml
```

#### `df-tuning validate` - Validate Configuration

Validate a DataFrame tuning configuration file for errors and warnings.

**Options:**
- `CONFIG_FILE`: Path to configuration file to validate (required)
- `--platform TEXT`: Target platform for validation (auto-detected from config if not specified)

**Usage Examples:**

```bash
# Validate a configuration file
benchbox df-tuning validate polars_tuning.yaml

# Validate with explicit platform
benchbox df-tuning validate my_config.yaml --platform polars
```

#### `df-tuning show-defaults` - Show Smart Defaults

Display recommended tuning settings based on your system profile.

**Options:**
- `--platform TEXT`: Target DataFrame platform (required)

**Usage Examples:**

```bash
# Show defaults for Polars
benchbox df-tuning show-defaults --platform polars

# Show defaults for Dask
benchbox df-tuning show-defaults --platform dask
```

#### `df-tuning list-platforms` - List Supported Platforms

List all DataFrame platforms that support tuning configuration.

**Usage:**

```bash
benchbox df-tuning list-platforms
```

## Configuration Categories

DataFrame tuning supports these configuration sections:

| Category | Settings | Description |
|----------|----------|-------------|
| `parallelism` | `thread_count`, `worker_count`, `threads_per_worker` | CPU resource allocation |
| `memory` | `memory_limit`, `chunk_size`, `spill_to_disk`, `rechunk_after_filter` | Memory management |
| `execution` | `streaming_mode`, `lazy_evaluation`, `engine_affinity` | Execution behavior |
| `data_types` | `dtype_backend`, `enable_string_cache`, `auto_categorize_strings` | Type handling |
| `io` | `memory_pool`, `memory_map`, `pre_buffer`, `row_group_size` | I/O optimization |
| `gpu` | `enabled`, `device_id`, `spill_to_host`, `pool_type` | GPU settings (cuDF) |

For complete configuration reference, see [DataFrame Platforms - Tuning](../../platforms/dataframe.md#dataframe-tuning).

## Related

- [Run Command](run.md) - Using `--tuning` option during benchmark execution
- [DataFrame Platforms](../../platforms/dataframe.md) - DataFrame platform documentation
