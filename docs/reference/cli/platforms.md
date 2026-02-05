(cli-platforms)=
# `platforms` - Platform Management

```{tags} reference, cli, sql-platform
```

Manage database platform adapters, check availability, and configure platforms for use with BenchBox.

The `platforms` command group provides tools for:
- Discovering available database platforms
- Enabling/disabling platforms for benchmark execution
- Checking platform dependencies and installation status
- Getting installation guidance for missing dependencies
- Interactive platform setup wizard

## Subcommands

### `platforms list` - List Available Platforms

List all database platforms with their current status and availability.

**Options:**
- `--all`: Show all platforms including those with missing dependencies
- `--format [table|simple]`: Output format (default: table)

**Usage Examples:**

```bash
# List enabled and available platforms
benchbox platforms list

# Show all platforms including unavailable
benchbox platforms list --all

# Simple list format
benchbox platforms list --format simple
```

### `platforms status` - Show Platform Status

Display detailed status information for platforms, including library versions and configuration.

**Usage:**

```bash
# Show status for all platforms
benchbox platforms status

# Show detailed status for specific platform
benchbox platforms status duckdb
benchbox platforms status databricks
```

### `platforms enable` - Enable a Platform

Enable a platform for use in benchmark execution.

**Options:**
- `--force`: Enable platform even if dependencies are missing (not recommended)

**Usage Examples:**

```bash
# Enable a platform
benchbox platforms enable clickhouse

# Force enable (skip dependency check)
benchbox platforms enable snowflake --force
```

### `platforms disable` - Disable a Platform

Disable a platform to prevent its use in benchmarks.

**Usage:**

```bash
# Disable a platform
benchbox platforms disable sqlite
```

### `platforms install` - Installation Guide

Get step-by-step installation guidance for platform dependencies.

**Options:**
- `--dry-run`: Show installation commands without explaining

**Usage Examples:**

```bash
# Get installation guide for platform
benchbox platforms install clickhouse

# Show only installation commands
benchbox platforms install databricks --dry-run
```

### `platforms check` - Check Platform Availability

Check platform availability and configuration status. Useful for CI/CD validation.

**Options:**
- `--enabled-only`: Check only enabled platforms

**Usage Examples:**

```bash
# Check all platforms
benchbox platforms check

# Check only enabled platforms
benchbox platforms check --enabled-only

# Check specific platforms
benchbox platforms check duckdb databricks bigquery
```

**Exit Codes:**
- `0`: All checked platforms are ready
- `1`: One or more platforms have issues

### `platforms setup` - Interactive Setup Wizard

Launch an interactive wizard to configure platforms. Guides you through enabling, disabling, and installing platforms.

**Options:**
- `--interactive` / `--non-interactive`: Setup mode (default: interactive)

**Usage Examples:**

```bash
# Interactive platform setup
benchbox platforms setup

# Non-interactive: auto-enable all available platforms
benchbox platforms setup --non-interactive
```

## Common Workflows

### First-Time Setup

```bash
# 1. Check what's available
benchbox platforms list

# 2. Check detailed status
benchbox platforms status

# 3. Install missing dependencies
benchbox platforms install clickhouse

# 4. Enable platforms you want to use
benchbox platforms enable clickhouse
benchbox platforms enable duckdb

# 5. Verify everything is ready
benchbox platforms check --enabled-only
```

### Cloud Platform Setup

```bash
# 1. Enable the cloud platform
benchbox platforms enable databricks

# 2. Configure credentials (separate command)
benchbox setup --platform databricks

# 3. Verify availability
benchbox platforms status databricks
```

### Troubleshooting

```bash
# Check if platform dependencies are installed
benchbox platforms status <platform>

# Get installation guidance
benchbox platforms install <platform>

# Re-enable platform after installing dependencies
benchbox platforms enable <platform>

# Verify platform is working
benchbox platforms check <platform>
```

## Platform Categories

BenchBox supports platforms in several categories:

- **Analytical**: DuckDB, ClickHouse (columnar OLAP engines)
- **Cloud**: Databricks, Snowflake, BigQuery, Redshift (cloud warehouses)
- **Embedded**: SQLite (lightweight row-store database)

## Configuration

Platform configuration is stored in `~/.benchbox/platforms.yaml`:

```yaml
enabled:
  - duckdb
  - clickhouse
  - databricks
```

You can manually edit this file, but it's recommended to use the `platforms enable/disable` commands.

## Notes

- **Credentials vs. Availability**: The `platforms` commands manage platform *availability* (dependencies installed). For cloud platforms, you also need to configure *credentials* using `benchbox setup --platform <name>`.
- **Dependency Management**: Platform dependencies are installed via `pip` or `uv`. The `platforms install` command provides guidance but doesn't execute installations automatically.
- **Enabled by Default**: Most platforms are enabled by default if their dependencies are detected. You only need to explicitly enable platforms if you've previously disabled them.

## Related

- [Configuration](configuration.md) - Environment variables and platform authentication
- [Platform Documentation](../../platforms/index.md) - Detailed platform-specific guides
