# Utility Commands

```{tags} reference, cli
```

This page covers smaller utility commands for dependency checking, system profiling, benchmark discovery, and configuration validation.

(cli-check-deps)=
## `check-deps` - Check Dependencies

Check dependency status and provide installation guidance for different platforms.

### Options

- `--platform TEXT`: Check dependencies for specific platform
- `--verbose`, `-v`: Show detailed dependency information
- `--matrix`: Show installation matrix and exit

### Usage Examples

```bash
# Overview of all platform dependencies
benchbox check-deps

# Check specific platform
benchbox check-deps --platform databricks

# Show detailed installation matrix
benchbox check-deps --matrix

# Verbose output with recommendations
benchbox check-deps --verbose
```

(cli-profile)=
## `profile` - System Profiling

Profile the current system to understand hardware capabilities and provide recommendations.

### Usage

```bash
benchbox profile
```

This command analyzes:
- CPU cores and architecture
- Memory capacity and availability
- Disk space
- Operating system details
- Python environment

Provides recommendations for:
- Appropriate scale factors
- Concurrency settings
- Platform selection

(cli-benchmarks)=
## `benchmarks` - Manage Benchmark Suites

Manage and browse available benchmark suites.

### Subcommands

#### `benchmarks list`

Display all available benchmark suites with descriptions.

```bash
benchbox benchmarks list
```

Shows information about:
- TPC-H, TPC-DS, TPC-DI (official TPC benchmarks)
- ClickBench, H2ODB (industry benchmarks)
- SSB, AMPLab (academic benchmarks)
- ReadPrimitives, WritePrimitives, TPC-Havoc (testing benchmarks)

(cli-validate)=
## `validate` - Validate Configuration

Validate BenchBox configuration files for syntax and completeness.

### Options

- `--config TEXT`: Configuration file path (optional)

### Usage Examples

```bash
# Validate default configuration
benchbox validate

# Validate specific configuration file
benchbox validate --config ./custom-config.yaml
```

## Related

- [Configuration](configuration.md) - Configuration file format and options
- [Platforms](platforms.md) - Platform management commands
