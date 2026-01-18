# CLAUDE.md – Quick Reference

→ **See AGENTS.md for comprehensive project guidance.** This file provides Claude Code-specific shortcuts only.

## Quick Commands
- pytest: `uv run -- python -m pytest {test}`
- smoke: `uv run -- python -m pytest -m fast -q`
- format: `uv run ruff format .`
- lint: `uv run ruff check .`
- typecheck: `uv run ty check`

### CLI Quick Reference
```bash
# Basic usage
benchbox run --platform duckdb --benchmark tpch --scale 0.01

# Query subset (faster iteration)
benchbox run --platform duckdb --benchmark tpch --queries Q1,Q6,Q17

# Dry run (preview without execution)
benchbox run --dry-run ./preview --platform snowflake --benchmark tpcds

# Force data regeneration
benchbox run --platform duckdb --benchmark tpch --force datagen

# With compression (advanced)
benchbox run --platform duckdb --benchmark tpch --compression zstd:9

# Full validation (advanced)
benchbox run --platform duckdb --benchmark tpch --validation full

# DataFrame platforms
benchbox run --platform polars-df --benchmark tpch --scale 0.1
benchbox run --platform pandas-df --benchmark tpch --scale 0.1

# Help system
benchbox run --help            # Common options
benchbox run --help all        # All options including advanced
benchbox run --help examples   # Categorized usage examples
```

### CLI Options Summary
| Option | Description |
|--------|-------------|
| `--platform` | Platform (duckdb, sqlite, snowflake, databricks, etc.) |
| `--benchmark` | Benchmark (tpch, tpcds, ssb, clickbench) |
| `--scale` | Scale factor [default: 0.01] |
| `--phases` | Phases: generate,load,warmup,power,throughput,maintenance |
| `--queries` | Query subset (e.g., 'Q1,Q6,Q17') |
| `--tuning` | Tuning: tuned, notuning, auto, or YAML path |
| `--dry-run` | Preview without execution |
| `--force` | Force: all, datagen, upload |
| `--compression` | Compression: zstd, zstd:9, gzip:6, none (advanced) |
| `--validation` | Validation: exact, loose, range, disabled, full (advanced) |
| `--help [TOPIC]` | Help: --help, --help all, --help examples |

### --queries Flag Constraints
- **Max 100 queries** per run (DoS protection)
- **Alphanumeric IDs only** (letters, numbers, dash, underscore; max 20 chars)
- **Breaks TPC-H compliance** by overriding stream permutations
- **Phase compatibility**: only works with `power`/`standard` phases
- Valid ranges: TPC-H (1-22), TPC-DS (1-99), SSB (1-13)

## Pre-approved Commands (no permission needed)
**Dev/Test**: `make test-*`, `make coverage*`, `make lint`, `make format`, `make typecheck`, `uv run -- python -m pytest *`
**Files** (read-only): `ls*`, `find*`, `cat*`, `head*`, `tail*`, `wc*`, `file*`, `stat*`, `du*`, `tree*`, `which*`
**Git** (read-only): `git status`, `git diff*`, `git log*`, `git show*`, `git branch*`, `git remote*`, `git config --list`
**Python**: `uv tree`, `uv pip list`, `uv pip show*`, `uv export`, `uv run -- python -c*`, `uv run -- python -m*`
**TPC Binaries**: `timeout 30s _binaries/tpc-{h,ds}/<platform>/dsdgen*`, `timeout 60s _binaries/tpc-{h,ds}/<platform>/dsqgen*`
**TODO Management**: `uv run scripts/todo_cli.py list*`, `uv run scripts/todo_cli.py show*`, `uv run scripts/todo_cli.py stats`, `uv run scripts/validate_todo.py*`, `uv run scripts/generate_indexes.py`
**System** (read-only): `ps*`, `uname*`, `whoami`, `pwd`, `env | grep*`

## Package Management (Modern uv Commands)
- Add dependency: `uv add package_name`
- Add with extras: `uv add benchbox --extra cloud`
- Multiple extras: `uv add benchbox --extra cloud --extra clickhouse`
- Dev dependencies: `uv sync --group dev`
- Install standalone tool: `uv tool install tool_name`
- List packages: `uv tree` or `uv pip list`
- Export deps: `uv export`

## Project Layout
- `_binaries/tpc-{h,ds}/<os-arch>/` – prebuilt TPC tools; scale ≥1 required for TPC‑DS (fractional causes segfault)
- `_project/` – working files, experiments, research; `_trash/` subdirectory for discards
- `_project/TODO/` – distributed TODO items: `{worktree}/{phase}/{item}.yaml` with auto-generated indexes
- `_project/DONE/` – completed items (mirror structure of TODO/)
- `_project/scripts/` – TODO CLI (todo_cli.py), migration (migrate_todos.py), validation (validate_todo.py), indexing (generate_indexes.py)
- `.claude/skills/` – 10 custom workflows (benchmark‑test, quick‑quality‑check, tpc‑compliance‑check, compare‑implementations, binary‑wrapper‑check, dialect‑translation‑test, live‑platform‑test, architecture‑review, benchmark‑plan‑and‑execute, project‑todo‑sync); see `.claude/skills/README.md`
- `benchmark_runs/` – execution outputs, results/, manifests

## Key Constraints
- Line length: 120 chars; ruff for formatting
- Type hints required for public APIs
- Conventional Commits: `feat:`, `fix:`, `docs:`, `test:`
- CI must pass: ruff, typecheck, tests
- TPC‑DS scale ≥1 only (fractional SF causes binary segfault)
- Test markers: `fast`, `unit`, `integration`, `tpch`, `tpcds`; coverage ≥80%