# BenchBox – Agent And Contributor Guide

- Purpose: consistent tooling, safe defaults, minimal surprises for contributors, agents, and CI.

## Tooling
- Runners: prefer `uv`; fallbacks: `make` → `uv run --` → `python -m`.
- Search: use `rg`; read files in ≤ 250‑line chunks.
- CLI: `benchbox --help` after dev install.

## Code Style
- Python 3.10+, 4 spaces, 120 cols.
- Ruff-only: `uv run ruff format .` and `uv run ruff check .`.
- Public APIs require type hints.

## Tests
- Markers: `fast`, `unit`, `integration`, `tpch`, `tpcds`; coverage ≥ 80%.
- Smoke: `uv run -- python -m pytest -m fast -q`.
- Standards: `uv run -- python -m pytest -m "tpch or tpcds" --tb=short`.
- Coverage: `make coverage`. Avoid `live_*` without approval.

## Sandbox & Approvals
- FS: workspace‑write; Network: restricted; Approvals: on‑request.
- Pre‑approved: repo reads (`rg`), tests (`uv run -- python -m pytest ...`).
- Needs approval: network, installs, writes outside workspace, live cloud tests.

## Security
- No credentials in repo; use env vars or `.env` (ignored). Redact secrets in logs.

## CLI Modes & Phases
- Non‑interactive: `--non-interactive` (or `BENCHBOX_NON_INTERACTIVE=true`) + `--platform --benchmark --scale --phases`.
- Dry‑run: `benchbox run --dry-run <DIR> --platform <plat> --benchmark <bm> --scale <sf> [--phases ...]`.
- Phases: `generate` (data), `load` (tables), `power|throughput|maintenance` (queries). Always propagate `--phases`; default generate+load+execute.
- Seeds: `--seed <int>` for deterministic power/throughput.
- Scale: TPC SF ≥ 1 must be whole integers; default SF=0.01 for smoke.

## DataFrame Platforms
BenchBox supports benchmarking DataFrame libraries using native APIs instead of SQL.

### Available DataFrame Platforms
| Platform | CLI Name | Family | Status |
|----------|----------|--------|--------|
| Polars | `polars-df` | Expression | Production-ready |
| Pandas | `pandas-df` | Pandas | Production-ready |

### DataFrame CLI Commands
- Polars DataFrame: `benchbox run --platform polars-df --benchmark tpch --scale 0.01`
- Pandas DataFrame: `benchbox run --platform pandas-df --benchmark tpch --scale 0.01`
- With options: `benchbox run --platform polars-df --benchmark tpch --scale 1 --platform-option streaming=true`

### DataFrame Platform Options
- `polars-df`: `streaming` (bool), `rechunk` (bool), `n_rows` (int)
- `pandas-df`: `dtype_backend` (numpy|numpy_nullable|pyarrow)

### Installation
- Polars: core dependency (already installed)
- Pandas: `uv add benchbox --extra dataframe-pandas`
- All DataFrame platforms: `uv add benchbox --extra dataframe-all`

## Data & Binaries
- Prebuilt TPC binaries under `_binaries/tpc-{h,ds}/<os-arch>/`; avoid compiling in CI.
- Output roots: `benchmark_runs/`; manifests enable reuse. Normalize cloud `--output` paths.
- Compression: default `zstd`; validate availability; fallback to `none`.

## Validation & Reporting
- Toggles: preflight, postgen‑manifest, postload.
- Prefer comprehensive CLI summaries (query breakdowns, top failures, recommendations).
- Artifacts: export JSON to `benchmark_runs/results/` with execution ID + timestamp.

## Logging
- Respect `--quiet`; use `-v/-vv` for diagnostics. Don’t change third‑party logger levels globally.

## Preferences
- Quick restart: `~/.benchbox/last_run.yaml` with actual database type, phases, scale, tuning mode.

## Commits & PRs
- Commits: Conventional Commits (`feat:`, `fix:`, ...).
- PRs: link issues, include tests and docs/examples when applicable.
- CI must pass: ruff, typecheck, tests (`make test-ci`).

## Planning & TODOs
- Distributed structure: `_project/TODO/{worktree}/{phase}/{item}.yaml` and `_project/DONE/` (mirror structure)
- CLI tools: `uv run scripts/todo_cli.py list|show|stats|validate|reindex`
- Indexes: Auto-generated in `_project/{TODO|DONE}/_indexes/` (master, by-category, by-priority, by-status)
- Template: `_project/TODO_ENTRY_TEMPLATE.yaml` - comprehensive format with phases, tasks, metadata
- Schema: `_project/TODO_SCHEMA.yaml` - JSON Schema for validation
- After changes: regenerate indexes with `uv run scripts/generate_indexes.py`
- See `.claude/skills/project-todo-sync.md` for detailed workflow

## Skills (.claude/skills)
- Common workflows: benchmark‑test, quick‑quality‑check, tpc‑compliance‑check, compare‑implementations, binary‑wrapper‑check, dialect‑translation‑test, live‑platform‑test, architecture‑review, benchmark‑plan‑and‑execute.
- Skills should auto‑detect runner, honor non‑interactive, output human + JSON summaries.

## Agent Workflow
- Plans: one step `in_progress`.
- Preambles: short, grouped before commands.
- Patches: surgical; update adjacent docs/tests only if directly impacted.

## Recipes
- TPC‑H smoke: `benchbox run --platform duckdb --benchmark tpch --scale 0.01 --phases power --non-interactive`.
- TPC‑DS data only: `benchbox run --benchmark tpcds --scale 0.1 --phases generate --output ./tpcds_sf01 --non-interactive`.
- Dry‑run preview: `benchbox run --platform duckdb --benchmark tpch --scale 0.1 --dry-run ./preview --phases power --seed 3`.
- DataFrame smoke (Polars): `benchbox run --platform polars-df --benchmark tpch --scale 0.01 --non-interactive`.
- DataFrame smoke (Pandas): `benchbox run --platform pandas-df --benchmark tpch --scale 0.01 --non-interactive`.

## Structure Reference
- `benchbox/` (core & CLI), `tests/`, `examples/`, `docs/`, configs (`pyproject.toml`, `pytest.ini`, `Makefile`), outputs in `benchmark_runs/`.
