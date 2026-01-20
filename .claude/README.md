# BenchBox Claude Code Extensions

Custom slash commands and skills for BenchBox development workflows.

## Quick Reference

| Type            | Invocation          | Location            | Example                     |
| --------------- | ------------------- | ------------------- | --------------------------- |
| **Commands**    | Explicit `/command` | `.claude/commands/` | `/test-run tpch`            |
| **Skills**      | Natural language    | `.claude/skills/`   | "Test TPC-H"                |
| **MCP Prompts** | `/server:prompt`    | MCP server          | `/benchbox:analyze_results` |

## Slash Commands

Explicit invocation with `/command-name arguments`.

### TODO Management

| Command            | Description                      | Example                      |
| ------------------ | -------------------------------- | ---------------------------- |
| `/todo-create`     | Create new TODO item(s)          | `/todo-create Add feature X` |
| `/todo-implement`  | Implement a TODO item            | `/todo-implement item-slug`  |
| `/todo-complete`   | Mark item(s) complete            | `/todo-complete item-slug`   |
| `/todo-prioritize` | Review/adjust priorities         | `/todo-prioritize rebalance` |
| `/todo-review`     | Quality review of TODOs          | `/todo-review worktree-name` |
| `/todo-from-spec`  | Convert spec to TODOs            | `/todo-from-spec spec.md`    |
| `/todo-cleanup`    | Validate and commit TODO changes | `/todo-cleanup`              |

### Documentation

| Command             | Description                      | Example                                 |
| ------------------- | -------------------------------- | --------------------------------------- |
| `/docs-review`      | Review existing docs             | `/docs-review tpc-h`                    |
| `/docs-create`      | Create new docs page             | `/docs-create CLI Guide`                |
| `/docs-build`       | Build documentation              | `/docs-build serve`                     |
| `/docs-adversarial` | Critical docs review             | `/docs-adversarial user-journey:newbie` |
| `/docs-compare`     | Semantic doc comparison          | `/docs-compare v1.md v2.md`             |
| `/docs-shrink`      | Compress docs preserving meaning | `/docs-shrink command.md`               |

### Testing

| Command          | Description                | Example                         |
| ---------------- | -------------------------- | ------------------------------- |
| `/test-run`      | Run specific tests         | `/test-run tpch`                |
| `/test-fix`      | Fix failing test           | `/test-fix test_name`           |
| `/test-create`   | Create new tests           | `/test-create goal description` |
| `/test-coverage` | Add test coverage          | `/test-coverage module.py`      |
| `/test-perf`     | Fix slow tests             | `/test-perf test_name`          |
| `/test-cleanup`  | Commit modified test files | `/test-cleanup`                 |

### Code Quality

| Command         | Description                       | Example                     |
| --------------- | --------------------------------- | --------------------------- |
| `/code-fix`     | Fix code error                    | `/code-fix lint`            |
| `/code-review`  | Adversarial review                | `/code-review staged`       |
| `/code-perf`    | Performance analysis              | `/code-perf slow_function`  |
| `/code-to-spec` | Generate spec from code           | `/code-to-spec module/`     |
| `/code-compare` | Semantic code comparison          | `/code-compare v1.py v2.py` |
| `/code-shrink`  | Compress code preserving behavior | `/code-shrink utils.py`     |
| `/code-commit`  | Commit session changes            | `/code-commit`              |

### Blog Writing

| Command          | Description                | Example                            |
| ---------------- | -------------------------- | ---------------------------------- |
| `/blog-plan`     | Plan a new post or series  | `/blog-plan Cloud SSD economics`   |
| `/blog-research` | Research outline for post  | `/blog-research cloud-ssd --deep`  |
| `/blog-draft`    | Draft post from outline    | `/blog-draft cloud-ssd`            |
| `/blog-critique` | Critique and improve draft | `/blog-critique cloud-ssd --final` |
| `/blog-cleanup`  | Commit modified blog files | `/blog-cleanup`                    |

## Skills (Natural Language)

Skills are invoked automatically via natural language. Just describe what you want.

| Skill                        | Trigger Phrases                  | Purpose                             |
| ---------------------------- | -------------------------------- | ----------------------------------- |
| `benchmark-test`             | "Test TPC-H", "Run TPC-DS tests" | Run benchmark-specific tests        |
| `tpc-compliance-check`       | "Check TPC-H compliance"         | Validate against TPC specs          |
| `compare-implementations`    | "Compare TPC-H and TPC-DS"       | Find implementation inconsistencies |
| `quick-quality-check`        | "Run quality checks"             | Pre-commit lint/format/test         |
| `dialect-translation-test`   | "Test dialect translation"       | Check SQL across platforms          |
| `binary-wrapper-check`       | "Check TPC binaries"             | Validate TPC C binaries             |
| `architecture-review`        | "Review architecture"            | Check code patterns                 |
| `live-platform-test`         | "Test on Databricks"             | Run on cloud platforms              |
| `benchmark-plan-and-execute` | "Implement X feature"            | Full feature workflow               |
| `project-todo-sync`          | "What's on the TODO list?"       | Manage TODO workflow                |

## MCP Prompts

When the BenchBox MCP server is connected, these prompts are available:

| Prompt                           | Parameters                              | Purpose                      |
| -------------------------------- | --------------------------------------- | ---------------------------- |
| `/benchbox:analyze_results`      | benchmark, platform, focus              | Analyze benchmark results    |
| `/benchbox:compare_platforms`    | benchmark, platforms, scale_factor      | Compare platform performance |
| `/benchbox:identify_regressions` | baseline_run, comparison_run, threshold | Find regressions             |
| `/benchbox:benchmark_planning`   | use_case, platforms, time_budget        | Plan benchmark strategy      |
| `/benchbox:troubleshoot_failure` | error_message, platform, benchmark      | Diagnose failures            |

## When to Use What

| Situation                               | Use                                          |
| --------------------------------------- | -------------------------------------------- |
| Specific operation with known arguments | **Slash Command** (`/test-run tpch`)         |
| Exploratory or complex workflow         | **Skill** ("Help me implement X")            |
| Benchmark data analysis                 | **MCP Prompt** (`/benchbox:analyze_results`) |

## Creating Extensions

### New Command

1. Create `.claude/commands/{name}.md`
2. Add YAML frontmatter with `description`
3. Document instructions, arguments, output format

### New Skill

1. Create `.claude/skills/{name}.md`
2. Add YAML frontmatter with `description`
3. Write detailed instructions and examples

## Related Documentation

- `CLAUDE.md` - Quick reference for Claude Code
- `AGENTS.md` - Comprehensive project guidance
- `_project/TODO/` - Distributed TODO system
