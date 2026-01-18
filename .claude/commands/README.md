# BenchBox Slash Commands

Explicit invocation commands for BenchBox development workflows.

## Usage

Invoke commands with `/command-name arguments`:

```
/todo-create Add ClickHouse adapter support
/test-run tpch
/docs-review CLI usage
/code-review benchbox/core/tpch/
/blog-draft cloud-ssd-economics
```

## Available Commands

### TODO Management

| Command | Description | Example |
|---------|-------------|---------|
| `/todo-create` | Create new TODO item(s) | `/todo-create Add feature X` |
| `/todo-implement` | Implement a TODO item | `/todo-implement item-slug` |
| `/todo-complete` | Mark item(s) complete | `/todo-complete item-slug` |
| `/todo-prioritize` | Review/adjust priorities | `/todo-prioritize rebalance` |
| `/todo-review` | Quality review of TODOs | `/todo-review worktree-name` |
| `/todo-from-spec` | Convert spec to TODOs | `/todo-from-spec spec.md` |

### Documentation

| Command | Description | Example |
|---------|-------------|---------|
| `/docs-review` | Review existing docs | `/docs-review tpc-h` |
| `/docs-create` | Create new docs page | `/docs-create CLI Guide` |
| `/docs-build` | Build documentation | `/docs-build serve` |
| `/docs-adversarial` | Critical docs review | `/docs-adversarial user-journey:newbie` |
| `/docs-compare` | Semantic doc comparison | `/docs-compare v1.md v2.md` |
| `/docs-shrink` | Compress docs preserving meaning | `/docs-shrink command.md` |

### Testing

| Command | Description | Example |
|---------|-------------|---------|
| `/test-run` | Run specific tests | `/test-run tpch` |
| `/test-fix` | Fix failing test | `/test-fix test_name` |
| `/test-coverage` | Add test coverage | `/test-coverage module.py` |

### Code Quality

| Command | Description | Example |
|---------|-------------|---------|
| `/code-fix` | Fix code error | `/code-fix lint` |
| `/code-review` | Adversarial review | `/code-review staged` |
| `/code-perf` | Performance analysis | `/code-perf slow_function` |
| `/code-to-spec` | Generate spec from code | `/code-to-spec module/` |
| `/code-compare` | Semantic code comparison | `/code-compare v1.py v2.py` |
| `/code-shrink` | Compress code preserving behavior | `/code-shrink utils.py` |
| `/code-commit` | Commit session changes | `/code-commit` |

### Blog Writing

| Command | Description | Example |
|---------|-------------|---------|
| `/blog-plan` | Plan a new post or series | `/blog-plan Cloud SSD economics` |
| `/blog-research` | Research outline for post | `/blog-research cloud-ssd --deep` |
| `/blog-draft` | Draft post from outline | `/blog-draft cloud-ssd` |
| `/blog-critique` | Critique and improve draft | `/blog-critique cloud-ssd --final` |

## Output Format

All review/adversarial commands produce structured reports with:

- **Summary**: Brief overview of what was analyzed
- **Issues Found**: Table with severity, location, issue, recommendation
- **Recommendations**: Prioritized (HIGH/MEDIUM/LOW) action items
- **Risk Assessment**: Impact, effort, urgency evaluation
- **Action Items**: Concrete checklist of next steps
- **Next Steps**: Guidance on what to do after the command

## Integration

Commands integrate with existing BenchBox tooling:

- **TODO CLI**: `uv run scripts/todo_cli.py`
- **Validation**: `uv run scripts/validate_todo.py`
- **Indexing**: `uv run scripts/generate_indexes.py`
- **Makefile**: `make test-*`, `make docs-*`, `make lint`, etc.
- **Blog**: `_blog/` directory with style guide and series structure

## Differences from Skills

| Aspect | Skills | Slash Commands |
|--------|--------|----------------|
| Invocation | Implicit (AI-detected) | Explicit (`/command`) |
| Arguments | Context-based | Explicit `$ARGUMENTS` |
| Output | Variable | Structured reports |
| Purpose | Workflows | Specific operations |

Skills in `.claude/skills/` remain available for natural language invocation. Commands provide predictable, explicit operations.

## Creating New Commands

1. Create `.claude/commands/{name}.md`
2. Add YAML frontmatter with `description`
3. Document instructions, arguments, output format
4. Update this README

See existing commands for patterns and examples.
