---
description: Adversarial review of TODO content quality and completeness
---

# Review TODO Quality

`$ARGUMENTS`: Item path, worktree name, `all`, or `high-priority` (default if empty).

## Instructions

1. **Select**: Path → file, worktree → `uv run scripts/todo_cli.py list --worktree={name}`, "all" → master.yaml, empty → high-priority.

2. **Score (0-3 each)**:
   - **Clarity**: 3=specific title+why, 2=clear but vague, 1=ambiguous, 0=unclear
   - **Completeness**: 3=all fields+tasks+deps, 2=required, 1=minimal, 0=stub
   - **Actionability**: 3=clear first step+effort+files, 2=actionable, 1=vague, 0=no path
   - **Freshness**: 3=recent+accurate, 2=stale, 1=outdated, 0=abandoned

3. **Identify Issues**: Missing fields, vague descriptions, unrealistic estimates, missing deps, stale status.

4. **Suggest Improvements**: Rewrites, missing info, breakdown tasks.

5. **Validate**: `uv run scripts/validate_todo.py <path>`

## Output Format

```markdown
## TODO Quality Review

### Summary
Reviewed X items. Average: Y/12.

### Issues
| Severity | Item | Issue | Fix |
|----------|------|-------|-----|
| Critical | slug | Missing description | Add context |

### Scores
| Item | Clarity | Complete | Action | Fresh | Total | Grade |
|------|---------|----------|--------|-------|-------|-------|
| item-1 | 3 | 2 | 3 | 2 | 10/12 | Good |

### Grades
- **Excellent (11-12)**: X
- **Good (9-10)**: Y
- **Needs Work (6-8)**: Z
- **Poor (0-5)**: W
```

## Scoring Guide

| Score | Meaning |
|-------|---------|
| 11-12 | Excellent - Ready to work |
| 9-10 | Good - Minor improvements |
| 6-8 | Needs Work - Improve first |
| 0-5 | Poor - Significant rework |

## Example Usage

```
/todo-review _project/TODO/platform-expansion/active/item.yaml
/todo-review platform-expansion
/todo-review high-priority
```
