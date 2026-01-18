---
description: Review and adjust priority of TODO items
---

# Prioritize TODO Items

Adjust priority levels based on impact, effort, dependencies.

## Arguments

`$ARGUMENTS`: Empty (all items), worktree name, item path, or `rebalance`.

## Instructions

1. **Load State**:
   ```bash
   uv run scripts/todo_cli.py stats
   uv run scripts/todo_cli.py list --priority=critical
   ```
   Read `_project/TODO/_indexes/by-priority.yaml`

2. **Analyze Distribution**: Ideal: Critical 0-2, High 3-5, Medium-High 5-10. Check inflation, stale items.

3. **Evaluate**: Business impact, dependencies, effort, time sensitivity, delay risk.

4. **Recommend**: Items to promote/demote, blocked items, stale items.

5. **Apply**: Update priority/last_updated, regenerate indexes.

## Output Format

```markdown
## Priority Analysis

### Distribution
| Priority | Count | Ideal | Status |
|----------|-------|-------|--------|
| Critical | X | 0-2 | OK/HIGH |
| High | X | 3-5 | OK/HIGH |

### Issues
| Severity | Item | Current | Recommended | Reason |
|----------|------|---------|-------------|--------|
| Warning | slug | Critical | High | No activity 30+ days |

### Recommendations
1. Demote stale Critical items
2. Promote blockers
```

## Priority Guidelines

| Priority | When |
|----------|------|
| Critical | Active incident, blocking release, security |
| High | Current sprint, high impact |
| Medium-High | Next sprint candidates |
| Medium | Backlog with value |
| Low | Nice-to-have, exploratory |

## Example Usage

```
/todo-prioritize
/todo-prioritize platform-expansion
/todo-prioritize rebalance
```
