---
description: Validate and commit uncommitted TODO/DONE changes
---

# TODO Cleanup

Validate uncommitted TODO/DONE items and commit.

## Arguments

`$ARGUMENTS`: Empty (full cleanup) or `--dry-run` (preview only).

## Instructions

1. **Run**: `uv run _project/scripts/todo_cli.py cleanup $ARGUMENTS`

2. **Handle Errors**:
   - `status: 'Done'` → `status: 'Completed'`
   - `commits`: hash only (e.g., `e03801e4`), not `e03801e4 - message`
   - `open_questions`: strings, not objects

3. **Fix**: Read failing files, fix violations, re-run.

4. **Verify**: `git log --oneline -1`

## Output Format

```markdown
## Cleanup Results

### Changes
- Modified: X | Deleted: Y | New: Z

### Validation
- [PASS/FAIL] Validated N items
- Errors: [list if any]

### Status
- Indexes regenerated
- Committed: `chore(todo): add X, update Y, remove Z`
```

## Example

```
/todo-cleanup
/todo-cleanup --dry-run
```
