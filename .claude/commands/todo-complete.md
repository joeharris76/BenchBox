---
description: Mark TODO item(s) as complete and move to DONE tree
---

# Complete TODO Item(s)

## Arguments

`$ARGUMENTS`: Path (`_project/TODO/worktree/phase/item.yaml`), slug/title, comma-separated list, or empty (show in-progress).

## Instructions

1. **Locate**: If path, verify exists. If slug: `uv run scripts/todo_cli.py list | grep -i "$ARGUMENTS"`. If empty: list in-progress.

2. **Confirm**: Show status, verify phases complete, check no blockers.

3. **Update**:
   - `status: "Completed"`
   - `completed_date: "YYYY-MM-DD"`
   - All tasks `done: true`
   - Optionally add notes

4. **Move**:
   ```bash
   mkdir -p _project/DONE/{worktree}/{phase}
   git mv {source} {destination}
   ```

5. **Index**: `uv run scripts/generate_indexes.py`

## Output Format

```markdown
## TODO Completed

### Item
- **Title**: {title}
- **From**: `{source}` | **To**: `{dest}`
- **Completed**: {date}

### Summary
- Phases: X/Y | Tasks: A/B

### Index
- TODO/DONE indexes updated

### Related
- Unblocked items: [list]
- Follow-up TODOs: [if needed]
```

## Example Usage

```
/todo-complete _project/TODO/platform-expansion/active/item.yaml
/todo-complete clickhouse adapter
/todo-complete item1, item2
/todo-complete
```
