---
description: Create new TODO item(s) in the distributed TODO system
---

# Create TODO Item(s)

Create TODO items in {project} distributed TODO structure.

## Arguments

`$ARGUMENTS`: Title/description, comma-separated titles, `from conversation`, or empty (interactive).

## Instructions

1. **Parse**: Empty → ask for title. "from conversation" → scan messages. Otherwise parse as titles.

2. **Gather Info**:
   - Title (required): 5-200 chars
   - Worktree (required): Git branch (lowercase-with-hyphens)
   - Priority: Critical | High | Medium-High | Medium | Low
   - Category: Core Functionality | Platform Expansion | Testing | Documentation
   - Description: Multi-line explanation

3. **Location**: `_project/TODO/{worktree}/{phase}/{slug}.yaml` - Phase: `planning/` (Not Started) or `active/` (In Progress)

4. **Generate**: Use `_project/TODO_ENTRY_TEMPLATE.yaml`, fill required fields, set created_date.

5. **Create**: Verify directory, write YAML (2-space indent).

6. **Validate**: `uv run scripts/validate_todo.py <path>`

7. **Index**: `uv run scripts/generate_indexes.py`

## Output Format

```markdown
## TODO Created

### Details
- **Title**: {title} | **File**: `{path}`
- **Priority**: {priority} | **Worktree**: {worktree}

### Validation
- Schema: PASSED | Indexes: COMPLETED

### Next Steps
- `/todo-prioritize` to adjust
- `/todo-review` to verify
```

## Example Usage

```
/todo-create Add ClickHouse adapter support
/todo-create Implement benchmark, Add streaming support
/todo-create from conversation
```
