---
description: Convert specification file into TODO items
---

# Create TODOs from Specification

Parse spec file and generate TODO items.

## Arguments

`$ARGUMENTS`: Path to spec (md/yaml/txt), `--worktree=name`, `--priority=level`, `--dry-run`

## Instructions

1. **Read Spec**: Locate file, determine format, parse structure.

2. **Extract Items**:
   - **Markdown**: H2/H3 → titles, bullets → tasks, code blocks → files_affected
   - **YAML**: Top keys → items, nested → phases, arrays → tasks
   - **Text**: Top bullets → items, nested → tasks, keywords: "implement", "add", "fix"

3. **Metadata**: Worktree from arg or derive, category from content, priority from arg or Medium, status "Not Started".

4. **Generate**: Create YAML per item, slugified filename, link related items, add source reference, set created_date.

5. **Preview**: If `--dry-run`, show without writing.

6. **Create**: Write to `_project/TODO/{worktree}/planning/{slug}.yaml`

7. **Validate**: `uv run scripts/validate_todo.py _project/TODO/{worktree}/`

8. **Index**: `uv run scripts/generate_indexes.py`

## Output Format

```markdown
## TODOs Created

### Source
- **File**: {path} | **Format**: {type} | **Items**: X

### Items
| # | Title | File | Priority | Tasks |
|---|-------|------|----------|-------|

### Relationships
- Items 1-3: Sequential
- Item 4: Parallel

### Validation
- Schema: PASSED | Indexes: YES

### Next Steps
- `/todo-review` to verify quality
- `/todo-prioritize` to set priorities
```

## Parsing Example

**Input**:
```markdown
## Add ClickHouse Support
- Research client libraries
- Implement adapter
- Add tests
```

**Output**:
```yaml
title: "Add ClickHouse Support"
worktree: "platform-expansion"
priority: "Medium"
status: "Not Started"
tasks:
  phases:
    - phase: "Implementation"
      items:
        - summary: "Research client libraries"
        - summary: "Implement adapter"
```

## Example Usage

```
/todo-from-spec docs/design/feature.md --worktree=feature-branch
/todo-from-spec requirements.md --dry-run
```
