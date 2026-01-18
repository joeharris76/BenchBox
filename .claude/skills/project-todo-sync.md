---
description: Manage distributed TODO/DONE items in new structure
---

# Project TODO Sync

Manage BenchBox project TODOs using the distributed file structure in `_project/TODO/` and `_project/DONE/`.

## Structure Overview

```
_project/
├── TODO/
│   ├── _indexes/              # Auto-generated indexes
│   │   ├── master.yaml        # Complete listing
│   │   ├── by-category.yaml   # Grouped by category
│   │   ├── by-priority.yaml   # Grouped by priority
│   │   └── by-status.yaml     # Grouped by status
│   ├── {worktree}/            # Git branch/worktree
│   │   ├── planning/          # Not Started, Identified
│   │   │   └── {item-slug}.yaml
│   │   └── active/            # In Progress, Blocked, Under Review
│   │       └── {item-slug}.yaml
│   └── README.md
├── DONE/                      # Same structure for completed items
└── scripts/
    ├── todo_cli.py            # CLI for querying/management
    ├── validate_todo.py       # Schema validation
    └── generate_indexes.py    # Index regeneration
```

## Instructions

When the user asks to manage TODOs, update project status, or mark items complete:

### 1. Query Items Using CLI or Indexes

**List items with filters:**
```bash
uv run scripts/todo_cli.py list --priority=high
uv run scripts/todo_cli.py list --status="in-progress"
uv run scripts/todo_cli.py list --worktree=platform-expansion
uv run scripts/todo_cli.py list --done  # Show DONE items
```

**Show specific item:**
```bash
uv run scripts/todo_cli.py show _project/TODO/{worktree}/{phase}/{item}.yaml
```

**View statistics:**
```bash
uv run scripts/todo_cli.py stats
```

**Read indexes directly:**
- `_project/TODO/_indexes/master.yaml` - All items with metadata
- `_project/TODO/_indexes/by-priority.yaml` - Grouped by priority
- Use Read tool to examine specific index files

### 2. TODO Entry Format

Each item file follows this structure (see `_project/TODO_ENTRY_TEMPLATE.yaml` for full template):

```yaml
title: "Descriptive title of the TODO item"
worktree: "git-branch-name"
priority: "Critical|High|Medium-High|Medium|Low"
status: "Not Started|Identified|In Progress|Blocked|Under Review|Completed"
description: |
  Multi-line detailed explanation

category: "Core Functionality"

tasks:
  phases:
    - phase: "Phase 1: Foundation"
      done: false
      items:
        - summary: "Task 1 description"
          done: false
        - summary: "Task 2 description"
          done: false

metadata:
  owners: ["github-username"]
  tags: ["tag1", "tag2"]
  estimated_effort: "2-3 days"
  due_date: "YYYY-MM-DD"

impact:
  business_value: "Why this matters"
  user_impact: "How users benefit"

files_affected:
  modified: ["path/to/file1.py"]
  added: ["path/to/file2.py"]
```

### 3. Create New TODO Items

**Steps:**
1. Determine worktree (git branch) and lifecycle phase (planning vs active)
2. Create slugified filename from title (lowercase, hyphens, max 80 chars)
3. Write file to: `_project/TODO/{worktree}/{phase}/{slug}.yaml`
4. Validate: `uv run scripts/validate_todo.py <file>`
5. Regenerate indexes: `uv run scripts/generate_indexes.py`

**Example:**
```yaml
# _project/TODO/platform-expansion/planning/clickhouse-adapter.yaml
title: "Implement ClickHouse Adapter"
worktree: "platform-expansion"
priority: "High"
status: "Not Started"
description: |
  Add ClickHouse platform adapter to support benchmarking against ClickHouse.

category: "Platform Expansion"

tasks:
  phases:
    - phase: "Phase 1: Research"
      done: false
      items:
        - summary: "Study ClickHouse client libraries"
          done: false
```

### 4. Update Existing Items

**To update an item:**
1. Find item using CLI or indexes
2. Read the file: `cat _project/TODO/{worktree}/{phase}/{slug}.yaml`
3. Use Edit tool to modify fields (title, status, tasks, etc.)
4. Update `metadata.last_updated` to today's date
5. Validate: `uv run scripts/validate_todo.py <file>`
6. Regenerate indexes: `uv run scripts/generate_indexes.py`

**To move between phases (planning → active):**
1. Read item file
2. Update `status` field (e.g., "Not Started" → "In Progress")
3. Move file from `planning/` to `active/` directory:
   ```bash
   mv _project/TODO/{worktree}/planning/{slug}.yaml \
      _project/TODO/{worktree}/active/{slug}.yaml
   ```
4. Regenerate indexes

### 5. Mark Items Complete

**Steps:**
1. Read item from TODO tree
2. Add `completed_date: "YYYY-MM-DD"` field
3. Set `status: "Completed"`
4. Optionally add `sections` with completion notes
5. Move to DONE tree (mirror directory structure):
   ```bash
   # Create DONE directory if needed
   mkdir -p _project/DONE/{worktree}/active

   # Move file
   mv _project/TODO/{worktree}/active/{slug}.yaml \
      _project/DONE/{worktree}/active/{slug}.yaml
   ```
6. Regenerate indexes for both trees: `uv run scripts/generate_indexes.py`

### 6. After Any Changes

**Always regenerate indexes after modifying TODO items:**
```bash
uv run scripts/generate_indexes.py
```

This updates master indexes and grouped indexes for quick navigation.

## Validation

**Validate single file:**
```bash
uv run scripts/validate_todo.py _project/TODO/{worktree}/{phase}/{slug}.yaml
```

**Validate all items:**
```bash
uv run scripts/validate_todo.py --all
```

## Notes

- YAML format is strict - maintain proper indentation (2 spaces)
- Use TODO_ENTRY_TEMPLATE.yaml as reference for structure
- Indexes are auto-generated - don't edit manually
- Schema validation ensures data integrity
- File slugs must be filesystem-safe (lowercase, hyphens, alphanumeric)
- Lifecycle phases: `planning/` for not-started/identified, `active/` for in-progress/blocked

## Schema Reference

See `_project/TODO_SCHEMA.yaml` for complete field definitions and validation rules.
