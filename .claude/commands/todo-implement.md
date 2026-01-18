# Implement TODO Item

Implement a TODO item from the distributed TODO system, writing production-quality code.

## Arguments

`$ARGUMENTS`: TODO slug (`platform-setup-numbered-list`), full path to YAML, or `list` to show available items.

## Instructions

### Phase 1: Load TODO

1. **Find TODO**: If `list`/empty: `uv run python _project/scripts/todo_cli.py list --status "Not Started" --status "In Progress"`. Otherwise: `find _project/TODO -name "*$ARGUMENTS*" -type f -name "*.yaml"`
2. **Read TODO file**: Extract title, description, tasks/phases, files affected, success metrics, constraints.
3. **Update status**: Set "In Progress" if "Not Started". Move from `planning/` to `active/` if needed.

### Phase 2: Plan

1. **Analyze codebase**: Read `files_affected.modified`, understand patterns.
2. **Create plan**: Break into steps from TODO phases, identify test files, note dependencies.
3. **Use TodoWrite** to track progress.

### Phase 3: Implement

For each phase/task:
1. **Implement** production-quality code following existing patterns
2. **Test**: `uv run python -m pytest <relevant_tests> -v`
3. **Fix failures** before proceeding
4. **Commit**: `git commit -m "<type>(<scope>): <description>"` (feat/fix/refactor/test/docs)
5. **Mark done** in TODO YAML (`done: true`)

### Phase 4: Finalize

1. **Full tests**: `uv run python -m pytest <all_relevant_tests> -v`
2. **Lint/format**: `uv run ruff check && uv run ruff format <files>`
3. **Verify** all success metrics
4. **Move to DONE**: Add `completed_date: "YYYY-MM-DD"` and summary, then `mv _project/TODO/<path> _project/DONE/<path>`
5. **Regenerate indexes**: `uv run python _project/scripts/generate_indexes.py`
6. **Final commit**: `git commit -m "chore: complete TODO - <title>"`

## Critical Requirements

1. **Work until complete** - No stopping mid-task
2. **Test after every change** - Don't proceed if tests fail
3. **Commit incrementally** - After each phase/major task
4. **Unlimited context** - Complete entire TODO this session
5. **Quality over speed** - Prioritize quality, simplicity, reliability
6. **No backward compatibility** - Pre-release, remove old code
7. **Production-quality only** - NO placeholders, stubs, TODO comments, pass statements, incomplete error handling
8. **Move to DONE** when complete

## Output Format

**Progress**:
```markdown
## Implementing: <Title>
### Current Phase: <Name>
- [x] Task 1 | - [ ] Task 2 - in progress
### Recent: Implemented X, Tests ✅, Committed: `feat(scope): desc`
```

**Completion**:
```markdown
## TODO Completed: <Title>
### Summary: <description>
### Files: `path/file.py` - <what changed>
### Tests: All passing ✅, New: <list>
### Commits: `abc1234` feat(scope): description
### Status: Moved to `_project/DONE/<path>`
```

## Example Usage

```
/todo-implement platform-setup-numbered-list
/todo-implement _project/TODO/ux-improvements/planning/item.yaml
/todo-implement list
```
