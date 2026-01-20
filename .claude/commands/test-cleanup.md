---
description: Create a cleanup commit for all modified test files
---

# Test Cleanup

Create a commit for all modified test files with a descriptive message.

## Arguments

`$ARGUMENTS`: Not used.

## Instructions

1. **Find modified test files**:

   ```bash
   git status --porcelain tests/
   ```

2. **If no files**, inform user and exit:

   > No modified test files found. Nothing to commit.

3. **Analyze changes**:

   - Run `git diff tests/` for modified files
   - Read new test files to understand coverage additions
   - Categorize by area (unit, integration, performance) and type (new tests, fixes, updates)
   - Count additions, modifications, and deletions

4. **Generate commit message**:

   Format: `test: <summary>`

   Examples:
   - `test: add coverage for cloud storage integration`
   - `test: fix failing platform adapter tests`
   - `test: add performance baseline tests, update existing coverage`
   - `test: update mocking strategy for influxdb adapter`

5. **Stage and commit**:

   ```bash
   git add tests/
   git commit -m "$(cat <<'EOF'
   test: <descriptive message>

   <optional details like files modified, coverage areas>

   🤖 Generated with [Claude Code](https://claude.com/claude-code)

   Co-Authored-By: Claude <noreply@anthropic.com>
   EOF
   )"
   ```

6. **Report result**: Show files committed and commit hash.

## Output Format

```markdown
## Test Cleanup Complete

**Committed N files:**
- `tests/path/to/test_file.py` (new/modified)
- ...

**Commit:** `abc1234` test: <message>
```

## Example

```
/test-cleanup
```
