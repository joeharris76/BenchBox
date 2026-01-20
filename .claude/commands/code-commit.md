---
description: Commit uncommitted changes from current session with descriptive message
---

# Code Commit

Commit files that Claude Code has modified during this session with an auto-generated descriptive commit message.

## Arguments

`$ARGUMENTS`: Optional scope hint (e.g., "platform work", "test fixes"). Used to help focus the commit message.

## Instructions

1. **Identify session-modified files**:

   Review the current conversation to identify all files that YOU (Claude Code) have created, edited, or deleted during this session using Write, Edit, or Bash tools.

   If no files were modified by Claude in this session, inform user and exit:
   > No files were modified by Claude Code in this session. Nothing to commit.

2. **Verify files have uncommitted changes**:

   ```bash
   git status --porcelain <session-modified-files>
   ```

   Filter to only files that actually have uncommitted changes (some may have been committed already or reverted).

3. **Gather change details**:

   ```bash
   # Diff of session-modified files only
   git diff <session-modified-files>
   git diff --cached <session-modified-files>

   # Recent commit style reference
   git log --oneline -5
   ```

4. **Analyze changes**:

   - Review what was changed in the session-modified files
   - Categorize: feature, fix, refactor, test, docs, chore, etc.
   - Note the primary area affected (core, platforms, tests, docs, etc.)

5. **Determine commit type** (Conventional Commits):

   | Type | When to use |
   |------|-------------|
   | `feat` | New feature or capability |
   | `fix` | Bug fix |
   | `refactor` | Code restructuring without behavior change |
   | `test` | Adding or updating tests |
   | `docs` | Documentation only |
   | `chore` | Build, config, tooling changes |
   | `perf` | Performance improvement |
   | `style` | Formatting, whitespace (no logic change) |

6. **Generate commit message**:

   Format: `type(scope): concise description`

   Guidelines:
   - Scope is optional but helpful (e.g., `feat(tpch):`, `fix(cli):`)
   - Description should be imperative ("add", "fix", "update", not "added", "fixed")
   - Keep first line under 72 characters
   - Add body for complex changes explaining WHY

7. **Stage and commit atomically** (single command):

   ```bash
   git add <file1> <file2> ... && git commit -m "$(cat <<'EOF'
   type(scope): descriptive message

   Optional body with more details about what changed and why.

   🤖 Generated with [Claude Code](https://claude.com/claude-code)

   Co-Authored-By: Claude <noreply@anthropic.com>
   EOF
   )"
   ```

   **CRITICAL**:
   - Stage and commit MUST be in a single `&&` chain to avoid conflicts with other committers
   - NEVER use `git add -A` - only add the specific session-modified files
   - ONLY include files that Claude Code modified in this session

8. **Verify and report**:

   ```bash
   git log --oneline -1
   ```

## Output Format

```markdown
## Commit Created

**Files committed (N):**
- `path/to/file.py` (modified)
- `path/to/new_file.py` (new)

**Commit:** `abc1234` type(scope): message

**Summary:** Brief description of what was committed.
```

## Examples

```
/code-commit
→ Commits only files Claude modified this session

/code-commit platform adapter work
→ Uses hint to focus message on platform changes
```

## Important Notes

- **Session scope**: ONLY commit files that Claude Code has modified during this conversation. Do NOT commit other uncommitted changes that existed before or were made manually by the user.
- **Atomic operations**: Always chain `git add` and `git commit` with `&&` in a single command.
- **No `git add -A`**: Always stage specific session-modified files explicitly.
- **Conventional Commits**: Follow project standards for commit message format.
- **Co-authorship**: Include Claude Code attribution in commit footer.
