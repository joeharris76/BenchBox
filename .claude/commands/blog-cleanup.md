---
description: Create a cleanup commit for all modified blog files
---

# Blog Cleanup

Create a commit for all modified blog files with a descriptive message.

## Arguments

`$ARGUMENTS`: Not used.

## Instructions

1. **Find modified blog files**:

   ```bash
   git status --porcelain _blog/
   ```

2. **If no files**, inform user and exit:

   > No modified blog files found. Nothing to commit.

3. **Analyze changes**:

   - Run `git diff _blog/` for modified files
   - Read untracked files to understand new content
   - Categorize by series (e.g., `introducing-oxbow/`, `tutorials/`) and type (drafts, outlines)

4. **Generate commit message**:

   Format: `docs(blog): <summary>`

   Examples:
   - `docs(blog): add slash-commands tutorial draft`
   - `docs(blog): revise introducing-oxbow outlines`
   - `docs(blog): update style guide, add 3 new drafts`

5. **Stage and commit**:

   ```bash
   git add _blog/
   git commit -m "$(cat <<'EOF'
   docs(blog): <descriptive message>

   <optional details>

   🤖 Generated with [Claude Code](https://claude.com/claude-code)

   Co-Authored-By: Claude <noreply@anthropic.com>
   EOF
   )"
   ```

6. **Report result**: Show files committed and commit hash.

## Output Format

```markdown
## Blog Cleanup Complete

**Committed N files:**
- `_blog/path/to/file.md` (new/modified)
- ...

**Commit:** `abc1234` docs(blog): <message>
```

## Example

```
/blog-cleanup
```
