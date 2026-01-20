---
description: >
  Compress documentation while preserving execution equivalence (validation-driven approach)
---

# Validation-Driven Document Compression

**Task**: Compress `{{arg}}`
**Goal**: Execution equivalence (score = 1.0). Target ~50% reduction.

---

## Step 1: Validate Document Type

**ALLOWED**: `.claude/` (agents/, commands/, hooks/, settings.json), `CLAUDE.md`, project instructions, `docs/project/`, `docs/code-style/*-claude.md`
**FORBIDDEN**: `README.md`, `changelog.md`, `docs/studies/`, `docs/decisions/`, `todo.md`

If forbidden: "This compression process only applies to Claude-facing documentation."

---

## Step 2: Check Existing Baseline

```bash
BASELINE="/tmp/original-{{filename}}"
[ -f "$BASELINE" ] && echo "Found baseline: $BASELINE ($(wc -l < "$BASELINE") lines). Scores compare against original."
```

---

## Step 3: Invoke Compression Agent

Use Task tool with `subagent_type: "general-purpose"`:

```
**Document Compression Task**

**File**: {{arg}}
**Goal**: Perfect execution equivalence (score = 1.0). ~50% reduction ideal, lesser acceptable.

## Execution Equivalence Definition

Reader achieves same results following compressed version.

**Preserve ALL**:
- YAML frontmatter (between `---`) - REQUIRED for slash commands
- Decision-affecting: claims, requirements, constraints
- Relationship structure: temporal ordering, conditionals, prerequisites, exclusions
- Control flow: sequences, blocking checkpoints (STOP, WAIT), branching
- Executable details: commands, paths, thresholds, values

**Remove**:
- Redundancy, verbose explanations
- Meta-commentary (NOT structural metadata)
- Non-essential examples, extended justifications

## Compression Approach

**Focus on relationships**: Keep explicit statements, temporal ordering (A→B), conditional logic (IF-THEN-ELSE), constraints.
**Condense explanations**: Remove verbose sections, combine claims, use principles vs exhaustive lists.

## Output

Read `{{arg}}`, compress, **USE WRITE TOOL** to save.
**CRITICAL**: Actually write the file, do NOT just describe.
```

---

## Step 4: Validate with /docs-compare

1. **Save original** (if missing):
   ```bash
   BASELINE="/tmp/original-{{filename}}"
   [ ! -f "$BASELINE" ] && cp {{arg}} "$BASELINE" && echo "Saved baseline: $BASELINE"
   ```

2. **Version and save compressed**:
   ```bash
   VERSION_FILE="/tmp/docs-shrink-{{filename}}-version.txt"
   if [ -f "$VERSION_FILE" ]; then
     VERSION=$(($(cat "$VERSION_FILE") + 1))
   else
     HIGHEST=$(ls /tmp/compressed-{{filename}}-v*.md 2>/dev/null | sed 's/.*-v\([0-9]*\)\.md/\1/' | sort -n | tail -1)
     VERSION=${HIGHEST:-0}; VERSION=$((VERSION + 1))
   fi
   echo "$VERSION" > "$VERSION_FILE"
   # Save to /tmp/compressed-{{filename}}-v${VERSION}.md
   ```

3. **Verify YAML**:
   ```bash
   head -5 /tmp/compressed-{{filename}}-v${VERSION}.md | grep -q "^---$" || echo "WARNING: YAML frontmatter missing!"
   ```

4. **Validate AGAINST ORIGINAL**:
   ```bash
   /docs-compare /tmp/original-{{filename}} /tmp/compressed-{{filename}}-v${VERSION}.md
   ```

---

## Step 5: Decision Logic

**Threshold**: 1.0

**Report (4 components)**:
1. Preserved/removed
2. Validation (claim/relationship/graph scores)
3. Results (original size, compressed size, reduction %, score)
4. Version table

**Version Table**:
```markdown
| Version | Lines | Reduction | Score | Status |
|---------|-------|-----------|-------|--------|
| Original | {lines} | baseline | N/A | Reference |
| V1 | {lines} | {%} | {score} | {status} |
```

**If score = 1.0**: **APPROVE**
→ Overwrite original with approved version
→ Clean up: `rm /tmp/compressed-{{filename}}-v*.md`
→ KEEP baseline for future iterations
→ Ask: "Try for better compression? YES → iterate, NO → cleanup baseline"

**If user done**: `rm /tmp/original-{{filename}} /tmp/docs-shrink-{{filename}}-version.txt`

**If score < 1.0**: **ITERATE**
```
Validation requires improvement. Score: {score}/1.0
Components: claim={}, relationship={}, graph={}

Issues: {warnings from /docs-compare}
Lost relationships: {lost_relationships}

Re-invoking agent with feedback...
```
→ Go to Step 6

**Decision Verification**: 0.97 < 1.0 → ITERATE. 0.99 < 1.0 → ITERATE.

---

## Step 6: Iteration Loop

**Iteration Prompt**:
```
**Document Compression - Revision {N}**

**Previous Score**: {score}/1.0 (threshold: 1.0)

**Issues**:
{warnings from /docs-compare}

**Lost Relationships**:
- **{type}**: {from} → {to} | Constraint: {constraint} | Fix: {recommendation}

**Task**: Restore lost relationships while maintaining compression.

**Original**: /tmp/original-{{filename}}
**Previous**: /tmp/compressed-{{filename}}-v${VERSION}.md

Focus: explicit relationships, conditionals, mutual exclusivity, escalation paths.

**CRITICAL**: USE WRITE TOOL to save revised document.
```

**After iteration**: Save next version, re-run /docs-compare AGAINST ORIGINAL, apply decision logic.

**MANDATORY**: /docs-compare for EVERY version. No manual validation/estimation/checklist scoring.

**Anti-Patterns**:
- Manual checklist: "Estimated Score: 0.97"
- Estimation without /docs-compare
- Custom Task with "verify improvements"

**Maximum iterations**: 3. If still < 1.0, report best version, ask guidance.

---

## Implementation Notes

- **Agent Type**: `subagent_type: "general-purpose"`
- **Validation**: /docs-compare (SlashCommand)
- **Baseline**: Set ONCE, reused for all validations
- **Versioning**: Incrementing for rollback
- **Rollback**: `/tmp/compressed-{{filename}}-v{N}.md`

---

## Success Criteria

**Approved when**: Score = 1.0
**Quality**: ~50% reduction, claim/relationship/graph = 1.0, no critical losses

---

## Edge Cases

- **Abstraction vs Enumeration**: High-level constraints may score 0.85-0.94. System iterates to restore explicit relationships.
- **Score Plateau**: No improvement after 2 attempts = hitting limits. After 3, report best to user.
- **Large Documents**: >10KB → consider sections.

---

## Example

```
/docs-shrink /workspace/.claude/commands/example-command.md
```

Flow: Validate type → Save baseline → Compress → Save v1 → /docs-compare → Score 1.0 → Approve → Cleanup
