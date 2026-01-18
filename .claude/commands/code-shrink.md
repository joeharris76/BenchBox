---
description: >
  Compress code while preserving behavioral equivalence (validation-driven approach)
---

# Validation-Driven Code Compression

**Task**: Compress `{{arg}}`
**Goal**: Behavioral equivalence score ≥ 0.95. Target ~30% reduction.

---

## Step 1: Validate File Type

**ALLOWED**: `.py`, `.js`, `.ts`, `.tsx`, `.jsx`, `.go`, `.rs`, `.java`, `.rb`, `.sql`

**FORBIDDEN**: `__init__.py` (if only exports), `conftest.py`, `*_test.py`, `test_*.py`, `*.json`, `*.yaml`, `*.toml`, `*.md`, generated files, vendored code, migrations

If forbidden: "This compression process only applies to application source code, not tests, configs, or generated files."

---

## Step 2: Check Existing Baseline

```bash
FILENAME=$(basename "{{arg}}")
BASELINE="/tmp/original-$FILENAME"
[ -f "$BASELINE" ] && echo "Found baseline: $BASELINE ($(wc -l < "$BASELINE") lines). Scores compare against original."
```

---

## Step 3: Invoke Compression Agent

Task tool with `subagent_type: "general-purpose"`:

```
**Code Compression Task**

**File**: {{arg}}
**Goal**: Compress while preserving **behavioral equivalence** (score ≥ 0.95).
**Target**: ~30% line reduction ideal, but less acceptable. Behavioral equivalence mandatory.

## What is Behavioral Equivalence?

Same inputs → same outputs, same side effects, same error handling.

**Preserve ALL**:
- Public API (function signatures, class interfaces, exports)
- Type contracts (parameter/return types, generics)
- Side effects (I/O, state mutations, external calls)
- Error handling (exception types, messages, validation)
- Control flow (branching, loops, early returns)
- Dependencies (imports, function calls, inheritance)
- Assertions and invariants
- Critical comments (TODO, FIXME, HACK, why-comments)

**Safe to remove**:
- Dead code (unreachable branches, unused functions)
- Redundant implementations (duplicate logic)
- Verbose patterns → concise equivalents (loops → comprehensions)
- Unnecessary intermediate variables
- Excessive comments repeating code
- Defensive code for impossible conditions
- Over-abstraction
- Unused imports

## Compression Techniques by Language

**Python**: comprehensions, `any()`/`all()`, walrus `:=`, `dict.get()`, unpacking, f-strings, `pathlib`, context managers, `@property`

**JS/TS**: arrow functions, destructuring, `?.`/`??`, array methods, template literals, object shorthand, spread

**General**: early returns, guard clauses, remove else-after-return, combine/inline single-use functions

## Anti-Patterns to Avoid

**DO NOT**: Remove error handling, change public signatures, remove type hints on APIs, delete TODO/FIXME, combine unrelated functions, remove input validation, change exception types, remove logging, prioritize cleverness over readability

## Output

Read `{{arg}}`, compress it, **USE WRITE TOOL** to save compressed version.
**CRITICAL**: You MUST write the file, not describe it.
```

---

## Step 4: Validate with /compare-code

1. **Save original** (if baseline doesn't exist):
   ```bash
   FILENAME=$(basename "{{arg}}")
   BASELINE="/tmp/original-$FILENAME"
   [ ! -f "$BASELINE" ] && cp "{{arg}}" "$BASELINE" && echo "Saved baseline: $BASELINE"
   ```

2. **Determine version and save compressed**:
   ```bash
   FILENAME=$(basename "{{arg}}")
   VERSION_FILE="/tmp/shrink-code-$FILENAME-version.txt"
   if [ -f "$VERSION_FILE" ]; then
     VERSION=$(($(cat "$VERSION_FILE") + 1))
   else
     HIGHEST=$(ls /tmp/compressed-$FILENAME-v*.* 2>/dev/null | sed 's/.*-v\([0-9]*\)\..*/\1/' | sort -n | tail -1)
     VERSION=${HIGHEST:-0}; VERSION=$((VERSION + 1))
   fi
   echo "$VERSION" > "$VERSION_FILE"
   EXT="${FILENAME##*.}"
   # Save to /tmp/compressed-$FILENAME-v${VERSION}.$EXT
   ```

3. **Run static analysis**:
   ```bash
   # Python
   uv run ruff check /tmp/compressed-$FILENAME-v${VERSION}.py
   uv run ty check /tmp/compressed-$FILENAME-v${VERSION}.py
   # TypeScript
   npx tsc --noEmit /tmp/compressed-$FILENAME-v${VERSION}.ts
   ```

4. **Run validation AGAINST ORIGINAL**:
   ```
   /code-compare /tmp/original-$FILENAME /tmp/compressed-$FILENAME-v${VERSION}.$EXT
   ```

**IMPORTANT**: Always compare against original baseline, NOT intermediate versions.

---

## Step 5: Decision Logic

**Threshold**: 0.95

**Report Format (ALL 4 components)**:
1. What was removed/simplified
2. Validation details (contract/dependency/flow scores)
3. Results (original LOC, compressed LOC, reduction %, score)
4. Version comparison table

**Version Table**:
```markdown
| Version  | Lines   | Reduction | Score   | Status    |
| -------- | ------- | --------- | ------- | --------- |
| Original | {lines} | baseline  | N/A     | Reference |
| V1       | {lines} | {%}       | {score} | {status}  |
```

**If score ≥ 0.95**: **APPROVE** (ALL required)
→ Run tests: `uv run pytest {{arg_dir}} -q`
→ If pass: Overwrite original with approved version
→ Clean up: `rm /tmp/compressed-$FILENAME-v*.*`
→ KEEP baseline for future iterations
→ Ask: "Try for better compression? YES → iterate, NO → cleanup baseline"

**If user done**: `rm /tmp/original-$FILENAME /tmp/shrink-code-$FILENAME-version.txt`

**If score < 0.95**: **ITERATE**
```
Validation requires improvement. Score: {score}/0.95
Components: contract={}, dependency={}, flow={}
Issues: {warnings from /compare-code}
Breaking changes: {breaking_changes}
Re-invoking agent with feedback...
```
→ Go to Step 6

**If lint/type errors**: **FIX FIRST**
→ Fix syntax/type errors before validation
→ Re-run static analysis

---

## Step 6: Iteration Loop

**Iteration Prompt**:
```
**Code Compression - Revision Attempt {N}**

**Previous Score**: {score}/0.95

**Issues Identified**: {warnings from /compare-code}

**Breaking Changes to Restore**:
- **{contract}**: {change} | Fix: {recommendation}

**Broken Dependencies**:
- **{type}**: {from} → {to} | Impact: {impact}

**Task**: Restore behavioral equivalence while maintaining compression.

**Original**: /tmp/original-$FILENAME
**Previous**: /tmp/compressed-$FILENAME-v${VERSION}.$EXT

Focus: restore contracts, fix dependencies, maintain error handling.

**CRITICAL**: USE WRITE TOOL to save revised code.
```

**After iteration**: Save next version, lint/type check, /code-compare AGAINST ORIGINAL, apply decision logic.

**MANDATORY**: /code-compare for EVERY version. No manual validation.

**Anti-Patterns**: Manual "looks equivalent" checks, estimation without /code-compare, skipping static analysis

**Max iterations**: 3. If < 0.95, report best version and ask for guidance.

---

## Implementation Notes

- **Agent Type**: `subagent_type: "general-purpose"`
- **Validation**: /code-compare (SlashCommand)
- **Baseline**: Set ONCE, reused for all validations
- **Versioning**: Incrementing for rollback
- **Rollback**: `/tmp/compressed-$FILENAME-v{N}.$EXT`
- **Static Analysis**: Must pass before behavioral validation

---

## Success Criteria

**Approved when**: Score ≥ 0.95 AND static analysis passes AND tests pass (if available)

**Quality metrics**: ~30% reduction, contract/dependency/flow ≥ 0.95, no breaking changes, no lint/type errors

---

## Edge Cases

- **Public API changes**: STOP and ask user, never silently break API
- **Test dependencies**: Preserve internal functions tests import or update tests
- **Score plateau**: No improvement after 2 attempts = hitting limits. After 3, report best
- **Large files**: For >500 LOC, consider sections or suggest splitting
- **Already minimal**: Report "minimal compression opportunities" and skip

---

## Example Usage

```
/code-shrink tpch/generator.py
/code-shrink src/utils/helpers.ts
/code-shrink lib/parser.rb
```

Flow: Validate type → Baseline → Compress → v1 → lint/type → /code-compare → Score ≥0.95 → tests → Approve → Cleanup
