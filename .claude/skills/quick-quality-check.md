---
description: Run comprehensive pre-commit quality checks
---

# Quick Quality Check

Run all quality checks before committing code. This ensures CI will pass.

## Instructions

When the user asks to run quality checks, verify code quality, or check if code is ready to commit:

1. **Run all quality checks in sequence**:

   ```bash
   # 1. Lint check (ruff)
   make lint

   # 2. Format check (ruff format)
   make format

   # 3. Type check (ty check)
   make typecheck

   # 4. Fast tests (quick smoke test)
   make test-fast
   ```

   Or as a single command:
   ```bash
   make lint && make format && make typecheck && make test-fast
   ```

2. **Analyze each result**:

   **Lint:** Look for errors (E), warnings (W), style issues; note violated rules; check if fixable automatically. Auto-fix: `uv run ruff check --fix .`

   **Format:** Shows which files would be reformatted. No output means all files already formatted. Auto-applied by `make format`.

   **Typecheck:** Look for type errors, missing hints, incompatible types. Fix by adding type hints, importing from `typing`, or using `# type: ignore[error-code]` for intentional issues.

   **Tests:** Count passing/failing; look for import errors, data generation issues, connection failures.

3. **Provide summary report**:

   ```markdown
   ## Quality Check Results

   ✅ Lint: Passed (or X issues found)
   ✅ Format: All files formatted correctly
   ✅ Typecheck: No type errors
   ✅ Tests: 45/45 passed

   Overall: READY TO COMMIT / NEEDS FIXES
   ```

4. **If failures exist**, group by category, prioritize critical issues (test failures, type errors), suggest fixes, offer to auto-fix lint issues.

## CI Requirements

BenchBox CI requires: no ruff errors, all files formatted with ruff, no type checker errors, all tests passing.

## Extended Checks

For more thorough checking:

```bash
make test-all              # Run all tests (not just fast)
make validate-imports      # Check import structure
make dependency-check      # Verify dependency lock file
```

## Output Format Example

```markdown
## Quality Check Summary

### ✅ Passing
- Lint: No issues
- Format: All files formatted

### ❌ Failing
- Typecheck: 3 errors in benchbox/core/tpch/generator.py
  - Line 45: Missing return type
  - Line 67: Incompatible type assignment
  - Line 89: Undefined name 'scale_factor'

- Tests: 2 failures
  - test_tpch_query_generation: AssertionError
  - test_schema_validation: ImportError

### Recommendations
1. Add return type hint to generate_data() at line 45
2. Fix type mismatch at line 67 (expected str, got int)
3. Import scale_factor or fix typo at line 89
4. Debug test failures - see full output above

### Ready to Commit?
NO - Fix 5 issues listed above
```

## Key Notes

- Run before every commit; CI runs similar checks—catch issues early
- Line length limit: 120 characters; type hints are required by project standards
- All code must be ruff-formatted
