---
description: Adversarial code review for quality and patterns
---

# Code Review

Adversarial review for quality, patterns, security, and issues.

## Arguments

`$ARGUMENTS`: File path, directory, `staged`, `recent` (last 5 commits), `pr` (PR changes), topic, or empty for recently modified.

## Instructions

1. **Determine Scope**: File/dir→read directly, `staged`→`git diff --cached`, `recent`→`git diff HEAD~5`, `pr`→`git diff main...HEAD`

2. **Review Patterns**:
   - **Architecture**: Proper inheritance (BaseBenchmark, PlatformAdapter), module structure, abstraction, single responsibility
   - **Code Quality**: Type hints on public APIs, docstrings, error handling, no hardcoded values, line length ≤120
   - **Testing**: Coverage, edge cases, fixtures, markers

3. **Security**: No credential exposure, parameterized queries, safe file ops (no path traversal), shell=False, input validation

4. **Performance**: No O(n²) where O(n) possible, appropriate data structures, no N+1 queries, lazy evaluation, no unnecessary copying

5. **Maintainability**: Clear naming, DRY (not over-abstracted), minimal complexity, "why" comments

6. **Automated Checks**: `make lint && make typecheck`

## Output Format

```markdown
## Code Review: {scope}

### Summary
Reviewed X files, Y lines. Found Z issues.

### Issues
| Severity | File    | Line | Issue                  | Recommendation            |
| -------- | ------- | ---- | ---------------------- | ------------------------- |
| Critical | file.py | 45   | SQL injection          | Use parameterized queries |
| Warning  | file.py | 90   | Missing error handling | Add try/except            |

### Pattern Compliance
| Pattern    | Status  | Notes |
| ---------- | ------- | ----- |
| Type hints | OK/WARN |       |
| Docstrings | OK/WARN |       |

### Security
| Check              | Status    |
| ------------------ | --------- |
| No hardcoded creds | PASS/FAIL |
| SQL injection safe | PASS/FAIL |

### Quality Score
| Aspect      | Score |
| ----------- | ----- |
| Correctness | X/10  |
| Clarity     | X/10  |
| Security    | X/10  |
| **Overall** | X/10  |

### Action Items
- [ ] Fix CRITICAL before merge
- [ ] Fix HIGH before merge
```

## Review Checklist

**Must Pass**: No security vulnerabilities, no hardcoded creds, error handling, tests pass, lint/type pass

**Should Pass**: Type hints on public APIs, docstrings, follows patterns, no perf issues

## Example Usage

```
/code-review tpch/generator.py
/code-review staged
/code-review pr
```
