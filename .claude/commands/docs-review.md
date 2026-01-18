---
description: Review existing documentation page or topic
---

# Review Documentation

Analyze documentation for accuracy, completeness, and quality.

## Arguments

`$ARGUMENTS`: Doc path (`docs/benchmarks/tpc-h.rst`), topic ("TPC-H", "CLI"), `all`, `recent`, or empty for high-traffic pages.

## Instructions

1. **Locate**: If path, read directly. If topic: `find docs/ -name "*.rst" | xargs grep -l -i "$ARGUMENTS"`. If recent: `find docs/ -name "*.rst" -mtime -7`.

2. **Analyze**:
   - **Accuracy**: Verify examples work, CLI matches `--help`, API refs match code, paths exist
   - **Completeness**: All features documented, examples provided, prerequisites listed, troubleshooting
   - **Quality**: Clear writing, logical organization, proper formatting, working cross-refs

3. **Validate**: Run CLI commands in dry-run, check syntax, verify paths, test URLs.

4. **Check Links**: `cd docs && make linkcheck`

## Output Format

```markdown
## Review: {topic}

### Summary
Reviewed X pages. Found Y issues.

### Issues
| Severity | File | Line | Issue | Fix |
|----------|------|------|-------|-----|
| Critical | file.rst | 45 | Outdated CLI | Update syntax |

### Accuracy
| Check | Status |
|-------|--------|
| CLI commands | PASS/FAIL |
| Code examples | PASS/FAIL |
| File paths | PASS/FAIL |

### Completeness
| Section | Status |
|---------|--------|
| Prerequisites | PASS/FAIL |
| Examples | PASS/FAIL |
| Troubleshooting | MISSING |

### Risk Assessment
- **User Confusion**: [H/M/L]
- **Outdated Info**: [H/M/L]
```

## Quality Criteria

| Aspect | Excellent | Needs Work |
|--------|-----------|------------|
| Accuracy | All examples work | Outdated content |
| Completeness | All features covered | Major gaps |
| Clarity | Easy to follow | Hard to understand |
