---
description: Build and validate documentation
---

# Build Documentation

Build Sphinx documentation and report issues.

## Arguments

`$ARGUMENTS`: Empty (full build), `serve`, `clean`, `linkcheck`, `validate`, `quick`

## Instructions

1. **Clean** (if `clean` or errors): `make docs-clean`
2. **Validate** (if `validate`): `make docs-validate`
3. **Build**: `make docs-build` or `cd docs && uv run sphinx-build -b html . _build/html`
4. **Link Check** (if `linkcheck`): `make docs-linkcheck`
5. **Serve** (if `serve`): `make docs-serve` (http://localhost:8000)
6. **Parse Output**: Count warnings by type, identify blocking errors.

## Output Format

```markdown
## Build Results

### Status
- **Status**: SUCCESS/FAILED
- **Warnings**: X | **Errors**: Y | **Time**: Z seconds

### Warnings
| Type | Count | Example |
|------|-------|---------|
| Missing ref | X | `file.rst:45: undefined label` |

### Errors
| File | Line | Error |
|------|------|-------|
| file.rst | 10 | Unexpected section title |

### Artifacts
- **HTML**: `docs/_build/html/` | **Pages**: X

### Link Check (if run)
- **Checked**: X | **Broken**: Y | **Redirects**: Z
```

## Common Warnings

| Warning | Cause | Fix |
|---------|-------|-----|
| `undefined label` | Missing :ref: target | Add label |
| `duplicate label` | Same label twice | Rename |
| `nonexisting document` | File deleted | Update toctree |
