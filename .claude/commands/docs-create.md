---
description: Create new documentation page or section
---

# Create Documentation

## Arguments

`$ARGUMENTS`: Topic/title, `--type=guide|reference|tutorial|concept`, `--location=path/in/docs/`

## Instructions

1. **Determine Type**:
   | Type | Purpose | Location |
   |------|---------|----------|
   | guide | How-to tasks | `docs/guides/` |
   | reference | API/CLI ref | `docs/reference/` |
   | tutorial | Step-by-step | `docs/tutorials/` |
   | concept | Explanatory | `docs/concepts/` |
   | platform | Platform-specific | `docs/platforms/{name}/` |
   | benchmark | Benchmark docs | `docs/benchmarks/` |

2. **Choose Location**: `docs/development/`, `docs/benchmarks/`, `docs/platforms/`, `docs/reference/`, `docs/advanced/`, `docs/guides/`

3. **Create RST File**: Headings: `=====` (H1), `-----` (H2), `~~~~~` (H3). Add to parent toctree.

4. **Follow Patterns**: `.. code-block::`, `:doc:`/`:ref:` cross-refs, `.. note::`/`.. warning::`, `.. seealso::`

5. **Update Parent Index**: Add to `.. toctree::` directive.

6. **Validate**: `make docs-build`

## Output Format

```markdown
## Documentation Created

### Details
- **Path**: `docs/{location}/{filename}.rst`
- **Type**: {type} | **Title**: {title}

### Toctree Updated
- **Parent**: `docs/{parent}/index.rst`

### Build Status
Sphinx build: PASSED/WARNINGS

### Next Steps
- `/docs-review` to verify quality
- `/docs-build` to check rendering
```

## RST Templates

**Guide**:
```rst
{Title}
=======
Brief overview.

Prerequisites
-------------
- Python 3.10+

Steps
-----
Step 1: {Description}
~~~~~~~~~~~~~~~~~~~~~
.. code-block:: bash
    {project} command

See Also
--------
- :doc:`related-guide`
```

**Reference**:
```rst
{Module} Reference
==================
.. module:: {project}.module

.. autoclass:: ClassName
   :members:
```

## Example Usage

```
/docs-create ClickHouse Platform Guide --type=platform --location=platforms/
/docs-create API Reference for Results Module --type=reference
```
