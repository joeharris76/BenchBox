---
description: Adversarial review of documentation from user perspective
---

# Adversarial Documentation Review

Analyze docs from skeptical user's perspective, identifying gaps and friction.

## Arguments

`$ARGUMENTS`: Doc path, topic, `user-journey:{persona}`, `new-user`, `developer`, or empty for critical paths.

## Personas

| Persona | Goal | Key Pages |
|---------|------|-----------|
| `new-user` | Install, run first benchmark | quickstart, installation, CLI |
| `developer` | Add platform adapter | development, architecture, API |
| `ops` | Debug production issue | troubleshooting, CLI, platforms |
| `contributor` | Understand codebase | architecture, development, testing |
| `evaluator` | Assess fit | features, benchmarks, platforms |

## Process

1. **Define Perspective**: Parse persona, identify goal, list pages to visit.
2. **Challenge Assumptions**: Prerequisites sufficient? Examples work? Errors documented? Jargon explained?
3. **Test Journeys**: Follow steps exactly, note confusion, identify missing steps, check links.
4. **Identify Gaps**: Missing edge cases, undocumented errors, implicit knowledge, broken examples.
5. **Verify Claims**: Test commands, verify performance claims, check compatibility statements.
6. **Assess Friction**: Where would user give up? What requires external research?

## Output Format

```markdown
## Adversarial Review

### Perspective
- **Persona**: {type} | **Goal**: {goal}

### Journey
| Step | Page | Outcome | Friction |
|------|------|---------|----------|
| 1 | install.rst | Blocked | Missing Python version |

### Issues
| Severity | Location | Issue | Fix |
|----------|----------|-------|-----|
| Critical | install:15 | Python version missing | Add "Requires 3.10+" |

### Friction Points
| Page | Point | Impact | Fix |
|------|-------|--------|-----|

### Missing Docs
| Topic | Why | Priority |
|-------|-----|----------|

### Risk Assessment
- **Abandonment**: [H/M/L] | **Support Burden**: [H/M/L]
```

## Adversarial Questions

**New Users**: Install <5 min? First benchmark without help? Understand or just copying?
**Developers**: Find needed API? Extension points documented? Architecture clear?
**Ops**: Debug without source? Errors documented? Runbook for common issues?

## Example Usage

```
/docs-adversarial new-user
/docs-adversarial developer
/docs-adversarial docs/quickstart.rst
```
