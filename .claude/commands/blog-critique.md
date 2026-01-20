---
description: Critique and improve a blog post draft
---

# Critique Blog Post Draft

Adversarial review of blog post quality, accuracy, style compliance, and engagement.

## Arguments

`$ARGUMENTS`: Draft path, slug/title, `--style`, `--technical`, `--engagement`, `--final`, or empty for drafts list.

## Instructions

1. **Load Context**: Draft, outline, `_blog/{project}_Blog_Style_Guide.md`, series plan.

2. **Style Compliance**: Voice (conversational/authoritative, "we"/"you", jargon), Content (data-first, methodology, nuance), Format (title, hook, TL;DR, sections, code blocks).

3. **Technical Accuracy**: Benchmarks/citations/claims/methodology correct? Claims supported? Counterarguments? Limitations? Environment documented?

4. **Engagement**: Hook compelling? Flow logical? Transitions smooth? Paragraphs short? Skimmable?

5. **Editorial**: Title specific, evidence present, environment documented, code valid, links work, no typos, length appropriate.

6. **Generate Improvements**: Rewrites, missing elements, sections to cut/expand, structure changes.

## Output Format

```markdown
## Blog Critique: {Title}

### Overview
- **File**: `_blog/{series}/drafts/{slug}.md`
- **Word Count**: X,XXX | **Quality**: [Excellent/Good/Needs Work/Major Revision]

### Style Compliance
| Category     | Score | Notes |
| ------------ | ----- | ----- |
| Voice & Tone | 8/10  |       |
| Data-First   | 9/10  |       |
| Formatting   | 10/10 |       |

### Issues Found
| Severity | Location  | Issue             | Recommendation |
| -------- | --------- | ----------------- | -------------- |
| Critical | Para 3    | Unsupported claim | Add citation   |
| Warning  | Section 2 | Weak transition   | Rewrite        |

### Section Analysis
#### {Section}
- **Strength**: {what works}
- **Weakness**: {what doesn't}
- **Suggestion**: {improvement}

### Technical Accuracy
| Check           | Status    |
| --------------- | --------- |
| Benchmark data  | PASS/FAIL |
| Citations valid | PASS/FAIL |
| Methodology     | PASS/FAIL |

### Engagement
| Element      | Score |
| ------------ | ----- |
| Hook         | 7/10  |
| Flow         | 8/10  |
| Skimmability | 8/10  |

### Rewrites Suggested
**Original**: > {text}
**Suggested**: > {improved}

### Action Items
- [ ] **CRITICAL**: {must fix}
- [ ] **HIGH**: {recommended}

### Recommendation
**Publish Ready**: [Yes / After Edits / Needs Revision / Major Rewrite]
```

## Critique Rubric

- **9-10**: Publish-ready with minor polish
- **7-8**: Solid, needs targeted improvements
- **5-6**: Significant revision required
- **< 5**: Structural/fundamental issues

## Common Issues

**Structure**: Burying lede, missing thesis, repetitive conclusion, poor section order
**Style**: Passive voice, unexplained jargon, too formal, missing "why care"
**Technical**: Unsupported claims, missing methodology, outdated sources
**Engagement**: Weak hook, long paragraphs, no visual breaks, abrupt ending

## Example Usage

```
/blog-critique _blog/business-of-analytics/drafts/cloud-ssd.md
/blog-critique cloud-ssd --style
/blog-critique arm-performance --technical
/blog-critique cloud-ssd --final
```
